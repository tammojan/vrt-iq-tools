//
// usrp_to_dada.cpp
//
// Stream groups of USRP RX channels straight into psrdada ring buffers.
//
// One ring buffer per group; the channels within a group are interleaved
// sample-by-sample as polarisations (NPOL = group size).  The default
// configuration is three dual-polarisation groups -- six USRP channels, three
// DADA buffers -- as used for a 1 GHz feed on a USRP X440.
//
// Derived from usrp_to_vrt.cpp (Copyright 2025 Thomas Telkamp, MIT), which is
// itself derived from Ettus' rx_samples_to_file.cpp, and from vrt_to_dada.cpp.
// The intermediate VRT/ZMQ layer is gone and so is all amplitude/phase
// scaling: samples reach DADA as the integers the radio produced.
//
// SPDX-License-Identifier: MIT
//

#include <uhd/exception.hpp>
#include <uhd/types/sensors.hpp>
#include <uhd/types/tune_request.hpp>
#include <uhd/usrp/multi_usrp.hpp>
#include <uhd/utils/safe_main.hpp>
#include <uhd/utils/thread.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/time.h>
#include <unistd.h>

#if defined(__x86_64__) || defined(__i386__)
#    include <immintrin.h>
#endif

#ifdef __linux__
#    include <pthread.h>
#    include <sched.h>
#endif

// DADA
#include <ascii_header.h>
#include <dada_def.h>
#include <dada_hdu.h>
#include <ipcbuf.h>
#include <ipcio.h>
#include <multilog.h>

namespace po = boost::program_options;

static const size_t MAX_POL = 8;

// ---------------------------------------------------------------------------
// Global run state
// ---------------------------------------------------------------------------

static std::atomic<bool> stop_signal_called{false};
static std::atomic<bool> fatal_error{false};
static std::mutex console_mutex;

// Thrown out of the DADA writer when a stop is requested while it is waiting
// for room in the ring.  Not an error; the worker unwinds quietly.
struct StopRequested : std::exception
{
    const char* what() const noexcept override { return "stop requested"; }
};

static void sig_int_handler(int)
{
    if (stop_signal_called) {
        // Second Ctrl-C.  The graceful path is wedged -- almost always a
        // worker blocked inside psrdada on a full ring with no reader -- so
        // leave now rather than pretend.  write()/_exit() are the only things
        // safe to call from here.
        static const char msg[] =
            "\nSecond interrupt: exiting immediately. The DADA ring buffers were "
            "not closed cleanly and may need to be reset.\n";
        const ssize_t ignored = ::write(STDERR_FILENO, msg, sizeof(msg) - 1);
        (void)ignored;
        _exit(EXIT_FAILURE);
    }
    stop_signal_called = true;
}

static void fail(const std::string& msg)
{
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "ERROR: " << msg << std::endl;
    }
    fatal_error          = true;
    stop_signal_called   = true;
}

static void note(const std::string& msg)
{
    std::lock_guard<std::mutex> lock(console_mutex);
    std::cerr << msg << std::endl;
}

// ---------------------------------------------------------------------------
// Small parsing helpers.  Lists are comma separated as in usrp_to_vrt; groups
// of channels are separated by ';'.
// ---------------------------------------------------------------------------

static std::vector<std::string> split_str(const std::string& s, const char* seps)
{
    std::vector<std::string> out;
    if (s.empty())
        return out;
    boost::split(out, s, boost::is_any_of(seps));
    for (auto& t : out)
        boost::trim(t);
    out.erase(std::remove_if(out.begin(),
                  out.end(),
                  [](const std::string& x) { return x.empty(); }),
        out.end());
    return out;
}

static std::vector<double> parse_doubles(const std::string& s)
{
    std::vector<double> out;
    for (const auto& t : split_str(s, "\"',"))
        out.push_back(boost::lexical_cast<double>(t));
    return out;
}

static std::vector<long> parse_longs(const std::string& s)
{
    std::vector<long> out;
    for (const auto& t : split_str(s, "\"',"))
        out.push_back(boost::lexical_cast<long>(t));
    return out;
}

// A per-stream / per-channel value list.  If the list has one entry per
// channel it is indexed by channel; if it has one entry per stream it is
// indexed by stream; otherwise the first entry applies everywhere.  When the
// number of streams equals the number of channels the channel index wins.
template <typename T>
static T pick(const std::vector<T>& v,
    size_t stream_i,
    size_t chan_i,
    size_t n_streams,
    size_t n_chans)
{
    if (v.size() == 1)
        return v.front();
    if (v.size() == n_chans)
        return v[chan_i];
    if (v.size() == n_streams)
        return v[stream_i];
    throw std::logic_error("option list length was not validated");
}

// A list of the wrong length is a typo, not something to guess at: silently
// falling back to the first value is how you end up recording the right band
// into the wrong ring buffer.  Checked once, at startup.
template <typename T>
static void check_list(const std::vector<T>& v,
    const char* option,
    size_t n_streams,
    size_t n_chans,
    bool per_channel_ok)
{
    if (v.empty())
        throw std::runtime_error(std::string(option) + ": no values given");
    if (v.size() == 1 || v.size() == n_streams
        || (per_channel_ok && v.size() == n_chans))
        return;
    std::string want = "1 (one for all) or " + std::to_string(n_streams)
                       + " (one per stream)";
    if (per_channel_ok && n_chans != n_streams)
        want += " or " + std::to_string(n_chans) + " (one per channel)";
    throw std::runtime_error(std::string(option) + ": got "
                             + std::to_string(v.size())
                             + (v.size() == 1 ? " value, expected " : " values, expected ")
                             + want);
}

// ---------------------------------------------------------------------------
// Front-panel ports.
//
// The wiring from feed polarisations to RF connectors is rarely tidy, so
// --streams accepts three spellings for each port and they can be mixed:
//
//   3        a bare UHD channel number
//   A:3      a subdev spec entry, as printed by uhd_usrp_probe
//   0/3      front-panel "DB 0 / RF 3"; DB n is subdev letter 'A' + n
//
// On the X440 with an 8-channel image the front panel maps straight through:
// DB 0 / RF 0..3 are A:0..A:3 and DB 1 / RF 0..3 are B:0..B:3, which with the
// default subdev spec are channels 0..7 in that order.  Names are resolved
// against the *active* spec, so they stay correct under a custom --subdev.
// ---------------------------------------------------------------------------

static std::string panel_label(const std::string& db, const std::string& sd)
{
    if (db.size() == 1 && db[0] >= 'A' && db[0] <= 'Z')
        return "DB" + std::to_string(db[0] - 'A') + "/RF" + sd;
    return db + ":" + sd;
}

static size_t resolve_port(const std::string& tok, const uhd::usrp::subdev_spec_t& spec)
{
    if (tok.find_first_not_of("0123456789") == std::string::npos)
        return static_cast<size_t>(std::stoul(tok));

    std::string db, sd;
    const size_t colon = tok.find(':');
    const size_t slash = tok.find('/');
    if (colon != std::string::npos) {
        db = tok.substr(0, colon);
        sd = tok.substr(colon + 1);
        boost::to_upper(db);
    } else if (slash != std::string::npos) {
        const std::string dbn = tok.substr(0, slash);
        sd                    = tok.substr(slash + 1);
        if (dbn.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("cannot parse port '" + tok + "'");
        db = std::string(1, char('A' + std::stoul(dbn)));
    } else {
        throw std::runtime_error(
            "cannot parse port '" + tok
            + "'; expected a channel number, A:n, or DB/RF as 0/n");
    }

    for (size_t i = 0; i < spec.size(); i++)
        if (spec[i].db_name == db && spec[i].sd_name == sd)
            return i;
    throw std::runtime_error("port '" + tok + "' resolves to subdev " + db + ":" + sd
                             + ", which is not in the active RX subdev spec");
}

// ---------------------------------------------------------------------------
// Polarisation interleave.
//
// UHD hands us one contiguous buffer per channel.  dspsr wants the
// polarisations interleaved per time sample:
//
//   [p0_re p0_im p1_re p1_im][p0_re p0_im p1_re p1_im] ...
//
// One complex sc16 sample is one uint32 (int16 I in the low half, int16 Q in
// the high half, little endian); one truncated 8-bit sample is one uint16.
// Nothing here scales or converts to float -- the 16-bit path is a pure
// reshuffle of the bytes UHD delivered.
// ---------------------------------------------------------------------------

static inline void store_fence()
{
#if defined(__AVX2__)
    _mm_sfence();
#endif
}

// Generic N-polarisation interleave, no truncation.
static inline void interleave_n_u32(uint32_t* __restrict dst,
    const uint32_t* const* src,
    size_t npol,
    size_t n)
{
    for (size_t k = 0; k < n; k++)
        for (size_t p = 0; p < npol; p++)
            dst[k * npol + p] = src[p][k];
}

// Two-polarisation sc16 interleave.  Non-temporal stores: the DADA ring block
// we are filling will not be read again by this process, so there is no point
// pulling it into cache (and no point in the read-for-ownership traffic).
static inline void interleave2_u32(uint32_t* __restrict dst,
    const uint32_t* __restrict a,
    const uint32_t* __restrict b,
    size_t n)
{
    size_t k = 0;
#if defined(__AVX2__)
    if ((reinterpret_cast<uintptr_t>(dst) & 31u) == 0) {
        for (; k + 8 <= n; k += 8) {
            const __m256i va = _mm256_loadu_si256((const __m256i*)(a + k));
            const __m256i vb = _mm256_loadu_si256((const __m256i*)(b + k));
            // Per 128-bit lane: lo = a0 b0 a1 b1 | a4 b4 a5 b5
            //                   hi = a2 b2 a3 b3 | a6 b6 a7 b7
            const __m256i lo = _mm256_unpacklo_epi32(va, vb);
            const __m256i hi = _mm256_unpackhi_epi32(va, vb);
            const __m256i o0 = _mm256_permute2x128_si256(lo, hi, 0x20);
            const __m256i o1 = _mm256_permute2x128_si256(lo, hi, 0x31);
            _mm256_stream_si256((__m256i*)(dst + 2 * k), o0);
            _mm256_stream_si256((__m256i*)(dst + 2 * k + 8), o1);
        }
        store_fence();
    }
#endif
    for (; k < n; k++) {
        dst[2 * k]     = a[k];
        dst[2 * k + 1] = b[k];
    }
}

static inline int8_t sat8(int32_t v)
{
    if (v > 127)
        return 127;
    if (v < -128)
        return -128;
    return static_cast<int8_t>(v);
}

// Truncate one sc16 word to one sc8 word, keeping bits [shift+7 : shift] of
// each component.  shift == 8 keeps the top 8 bits and cannot overflow.
static inline uint16_t trunc_word(uint32_t w, int shift)
{
    const int16_t i = static_cast<int16_t>(static_cast<uint16_t>(w & 0xffffu));
    const int16_t q = static_cast<int16_t>(static_cast<uint16_t>(w >> 16));
    const uint8_t i8 = static_cast<uint8_t>(sat8(static_cast<int32_t>(i) >> shift));
    const uint8_t q8 = static_cast<uint8_t>(sat8(static_cast<int32_t>(q) >> shift));
    return static_cast<uint16_t>(i8 | (static_cast<uint16_t>(q8) << 8));
}

static inline void interleave_n_trunc(uint16_t* __restrict dst,
    const uint32_t* const* src,
    size_t npol,
    size_t n,
    int shift)
{
    for (size_t k = 0; k < n; k++)
        for (size_t p = 0; p < npol; p++)
            dst[k * npol + p] = trunc_word(src[p][k], shift);
}

// Two-polarisation truncate-and-interleave.
static inline void interleave2_trunc(uint16_t* __restrict dst,
    const uint32_t* __restrict a,
    const uint32_t* __restrict b,
    size_t n,
    int shift)
{
    size_t k = 0;
#if defined(__AVX2__)
    if ((reinterpret_cast<uintptr_t>(dst) & 31u) == 0) {
        const __m128i cnt = _mm_cvtsi32_si128(shift);
        // Within each 128-bit lane, packs gives [A0 A1 A2 A3 B0 B1 B2 B3]
        // (each Ak/Bk a 16-bit complex sc8 sample); shuffle to [A0 B0 A1 B1 ...]
        const __m256i mask = _mm256_setr_epi8(0,
            1, 8, 9, 2, 3, 10, 11, 4, 5, 12, 13, 6, 7, 14, 15,
            0, 1, 8, 9, 2, 3, 10, 11, 4, 5, 12, 13, 6, 7, 14, 15);
        for (; k + 8 <= n; k += 8) {
            __m256i va = _mm256_loadu_si256((const __m256i*)(a + k));
            __m256i vb = _mm256_loadu_si256((const __m256i*)(b + k));
            va               = _mm256_sra_epi16(va, cnt);
            vb               = _mm256_sra_epi16(vb, cnt);
            const __m256i pk = _mm256_packs_epi16(va, vb);
            const __m256i o  = _mm256_shuffle_epi8(pk, mask);
            _mm256_stream_si256((__m256i*)(dst + 2 * k), o);
        }
        store_fence();
    }
#endif
    for (; k < n; k++) {
        dst[2 * k]     = trunc_word(a[k], shift);
        dst[2 * k + 1] = trunc_word(b[k], shift);
    }
}

// ---------------------------------------------------------------------------
// DADA ring buffer writer.
//
// Uses the block-write API rather than ipcio_write(), so that the interleave
// above writes directly into the ring's shared memory.  At 8 GB/s aggregate,
// the memcpy that ipcio_write() would perform is worth avoiding.
// ---------------------------------------------------------------------------

class DadaWriter
{
public:
    DadaWriter(key_t key, std::string name) : key_(key), name_(std::move(name)) {}
    ~DadaWriter() { close(); }

    DadaWriter(const DadaWriter&)            = delete;
    DadaWriter& operator=(const DadaWriter&) = delete;

    void connect()
    {
        log_ = multilog_open(name_.c_str(), 0);
        multilog_add(log_, stderr);
        hdu_ = dada_hdu_create(log_);
        dada_hdu_set_key(hdu_, key_);
        if (dada_hdu_connect(hdu_) < 0)
            throw std::runtime_error(name_ + ": could not connect to DADA HDU");
        if (dada_hdu_lock_write(hdu_) < 0)
            throw std::runtime_error(name_ + ": could not get write lock on DADA HDU");
        block_bytes_  = ipcbuf_get_bufsz((ipcbuf_t*)hdu_->data_block);
        header_bytes_ = ipcbuf_get_bufsz(hdu_->header_block);
        connected_    = true;
    }

    uint64_t block_bytes() const { return block_bytes_; }
    uint64_t header_bytes() const { return header_bytes_; }

    // Ring occupancy, for the progress line and the startup sanity check.
    // Both return 0 when the probe is compiled out.
    uint64_t nfull() const
    {
#ifndef USRP_TO_DADA_NO_RING_PROBE
        return connected_ ? ipcbuf_get_nfull((ipcbuf_t*)hdu_->data_block) : 0;
#else
        return 0;
#endif
    }
    uint64_t nbufs() const
    {
#ifndef USRP_TO_DADA_NO_RING_PROBE
        return connected_ ? ipcbuf_get_nbufs((ipcbuf_t*)hdu_->data_block) : 0;
#else
        return 0;
#endif
    }

    void write_header(const std::string& hdr)
    {
        char* ipc_header = ipcbuf_get_next_write(hdu_->header_block);
        if (ipc_header == nullptr)
            throw std::runtime_error(name_ + ": ipcbuf_get_next_write failed");
        std::memset(ipc_header, 0, header_bytes_);
        std::memcpy(ipc_header,
            hdr.data(),
            std::min<size_t>(hdr.size(), static_cast<size_t>(header_bytes_)));
        if (ipcbuf_mark_filled(hdu_->header_block, header_bytes_) < 0)
            throw std::runtime_error(name_ + ": could not mark header block filled");
    }

    // Returns a pointer into the current ring block with 'avail' writable
    // bytes.  Blocks until a free block is available -- that back pressure is
    // what surfaces as a USRP overflow when the consumer cannot keep up.
    char* reserve(uint64_t& avail)
    {
        if (!have_block_)
            open_block();
        avail = block_bytes_ - used_;
        return cur_ + used_;
    }

    void commit(uint64_t nbytes)
    {
        used_ += nbytes;
        if (used_ >= block_bytes_) {
            if (ipcio_close_block_write(hdu_->data_block, used_) < 0)
                throw std::runtime_error(name_ + ": ipcio_close_block_write failed");
            have_block_ = false;
            used_       = 0;
        }
    }

    void close()
    {
        if (!connected_)
            return;
        if (have_block_) {
            ipcio_close_block_write(hdu_->data_block, used_);
            have_block_ = false;
        }
        if (dada_hdu_unlock_write(hdu_) < 0)
            std::cerr << name_ << ": dada_hdu_unlock_write failed" << std::endl;
        if (dada_hdu_disconnect(hdu_) < 0)
            std::cerr << name_ << ": dada_hdu_disconnect failed" << std::endl;
        dada_hdu_destroy(hdu_);
        connected_ = false;
    }

private:
    // ipcio_open_block_write() blocks uninterruptibly when the ring has no room,
    // and while it is blocked this thread cannot see the stop flag -- that is
    // what makes Ctrl-C appear to do nothing.  So wait in a loop we control.
    //
    // "No room" means every block is full and waiting for the reader: a writer
    // may take a block that is free or that the reader has returned.  Do NOT
    // use ipcbuf_get_nclear() for this -- it counts only blocks the reader has
    // handed back, so it reads zero on a freshly created ring that is entirely
    // available, and waiting on it deadlocks before the first write.
    //
    // The probe is advisory only.  If it ever disagrees with psrdada we give up
    // waiting and make the blocking call, so a wrong probe can never wedge the
    // writer -- the worst case is the old, less interruptible behaviour.
    void open_block()
    {
#ifndef USRP_TO_DADA_NO_RING_PROBE
        const uint64_t nb = nbufs();
        if (nb > 1) {
            const auto t_enter = std::chrono::steady_clock::now();
            while (nfull() + 1 >= nb) {
                if (stop_signal_called)
                    throw StopRequested();
                const auto waited = std::chrono::steady_clock::now() - t_enter;
                if (waited > std::chrono::seconds(10)) {
                    note(name_
                         + ": ring still reports full after 10 s; falling back to "
                           "a blocking write (press Ctrl-C twice if this wedges)");
                    break;
                }
                if (!ring_full_warned_ && waited > std::chrono::milliseconds(200)) {
                    ring_full_warned_ = true;
                    note(name_
                         + ": DADA ring is full, waiting for a free block. "
                           "Is a reader attached and keeping up?");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
#endif
        uint64_t block_id = 0;
        cur_              = ipcio_open_block_write(hdu_->data_block, &block_id);
        if (cur_ == nullptr)
            throw std::runtime_error(name_ + ": ipcio_open_block_write failed");
        used_       = 0;
        have_block_ = true;
    }

    key_t key_;
    std::string name_;
    multilog_t* log_    = nullptr;
    dada_hdu_t* hdu_    = nullptr;
    char* cur_          = nullptr;
    uint64_t block_bytes_  = 0;
    uint64_t header_bytes_ = 0;
    uint64_t used_         = 0;
    bool have_block_       = false;
    bool connected_        = false;
    bool ring_full_warned_ = false;
};

// Write nsamps time samples into the ring, splitting across block boundaries.
// 'fill' receives (dst, sample_offset, count).
template <typename Fill>
static void emit(DadaWriter& w, size_t nsamps, size_t sample_bytes, Fill fill)
{
    size_t done = 0;
    while (done < nsamps) {
        uint64_t avail = 0;
        char* dst      = w.reserve(avail);
        const size_t fit = static_cast<size_t>(avail / sample_bytes);
        if (fit == 0)
            throw std::runtime_error(
                "DADA block size is not a multiple of the time-sample size");
        const size_t n = std::min(nsamps - done, fit);
        fill(dst, done, n);
        w.commit(static_cast<uint64_t>(n) * sample_bytes);
        done += n;
    }
}

// ---------------------------------------------------------------------------
// DADA header assembly
// ---------------------------------------------------------------------------

// Laid out in the conventional psrdada style: key padded to column 13, value
// padded to column 33, then a '#' comment, with blank lines and comment lines
// marking sections.  An entry with an empty key is a literal blank or comment
// line rather than a parameter.
static const size_t HDR_KEY_COL     = 13;
static const size_t HDR_COMMENT_COL = 33;

struct HeaderEntry
{
    std::string key;
    std::string value;
    std::string comment;
    bool optional = false; // provenance: dropped by --no-provenance
};
using HeaderKV = std::vector<HeaderEntry>;

// Sets or updates a parameter.  An existing entry keeps its comment unless a
// new one is given, so a --header-file override does not strip the annotation.
static void set_kv(HeaderKV& kv,
    const std::string& k,
    const std::string& v,
    const std::string& comment = "")
{
    for (auto& e : kv) {
        if (!e.key.empty() && e.key == k) {
            e.value = v;
            if (!comment.empty())
                e.comment = comment;
            return;
        }
    }
    kv.push_back(HeaderEntry{k, v, comment});
}

static void add_blank(HeaderKV& kv, bool optional = false)
{
    kv.push_back(HeaderEntry{"", "", "", optional});
}

static void add_section(HeaderKV& kv, const std::string& text, bool optional = false)
{
    kv.push_back(HeaderEntry{"", "", text, optional});
}

// As set_kv, but marks the entry as provenance rather than something a reader
// needs.  Kept separate so --no-provenance can drop exactly this set.
static void set_prov(HeaderKV& kv,
    const std::string& k,
    const std::string& v,
    const std::string& comment = "")
{
    set_kv(kv, k, v, comment);
    for (auto& e : kv)
        if (!e.key.empty() && e.key == k)
            e.optional = true;
}

static std::string pad_to(const std::string& s, size_t w)
{
    return s.size() >= w ? s + " " : s + std::string(w - s.size(), ' ');
}

static std::string render_header(const HeaderKV& kv,
    uint64_t hdr_size,
    bool include_optional = true)
{
    std::ostringstream os;
    for (const auto& e : kv) {
        if (e.optional && !include_optional)
            continue;
        if (e.key.empty()) {
            if (e.comment.empty())
                os << "\n";
            else
                os << "# " << e.comment << "\n";
            continue;
        }
        std::string line = pad_to(e.key, HDR_KEY_COL) + e.value;
        if (!e.comment.empty()) {
            line += line.size() < HDR_COMMENT_COL
                        ? std::string(HDR_COMMENT_COL - line.size(), ' ')
                        : std::string(" ");
            line += "# " + e.comment;
        }
        os << line << "\n";
    }
    os << "# end of header\n";
    std::string s = os.str();
    if (s.size() >= hdr_size)
        throw std::runtime_error("DADA header does not fit in the header block");
    // Pad with NUL, not spaces.  psrdada's ascii_header_get() is strstr()-based,
    // so a header with no terminator sends every lookup of an absent key --
    // and readers probe plenty of optional ones -- straight off the end of the
    // block and into whatever follows it on the heap.  That reads as a crash
    // that appears and disappears when unrelated lines are added or removed.
    s.resize(hdr_size, '\0');
    return s;
}

// KEY VALUE lines; '#' comments and blank lines ignored.
static HeaderKV read_header_file(const std::string& path)
{
    HeaderKV kv;
    std::ifstream f(path);
    if (!f)
        throw std::runtime_error("could not open header file " + path);
    std::string line;
    while (std::getline(f, line)) {
        // Allow a trailing comment on a value line, as the reference headers use.
        const size_t hash = line.find('#');
        if (hash != std::string::npos)
            line.erase(hash);
        boost::trim(line);
        if (line.empty())
            continue;
        const size_t sp = line.find_first_of(" \t");
        if (sp == std::string::npos)
            set_kv(kv, line, "");
        else {
            std::string k = line.substr(0, sp);
            std::string v = line.substr(sp + 1);
            boost::trim(v);
            set_kv(kv, k, v);
        }
    }
    return kv;
}

// The Unix epoch is MJD 40587.  PICOSECONDS carries the exact fraction of a
// second; this is the same instant rendered as an MJD for convenience.
static std::string mjd_string(const uhd::time_spec_t& ts)
{
    const long double mjd =
        40587.0L
        + (static_cast<long double>(ts.get_full_secs())
              + static_cast<long double>(ts.get_frac_secs()))
              / 86400.0L;
    std::ostringstream os;
    os << std::fixed << std::setprecision(14) << mjd;
    return os.str();
}

static std::string utc_string(const uhd::time_spec_t& ts)
{
    const std::time_t t = static_cast<std::time_t>(ts.get_full_secs());
    std::tm tm_buf{};
    gmtime_r(&t, &tm_buf);
    std::ostringstream os;
    os << std::put_time(&tm_buf, "%Y-%m-%d-%H:%M:%S");
    return os.str();
}

// ---------------------------------------------------------------------------
// Per-stream configuration and statistics
// ---------------------------------------------------------------------------

struct StreamCfg
{
    size_t index = 0;
    std::vector<size_t> channels; // USRP channels, in polarisation order
    double rate      = 0.0;       // actual rate reported by UHD
    double sky_freq  = 0.0;       // header FREQ, in Hz
    double bw_sign   = 1.0;       // -1 for an inverted band
    std::string key_str;
    key_t key  = 0;
    int shift  = 8;               // bit shift for the 8-bit path
    int cpu    = -1;              // CPU to pin the worker to, or -1
};

struct StreamStats
{
    std::atomic<unsigned long long> samps{0};
    std::atomic<unsigned long long> dropped{0};
    std::atomic<unsigned long long> overflows{0};
    std::atomic<unsigned long long> scanned{0};
    std::atomic<int> peak[MAX_POL];
    std::atomic<unsigned long long> clip[MAX_POL];
    std::atomic<bool> started{false};
    std::atomic<bool> finished{false};
    uhd::time_spec_t first_time;

    StreamStats()
    {
        for (size_t p = 0; p < MAX_POL; p++) {
            peak[p] = 0;
            clip[p] = 0;
        }
    }
};

// ---------------------------------------------------------------------------
// Aligned receive buffers
// ---------------------------------------------------------------------------

struct AlignedFree
{
    void operator()(void* p) const { std::free(p); }
};
using AlignedBuf = std::unique_ptr<uint32_t[], AlignedFree>;

static AlignedBuf make_buf(size_t n)
{
    void* p = nullptr;
    if (posix_memalign(&p, 64, n * sizeof(uint32_t)) != 0)
        throw std::bad_alloc();
    std::memset(p, 0, n * sizeof(uint32_t));
    return AlignedBuf(static_cast<uint32_t*>(p));
}

#ifdef __linux__
static void pin_thread(int cpu)
{
    if (cpu < 0)
        return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0)
        note("Warning: could not set CPU affinity to " + std::to_string(cpu));
}
#else
static void pin_thread(int) {}
#endif

// Ring occupancy as a phrase, so an overflow or gap message says at a glance
// whether the reader was the bottleneck or the receive path was.
static std::string ring_note(DadaWriter* dada)
{
    if (dada == nullptr)
        return "";
    const uint64_t nb = dada->nbufs();
    if (nb == 0)
        return "";
    const uint64_t nf = dada->nfull();
    std::string s     = " (ring " + std::to_string(nf) + "/" + std::to_string(nb);
    if (nf + 1 >= nb)
        return s + ", FULL: the reader is the bottleneck)";
    return s + ": not ring back-pressure, the receive path is the bottleneck)";
}

// ---------------------------------------------------------------------------
// Receive worker: one per DADA buffer
// ---------------------------------------------------------------------------

static void rx_worker(const StreamCfg& cfg,
    uhd::rx_streamer::sptr rx,
    DadaWriter* dada,
    HeaderKV header_kv,
    size_t spb,
    int nbit,
    unsigned long long nsamps_requested,
    bool continue_on_bad_packet,
    bool provenance,
    double max_gap_secs,
    bool priority,
    size_t stat_stride,
    StreamStats* st)
{
    const size_t npol         = cfg.channels.size();
    const size_t sample_bytes = npol * (nbit == 16 ? 4u : 2u);

    try {
        if (priority)
            uhd::set_thread_priority_safe();
        pin_thread(cfg.cpu);

        std::vector<AlignedBuf> bufs;
        std::vector<void*> buff_ptrs;
        std::vector<const uint32_t*> src(npol, nullptr);
        for (size_t p = 0; p < npol; p++) {
            bufs.push_back(make_buf(spb));
            buff_ptrs.push_back(bufs.back().get());
        }

        uhd::rx_metadata_t md;
        bool first          = true;
        int64_t t0_ticks    = 0;
        int64_t next_index  = 0;

        while (!stop_signal_called
               && (nsamps_requested == 0 || st->samps.load() < nsamps_requested)) {

            const double timeout = first ? 30.0 : 3.0;
            const size_t n       = rx->recv(buff_ptrs, spb, md, timeout, false);

            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) {
                if (stop_signal_called)
                    break;
                fail(cfg.key_str + ": timeout while streaming");
                break;
            }
            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
                st->overflows++;
                if (!continue_on_bad_packet) {
                    fail(cfg.key_str
                         + ": overflow -- host is not consuming data fast enough "
                           "(use --continue to zero-fill the gap and carry on)");
                    break;
                }
                note(cfg.key_str + ": overflow, will zero-fill the gap"
                     + ring_note(dada));
                continue;
            }
            if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
                if (continue_on_bad_packet) {
                    note(cfg.key_str + ": receiver error: " + md.strerror());
                    continue;
                }
                fail(cfg.key_str + ": receiver error: " + md.strerror());
                break;
            }
            if (n == 0)
                continue;

            const int64_t idx =
                md.has_time_spec ? md.time_spec.to_ticks(cfg.rate) : (t0_ticks + next_index);

            if (first) {
                t0_ticks         = idx;
                next_index       = 0;
                st->first_time   = md.time_spec;
                st->started      = true;
                first            = false;

                if (dada != nullptr) {
                    set_kv(header_kv, "UTC_START", utc_string(md.time_spec));
                    set_kv(header_kv,
                        "PICOSECONDS",
                        std::to_string(static_cast<uint64_t>(
                            llround(md.time_spec.get_frac_secs() * 1e12))));
                    set_kv(header_kv, "MJD_START", mjd_string(md.time_spec));
                    // The .txt always carries the full header including
                    // provenance; the ring gets what the reader asked for.
                    const std::string full =
                        render_header(header_kv, dada->header_bytes(), true);
                    std::ofstream dbg("dada_header_" + cfg.key_str + ".txt");
                    dbg << full.c_str(); // stop at the NUL padding
                    dbg.close();
                    dada->write_header(provenance
                            ? full
                            : render_header(header_kv, dada->header_bytes(), false));
                }

                std::lock_guard<std::mutex> lock(console_mutex);
                std::cout << boost::format(
                                 "  %s: first frame, %u samples, %ld full secs, "
                                 "%.09f frac secs")
                                 % cfg.key_str % n % md.time_spec.get_full_secs()
                                 % md.time_spec.get_frac_secs()
                          << std::endl;
            }

            // Counted here, not after the DADA write: this is the rate coming
            // off the radio.  If the ring is jammed the worker never gets back
            // to recv() and the displayed rate correctly falls to zero -- the
            // ring occupancy on the same line is what says why.
            st->samps += n;

            const int64_t rel = idx - t0_ticks;

            if (rel > next_index) {
                const int64_t gap      = rel - next_index;
                const double gap_secs  = double(gap) / cfg.rate;
                const double gap_mbytes = double(gap) * sample_bytes / 1e6;
                st->dropped += static_cast<unsigned long long>(gap);
                if (!continue_on_bad_packet) {
                    fail(cfg.key_str + ": gap of " + std::to_string(gap)
                         + " samples ("
                         + (boost::format("%.3f") % gap_secs).str()
                         + " s) detected" + ring_note(dada)
                         + "; use --continue to zero-fill");
                    break;
                }
                // Zero-filling happens in this thread, so it costs one memset
                // of the whole gap before the next recv().  For a large gap
                // that is long enough to cause the NEXT gap, which is longer
                // still: left alone it diverges.  Past --max-gap, stop instead.
                if (max_gap_secs > 0.0 && gap_secs > max_gap_secs) {
                    fail(cfg.key_str + ": gap of "
                         + (boost::format("%.3f") % gap_secs).str() + " s exceeds "
                         + "--max-gap " + (boost::format("%.3f") % max_gap_secs).str()
                         + " s" + ring_note(dada) + ". Zero-filling it would mean "
                         + (boost::format("%.0f") % gap_mbytes).str()
                         + " MB of writes in the receive thread, which would make "
                           "the next gap larger again. Stopping rather than "
                           "spiralling; pass --max-gap 0 to disable this limit.");
                    break;
                }
                note(cfg.key_str + ": zero-filling " + std::to_string(gap)
                     + " samples (" + (boost::format("%.3f") % gap_secs).str()
                     + " s, " + (boost::format("%.0f") % gap_mbytes).str() + " MB)");
                if (dada != nullptr)
                    emit(*dada,
                        static_cast<size_t>(gap),
                        sample_bytes,
                        [&](char* dst, size_t, size_t m) {
                            std::memset(dst, 0, m * sample_bytes);
                        });
                next_index = rel;
            } else if (rel < next_index) {
                note(cfg.key_str + ": non-monotonic timestamp ("
                     + std::to_string(next_index - rel) + " samples back), ignoring");
            }

            if (dada != nullptr) {
                emit(*dada, n, sample_bytes, [&](char* dst, size_t off, size_t m) {
                    for (size_t p = 0; p < npol; p++)
                        src[p] = bufs[p].get() + off;
                    if (nbit == 16) {
                        if (npol == 2)
                            interleave2_u32(
                                reinterpret_cast<uint32_t*>(dst), src[0], src[1], m);
                        else
                            interleave_n_u32(
                                reinterpret_cast<uint32_t*>(dst), src.data(), npol, m);
                    } else {
                        if (npol == 2)
                            interleave2_trunc(reinterpret_cast<uint16_t*>(dst),
                                src[0],
                                src[1],
                                m,
                                cfg.shift);
                        else
                            interleave_n_trunc(reinterpret_cast<uint16_t*>(dst),
                                src.data(),
                                npol,
                                m,
                                cfg.shift);
                    }
                });
            }

            next_index += static_cast<int64_t>(n);

            // Level statistics on a strided subsample -- scanning every sample
            // at these rates would cost more than the interleave itself.
            if (stat_stride > 0) {
                size_t scanned = 0;
                for (size_t p = 0; p < npol; p++) {
                    const uint32_t* b = bufs[p].get();
                    int local_peak         = 0;
                    unsigned long long clp = 0;
                    size_t cnt             = 0;
                    for (size_t k = 0; k < n; k += stat_stride, cnt++) {
                        const uint32_t w = b[k];
                        const int i      = std::abs(static_cast<int>(
                            static_cast<int16_t>(static_cast<uint16_t>(w & 0xffffu))));
                        const int q = std::abs(static_cast<int>(
                            static_cast<int16_t>(static_cast<uint16_t>(w >> 16))));
                        const int m = std::max(i, q);
                        if (m > local_peak)
                            local_peak = m;
                        if (m > 32440) // 0.99 full scale
                            clp++;
                    }
                    scanned = cnt;
                    if (local_peak > st->peak[p].load(std::memory_order_relaxed))
                        st->peak[p].store(local_peak, std::memory_order_relaxed);
                    st->clip[p] += clp;
                }
                st->scanned += scanned;
            }
        }

        uhd::stream_cmd_t stop_cmd(uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS);
        rx->issue_stream_cmd(stop_cmd);

        // Drain whatever is still in flight so the streamer shuts down cleanly,
        // but bound it in wall-clock time rather than in iterations: a deep
        // socket backlog must not turn Ctrl-C into a long wait.
        const auto drain_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);
        while (std::chrono::steady_clock::now() < drain_deadline) {
            uhd::rx_metadata_t drain_md;
            if (rx->recv(buff_ptrs, spb, drain_md, 0.1, false) == 0)
                break;
        }
    } catch (const StopRequested&) {
        // Asked to stop while waiting for room in the ring; nothing to report.
    } catch (const std::exception& e) {
        fail(std::string(cfg.key_str) + ": " + e.what());
    }
    st->finished = true;
}

// ---------------------------------------------------------------------------
// Sensor lock check (from usrp_to_vrt)
// ---------------------------------------------------------------------------

typedef std::function<uhd::sensor_value_t(const std::string&)> get_sensor_fn_t;

static bool check_locked_sensor(std::vector<std::string> sensor_names,
    const char* sensor_name,
    get_sensor_fn_t get_sensor_fn,
    double setup_time)
{
    if (std::find(sensor_names.begin(), sensor_names.end(), sensor_name)
        == sensor_names.end())
        return false;

    auto setup_timeout = std::chrono::steady_clock::now()
                         + std::chrono::milliseconds(int64_t(setup_time * 1000));
    bool lock_detected = false;

    std::cout << boost::format("Waiting for \"%s\": ") % sensor_name;
    std::cout.flush();

    while (true) {
        if (lock_detected and (std::chrono::steady_clock::now() > setup_timeout)) {
            std::cout << " locked." << std::endl;
            break;
        }
        if (get_sensor_fn(sensor_name).to_bool()) {
            std::cout << "+";
            std::cout.flush();
            lock_detected = true;
        } else {
            if (std::chrono::steady_clock::now() > setup_timeout) {
                std::cout << std::endl;
                throw std::runtime_error(
                    str(boost::format(
                            "timed out waiting for consecutive locks on sensor \"%s\"")
                        % sensor_name));
            }
            std::cout << "_";
            std::cout.flush();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << std::endl;
    return true;
}

// ---------------------------------------------------------------------------

int UHD_SAFE_MAIN(int argc, char* argv[])
{
    std::string args, stream_spec, rate_list, freq_list, ant_list, bw_list, subdev,
        key_list, invert_list, shift_list, affinity_list, source_name, ra_str, dec_str,
        telescope, receiver, instrument, header_file, start_reception;
    size_t spb, total_num_samps, stat_stride;
    int nbit;
    double total_time, setup_time, pps_offset, start_delay, max_gap_secs, file_seconds;



    po::options_description desc("Allowed options");
    // clang-format off
    desc.add_options()
        ("help,h", "help message")
        ("args", po::value<std::string>(&args)->default_value(""),
            "multi uhd device address args. Everything the device must be told before it is "
            "made goes here: master_clock_rate, clock_source, time_source, use_dpdk. "
            "For X440 dual-rate operation master_clock_rate takes two values and they are "
            "positional BY DAUGHTERBOARD, not by --streams order: the first applies to "
            "daughterboard 0 (subdev A:) and the second to daughterboard 1 (subdev B:), "
            "e.g. master_clock_rate=300e6,450e6 for A at 300 Msps and B at 450")
        ("subdev", po::value<std::string>(&subdev), "subdevice specification")
        ("streams", po::value<std::string>(&stream_spec)->default_value("A:0,A:2;A:1,A:3;B:0,B:1"),
            "channel groups: one DADA buffer per group, polarisations comma separated in pol order, groups semicolon separated. "
            "Each port may be written as a UHD channel number (3), a subdev entry (A:3) or a front-panel label (0/3 = DB 0 / RF 3), "
            "and the three spellings may be mixed. Order is free, so wire order need not be channel order")
        ("key", po::value<std::string>(&key_list)->default_value("c2c2,c4c4,c6c6"), "DADA key per stream (hex)")
        ("rate", po::value<std::string>(&rate_list)->default_value("300e6,300e6,450e6"), "sample rate per stream (or per channel)")
        ("freq", po::value<std::string>(&freq_list)->required(), "centre frequency per stream in Hz")
        ("ant", po::value<std::string>(&ant_list), "antenna per channel (or per stream)")
        ("bw", po::value<std::string>(&bw_list), "analog frontend filter bandwidth per channel (or per stream) in Hz")
        ("invert", po::value<std::string>(&invert_list)->default_value("0"),
            "per stream: 1 if the band is spectrally inverted, which writes a negative BW into the header")
        ("nbit", po::value<int>(&nbit)->default_value(16), "bits per component written to DADA: 16 (byte-for-byte sc16) or 8 (truncated)")
        ("shift", po::value<std::string>(&shift_list)->default_value("8"),
            "per stream: when --nbit 8, keep bits [shift+7:shift] of each int16 component (8 keeps the top byte)")
        ("spb", po::value<size_t>(&spb)->default_value(16384), "samples per receive buffer per channel")
        ("nsamps", po::value<size_t>(&total_num_samps)->default_value(0), "total samples to receive per stream")
        ("duration", po::value<double>(&total_time)->default_value(0), "seconds to receive")
        ("start-time", po::value<std::string>(&start_reception), "start at this absolute UTC time (unix seconds or ISO 8601)")
        ("start-delay", po::value<double>(&start_delay)->default_value(3.0), "seconds from now to the aligned start when --start-time is not given")
        ("pps-offset", po::value<double>(&pps_offset)->default_value(0),
            "offset of the PPS pulse in sec, used when the time source from --args is not internal")
        ("setup", po::value<double>(&setup_time)->default_value(1.0), "seconds of setup time")
        ("skip-lo", "skip checking LO lock status")
        ("file-seconds", po::value<double>(&file_seconds)->default_value(10.0),
            "seconds of data per output file, written to the header as FILE_SIZE in bytes "
            "(rounded down to a whole time sample). 0 omits FILE_SIZE, in which case a "
            "reader such as dada_dbdisk writes one unbounded file")
        ("source", po::value<std::string>(&source_name)->default_value("undefined"), "SOURCE header value")
        ("ra", po::value<std::string>(&ra_str)->default_value("00:00:00.000"), "RA header value, hh:mm:ss.sss")
        ("dec", po::value<std::string>(&dec_str)->default_value("+00:00:00.000"), "DEC header value, +dd:mm:ss.sss")
        ("telescope", po::value<std::string>(&telescope)->default_value("DWL"), "TELESCOPE header value")
        ("receiver", po::value<std::string>(&receiver)->default_value("USRP"), "RECEIVER header value")
        ("instrument", po::value<std::string>(&instrument)->default_value("dspsr"), "INSTRUMENT header value")
        ("no-provenance",
            "omit the USRP_* provenance block from the DADA header. The full header, "
            "provenance included, is still written to dada_header_<key>.txt")
        ("header-file", po::value<std::string>(&header_file), "file of extra KEY VALUE lines merged into every DADA header (wins over the options above)")
        ("affinity", po::value<std::string>(&affinity_list), "CPU to pin each receive thread to, one per stream")
        ("priority", "enable realtime scheduling on the receive threads")
        ("progress", "periodically display short-term bandwidth and levels")
        ("stat-stride", po::value<size_t>(&stat_stride)->default_value(64), "sample stride used for the level statistics (0 disables)")
        ("stats", "show average bandwidth on exit")
        ("continue", "zero-fill gaps and keep going instead of aborting on an overflow or lost packet")
        ("max-gap", po::value<double>(&max_gap_secs)->default_value(0.5),
            "with --continue, abort anyway if a single gap exceeds this many seconds. "
            "Zero-filling costs a memset of the whole gap in the receive thread, so a large "
            "gap makes the next one larger; 0 disables the limit")
        ("null", "run without writing to DADA")
    ;
    // clang-format on

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).positional({}).run(), vm);

    if (vm.count("help") || argc < 2) {
        std::cout << boost::format("USRP samples to DADA. %s") % desc << std::endl;
        std::cout
            << std::endl
            << "Streams groups of USRP RX channels straight into psrdada ring\n"
               "buffers, one buffer per group, with the channels of a group\n"
               "interleaved as polarisations.  No VRT, no ZMQ, no scaling.\n"
            << std::endl;
        return ~0;
    }
    po::notify(vm);

    const bool progress               = vm.count("progress") > 0;
    const bool stats                  = vm.count("stats") > 0;
    const bool null_mode              = vm.count("null") > 0;
    const bool continue_on_bad_packet = vm.count("continue") > 0;
    const bool provenance             = vm.count("no-provenance") == 0;
    const bool priority               = vm.count("priority") > 0;

    if (nbit != 16 && nbit != 8)
        throw std::runtime_error("--nbit must be 16 or 8");

    // ---- channel groups ---------------------------------------------------
    // Parsed as text here; the ports are only resolved to UHD channel numbers
    // once the device exists and its RX subdev spec is known.
    std::vector<std::vector<std::string>> group_tokens;
    size_t n_chans = 0;
    for (const auto& g : split_str(stream_spec, ";")) {
        std::vector<std::string> toks = split_str(g, "\"',");
        if (toks.empty())
            continue;
        if (toks.size() > MAX_POL)
            throw std::runtime_error("at most " + std::to_string(MAX_POL)
                                     + " polarisations per stream");
        n_chans += toks.size();
        group_tokens.push_back(toks);
    }
    if (group_tokens.empty())
        throw std::runtime_error("no channel groups given");

    const size_t n_streams = group_tokens.size();

    const auto rates    = parse_doubles(rate_list);
    const auto freqs    = parse_doubles(freq_list);
    const auto inverts  = parse_longs(invert_list);
    const auto shifts   = parse_longs(shift_list);
    const auto keys     = split_str(key_list, "\"',");
    const auto bws      = vm.count("bw") ? parse_doubles(bw_list) : std::vector<double>();
    const auto antennas = vm.count("ant") ? split_str(ant_list, "\"',") : std::vector<std::string>();
    const auto affinity = vm.count("affinity") ? parse_longs(affinity_list) : std::vector<long>();

    // Each stream owns its own ring buffer, so keys are one per stream exactly:
    // "one for all" would mean two writers on one ring.  Likewise --affinity,
    // which is one CPU per receive thread.
    if (keys.size() != n_streams)
        throw std::runtime_error(
            "--key: got " + std::to_string(keys.size())
            + (keys.size() == 1 ? " value, expected " : " values, expected ")
            + std::to_string(n_streams) + " (one DADA key per stream)");
    if (!affinity.empty() && affinity.size() != n_streams)
        throw std::runtime_error(
            "--affinity: got " + std::to_string(affinity.size())
            + (affinity.size() == 1 ? " value, expected " : " values, expected ")
            + std::to_string(n_streams) + " (one CPU per stream)");

    check_list(rates, "--rate", n_streams, n_chans, true);
    check_list(freqs, "--freq", n_streams, n_chans, true);
    check_list(inverts, "--invert", n_streams, n_chans, false);
    check_list(shifts, "--shift", n_streams, n_chans, false);
    if (!bws.empty())
        check_list(bws, "--bw", n_streams, n_chans, true);
    if (!antennas.empty())
        check_list(antennas, "--ant", n_streams, n_chans, true);

    // ---- device -----------------------------------------------------------
    // Supply a default only for keys --args did not set.  Without DPDK the
    // socket buffer and frame count are the main defence against scheduler
    // stalls, so they have to be tunable from the command line.
    {
        const auto has_key = [&args](const std::string& k) {
            const size_t p = args.find(k + "=");
            return p != std::string::npos && (p == 0 || args[p - 1] == ',' || args[p - 1] == ' ');
        };
        for (const auto& kv : {std::string("num_recv_frames=1024")}) {
            const std::string key = kv.substr(0, kv.find('='));
            if (!has_key(key))
                args = args.empty() ? kv : kv + "," + args;
        }
    }

    std::cout << std::endl
              << boost::format("Creating the usrp device with: %s...") % args << std::endl;
    uhd::usrp::multi_usrp::sptr usrp = uhd::usrp::multi_usrp::make(args);

    // The clock and time sources are whatever --args asked for: on the X440
    // they cannot be set again afterwards, so this only reads them back and
    // lets everything below adapt to what the device actually has.
    const std::string clock_source = usrp->get_clock_source(0);
    const std::string time_source  = usrp->get_time_source(0);
    std::cout << "Clock source is " << clock_source << std::endl;
    std::cout << "Time source is " << time_source << std::endl;

    if (vm.count("subdev"))
        usrp->set_rx_subdev_spec(subdev);

    std::cout << boost::format("Using Device: %s") % usrp->get_pp_string() << std::endl;

    // ---- resolve the front-panel ports to UHD channels --------------------
    const uhd::usrp::subdev_spec_t rx_spec = usrp->get_rx_subdev_spec(0);
    std::cout << "RX subdev spec: " << rx_spec.to_string() << std::endl;

    std::vector<std::vector<size_t>> groups;
    std::vector<size_t> all_channels;
    std::vector<std::string> chan_subdev(usrp->get_rx_num_channels());
    std::vector<std::string> chan_panel(usrp->get_rx_num_channels());
    for (size_t i = 0; i < rx_spec.size() && i < chan_subdev.size(); i++) {
        chan_subdev[i] = rx_spec[i].db_name + ":" + rx_spec[i].sd_name;
        chan_panel[i]  = panel_label(rx_spec[i].db_name, rx_spec[i].sd_name);
    }

    for (const auto& toks : group_tokens) {
        std::vector<size_t> chans;
        for (const auto& t : toks) {
            const size_t c = resolve_port(t, rx_spec);
            if (c >= usrp->get_rx_num_channels())
                throw std::runtime_error("port '" + t + "' resolves to channel "
                                         + std::to_string(c)
                                         + ", which this device does not have");
            chans.push_back(c);
        }
        for (size_t c : chans)
            all_channels.push_back(c);
        groups.push_back(chans);
    }
    {
        std::vector<size_t> sorted = all_channels;
        std::sort(sorted.begin(), sorted.end());
        if (std::adjacent_find(sorted.begin(), sorted.end()) != sorted.end())
            throw std::runtime_error("a USRP channel appears in more than one stream");
    }

    // Print the wiring as resolved, so a swapped polarisation is visible in
    // the log rather than in the data six weeks later.
    std::cout << std::endl << "Port mapping:" << std::endl;
    for (size_t si = 0; si < groups.size(); si++) {
        for (size_t p = 0; p < groups[si].size(); p++) {
            const size_t c = groups[si][p];
            std::cout << boost::format(
                             "  stream %u pol %u  <-  %-10s %-6s chan %u")
                             % si % p
                             % (c < chan_panel.size() ? chan_panel[c] : std::string("?"))
                             % (c < chan_subdev.size() ? chan_subdev[c] : std::string("?"))
                             % c
                      << std::endl;
        }
    }

    // Both channels of a pair must sit on one daughterboard: on the X440 the
    // master clock rate is per daughterboard, and multi-tile sync is skipped
    // across daughterboards when the two rates differ.
    for (size_t si = 0; si < groups.size(); si++) {
        std::string db0;
        for (size_t c : groups[si]) {
            const std::string db =
                (c < chan_subdev.size() && !chan_subdev[c].empty())
                    ? chan_subdev[c].substr(0, chan_subdev[c].find(':'))
                    : std::string();
            if (db0.empty())
                db0 = db;
            else if (db != db0) {
                note("WARNING: stream " + std::to_string(si)
                     + " spans daughterboards " + db0 + " and " + db
                     + ".  Those channels cannot run at different master clock "
                       "rates, and their relative phase is not guaranteed across "
                       "retunes.  Check the cabling.");
                break;
            }
        }
    }

    // ---- per-channel configuration ---------------------------------------
    std::vector<StreamCfg> cfgs(n_streams);
    size_t chan_i = 0;
    for (size_t si = 0; si < n_streams; si++) {
        StreamCfg& cfg = cfgs[si];
        cfg.index      = si;
        cfg.channels   = groups[si];
        cfg.key_str    = keys[si];
        cfg.key        = static_cast<key_t>(std::stoul(keys[si], nullptr, 16));
        cfg.shift =
            static_cast<int>(shifts.size() == 1 ? shifts.front() : shifts[si]);
        cfg.bw_sign =
            ((inverts.size() == 1 ? inverts.front() : inverts[si]) != 0) ? -1.0 : 1.0;
        cfg.cpu = affinity.empty() ? -1 : static_cast<int>(affinity[si]);

        if (cfg.shift < 0 || cfg.shift > 8)
            throw std::runtime_error("--shift must be between 0 and 8");

        cfg.sky_freq = pick(freqs, si, chan_i, n_streams, n_chans);

        std::cout << std::endl
                  << boost::format("Stream %u -> DADA key %s, channels") % si
                         % cfg.key_str;
        for (size_t c : cfg.channels)
            std::cout << " " << c;
        std::cout << std::endl;

        for (size_t p = 0; p < cfg.channels.size(); p++, chan_i++) {
            const size_t channel = cfg.channels[p];

            const double rate = pick(rates, si, chan_i, n_streams, n_chans);
            usrp->set_rx_rate(rate, channel);
            const double actual_rate = usrp->get_rx_rate(channel);
            std::cout << boost::format(
                             "  ch %u: RX rate %f Msps requested, %f Msps actual")
                             % channel % (rate / 1e6) % (actual_rate / 1e6)
                      << std::endl;
            if (p == 0)
                cfg.rate = actual_rate;
            else if (std::abs(actual_rate - cfg.rate) > 1.0)
                throw std::runtime_error(
                    "channels within one stream must run at the same sample rate");

            const double tune_freq = cfg.sky_freq;
            if (tune_freq < 1e6)
                throw std::runtime_error("frequency should be given in Hz; "
                                         + std::to_string(tune_freq)
                                         + " Hz is probably not what you meant");
            // Direct sampling: no LO offset, no integer-N mode, so the request
            // is just the frequency.
            usrp->set_rx_freq(uhd::tune_request_t(tune_freq), channel);
            std::cout << boost::format("  ch %u: RX freq %f MHz requested, %f MHz actual")
                             % channel % (tune_freq / 1e6)
                             % (usrp->get_rx_freq(channel) / 1e6)
                      << std::endl;

            if (!bws.empty()) {
                const double b = pick(bws, si, chan_i, n_streams, n_chans);
                usrp->set_rx_bandwidth(b, channel);
                std::cout << boost::format("  ch %u: RX bandwidth %f MHz actual") % channel
                                 % (usrp->get_rx_bandwidth(channel) / 1e6)
                          << std::endl;
            }
            if (!antennas.empty()) {
                const std::string a =
                    pick(antennas, si, chan_i, n_streams, n_chans);
                usrp->set_rx_antenna(a, channel);
                std::cout << boost::format("  ch %u: antenna %s") % channel
                                 % usrp->get_rx_antenna(channel)
                          << std::endl;
            }
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(int64_t(1000 * setup_time)));

    // ---- lock checks ------------------------------------------------------
    if (!vm.count("skip-lo")) {
        for (size_t c : all_channels) {
            check_locked_sensor(
                usrp->get_rx_sensor_names(c),
                "lo_locked",
                [usrp, c](const std::string& name) { return usrp->get_rx_sensor(name, c); },
                setup_time);
        }
        // Driven by what --args actually configured, since we never set it.
        if (clock_source != "internal") {
            const char* sensor =
                (clock_source == "mimo") ? "mimo_locked" : "ref_locked";
            check_locked_sensor(
                usrp->get_mboard_sensor_names(0),
                sensor,
                [usrp](const std::string& name) { return usrp->get_mboard_sensor(name); },
                setup_time);
        }
    }

    // ---- device time ------------------------------------------------------
    //
    // The time SOURCE is never set here -- on the X440 it cannot be changed
    // after the device has been made with it in --args.  What is set here is
    // the device's time COUNTER, which is a different thing and is what puts a
    // meaningful UTC_START in the DADA header.  Which method is used follows
    // from the time source --args configured:
    //
    //   gpsdo     align the counter to the gps_time sensor on a PPS edge
    //   external  align the counter to the host's integer second on a PPS edge
    //   (others)  same PPS alignment, since a non-internal source implies one
    //   internal  no PPS available, so seed from the host clock and accept it
    //
    std::cout << std::endl << "Setting device timestamp..." << std::endl;
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    usrp->set_time_now(uhd::time_spec_t(time_now.tv_sec, (double)time_now.tv_usec / 1e6));

    uint32_t timestamp_calibration_time = 0;

    if (time_source == "gpsdo") {
        signed gps_seconds;
        long long pps_seconds;
        do {
            std::cout << "Aligning the device time to GPS time..." << std::endl;
            uhd::sensor_value_t gps_time = usrp->get_mboard_sensor("gps_time");
            usrp->set_time_next_pps(uhd::time_spec_t(gps_time.to_int() + 1.0));
            std::this_thread::sleep_for(std::chrono::milliseconds(1100));
            gps_seconds = usrp->get_mboard_sensor("gps_time").to_int();
            pps_seconds = usrp->get_time_last_pps().to_ticks(1.0);
        } while (pps_seconds != gps_seconds);
        timestamp_calibration_time = (uint32_t)pps_seconds;
        std::cout << "GPS and UHD device time are aligned." << std::endl;
    } else if (time_source != "internal") {
        uint32_t usrp_seconds;
        do {
            gettimeofday(&time_now, nullptr);
            const int64_t integer_time = (int64_t)((double)time_now.tv_sec
                                                   + (double)time_now.tv_usec / 1e6 + 2.0
                                                   - pps_offset);
            usrp->set_time_unknown_pps(uhd::time_spec_t(integer_time, pps_offset));
            std::cout << "Waiting for PPS sync..." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(2100));
            gettimeofday(&time_now, nullptr);
            usrp_seconds = (uint32_t)usrp->get_time_now().get_full_secs();
        } while (usrp_seconds != (uint32_t)time_now.tv_sec);
        timestamp_calibration_time = usrp_seconds;
        std::cout << "PPS sync done." << std::endl;
    } else {
        note("WARNING: time source is \"internal\", so the device clock is only "
             "as good as this host's clock.  UTC_START will be correspondingly "
             "approximate.  Pass time_source=external (or gpsdo) in --args for "
             "PPS-aligned timestamps.");
    }

    std::cout << boost::format("UHD device time right now: %.6f seconds")
                     % usrp->get_time_now().get_real_secs()
              << std::endl;
    gettimeofday(&time_now, nullptr);
    std::cout << boost::format("PC clock time:             %.6f seconds")
                     % (time_now.tv_sec + (double)time_now.tv_usec / 1e6)
              << std::endl;

    // ---- streamers --------------------------------------------------------
    std::vector<uhd::rx_streamer::sptr> rx_streams(n_streams);
    for (size_t si = 0; si < n_streams; si++) {
        // The wire format is sc16 because that is all X4xx supports; when
        // --nbit 8 is given the truncation happens in the interleave.
        uhd::stream_args_t stream_args("sc16", "sc16");
        stream_args.channels = cfgs[si].channels;
        rx_streams[si]       = usrp->get_rx_stream(stream_args);
    }

    // ---- DADA -------------------------------------------------------------
    std::vector<std::unique_ptr<DadaWriter>> writers(n_streams);
    std::vector<HeaderKV> headers(n_streams);

    HeaderKV extra;
    if (vm.count("header-file"))
        extra = read_header_file(header_file);

    for (size_t si = 0; si < n_streams; si++) {
        const StreamCfg& cfg      = cfgs[si];
        const size_t npol         = cfg.channels.size();
        const size_t sample_bytes = npol * (nbit == 16 ? 4u : 2u);

        if (!null_mode) {
            writers[si].reset(new DadaWriter(cfg.key, "usrp_to_dada_" + cfg.key_str));
            writers[si]->connect();
            if (writers[si]->block_bytes() % sample_bytes != 0)
                throw std::runtime_error(
                    "DADA buffer " + cfg.key_str + ": block size "
                    + std::to_string(writers[si]->block_bytes())
                    + " is not a multiple of the time-sample size "
                    + std::to_string(sample_bytes)
                    + "; recreate the ring with dada_db -b <multiple>");
            // Say how much time the ring actually holds.  A ring that only
            // buffers a few tens of milliseconds will jam the moment the
            // reader pauses, and that is worth knowing before the observation
            // rather than from a wall of overflows during it.
            const double bytes_per_sec = cfg.rate * sample_bytes;
            const double block_secs =
                double(writers[si]->block_bytes()) / bytes_per_sec;
            const uint64_t nb = writers[si]->nbufs();
            std::cout << boost::format(
                             "DADA %s: %.2f MiB/block = %.4f s, header %llu bytes, "
                             "%.1f blocks/s at %.3f Msps (%.2f GB/s)")
                             % cfg.key_str
                             % (writers[si]->block_bytes() / 1048576.0) % block_secs
                             % (unsigned long long)writers[si]->header_bytes()
                             % (1.0 / block_secs) % (cfg.rate / 1e6)
                             % (bytes_per_sec / 1e9)
                      << std::endl;
            if (nb > 0) {
                const double ring_secs = block_secs * double(nb);
                std::cout << boost::format(
                                 "        %llu blocks -> the ring holds %.3f s "
                                 "(%.2f GiB)")
                                 % (unsigned long long)nb % ring_secs
                                 % (double(nb) * writers[si]->block_bytes() / 1073741824.0)
                          << std::endl;
                if (file_seconds > 0.0)
                    std::cout << boost::format(
                                     "        FILE_SIZE %llu bytes = %.2f GB per "
                                     "file (%.1f s)")
                                     % (unsigned long long)(llround(cfg.rate * file_seconds)
                                                            * sample_bytes)
                                     % (llround(cfg.rate * file_seconds) * sample_bytes
                                           / 1e9)
                                     % file_seconds
                              << std::endl;
                if (ring_secs < 0.5)
                    note("WARNING: DADA buffer " + cfg.key_str + " holds only "
                         + (boost::format("%.3f") % ring_secs).str()
                         + " s of data.  It will fill the moment the reader "
                           "pauses; give dada_db a larger -n (number of blocks).");
            }
        }

        HeaderKV kv;
        std::ostringstream chan_str, port_str;
        for (size_t p = 0; p < npol; p++) {
            const size_t c = cfg.channels[p];
            chan_str << (p ? "," : "") << c;
            port_str << (p ? "," : "")
                     << (c < chan_subdev.size() && !chan_subdev[c].empty()
                                ? chan_subdev[c] + "(" + chan_panel[c] + ")"
                                : std::string("?"));
        }

        set_kv(kv, "HEADER", "DADA",
            "Distributed aquisition and data analysis");
        set_kv(kv, "HDR_VERSION", "1.0", "Version of this ASCII header");
        set_kv(kv, "HDR_SIZE",
            std::to_string((unsigned long long)(null_mode
                                                    ? 4096
                                                    : writers[si]->header_bytes())),
            "Size of the header in bytes");

        if (file_seconds > 0.0) {
            // Whole time samples, so a file never splits mid-sample.
            const unsigned long long fsz =
                (unsigned long long)(llround(cfg.rate * file_seconds)) * sample_bytes;
            add_blank(kv);
            add_section(kv, "DADA parameters");
            set_kv(kv, "FILE_SIZE", std::to_string(fsz),
                "bytes per output file, see --file-seconds");
        }

        add_blank(kv);
        add_section(kv, "time of the rising edge of the first time sample");
        // Filled in by the worker from the first frame; placeholders here so
        // that they appear in the right place in the rendered header.
        set_kv(kv, "UTC_START", "unset", "yyyy-mm-dd-hh:mm:ss");
        set_kv(kv, "PICOSECONDS", "0", "fraction of a second after UTC_START");
        set_kv(kv, "MJD_START", "unset", "MJD equivalent to the start UTC");
        set_kv(kv, "OBS_OFFSET", "0", "bytes offset from the start MJD/UTC");

        add_blank(kv);
        add_section(kv, "description of the source");
        set_kv(kv, "SOURCE", source_name, "name of the astronomical source");
        set_kv(kv, "RA", ra_str, "Right Ascension of the source");
        set_kv(kv, "DEC", dec_str, "Declination of the source");

        add_blank(kv);
        add_section(kv, "description of the instrument");
        set_kv(kv, "TELESCOPE", telescope, "telescope name");
        set_kv(kv, "INSTRUMENT", instrument, "instrument name");
        set_kv(kv, "RECEIVER", receiver, "receiver name");
        set_kv(kv, "FREQ", (boost::format("%.6f") % (cfg.sky_freq / 1e6)).str(),
            "centre frequency in MHz");
        set_kv(kv, "BW",
            (boost::format("%.6f") % (cfg.bw_sign * cfg.rate / 1e6)).str(),
            "bandwidth in MHz (-ve for lower sideband)");
        set_kv(kv, "TSAMP", (boost::format("%.12f") % (1e6 / cfg.rate)).str(),
            "sampling interval in microseconds");

        add_blank(kv);
        set_kv(kv, "NBIT", std::to_string(nbit), "number of bits per sample");
        set_kv(kv, "NDIM", "2", "dimension of samples (2=complex, 1=real)");
        set_kv(kv, "NPOL", std::to_string(npol),
            "number of polarizations observed");
        set_kv(kv, "NCHAN", "1", "number of channels here");
        set_kv(kv, "DSB", "1", "1 = both sidebands present about FREQ");
        // RESOLUTION must stay 1.  The byte size of one time sample looks more
        // correct, but psrdada's dada_client rounds its transfer size up to a
        // multiple of it, which overruns a buffer sized before the rounding --
        // dada_dbdisk and dada_dbnull then die with "double free or corruption".
        // dspsr does not use dada_client and so never saw it.
        set_kv(kv, "RESOLUTION", "1",
            "byte granularity; keep at 1, see the source");
        set_kv(kv, "BYTES_PER_SECOND",
            std::to_string((unsigned long long)llround(cfg.rate * sample_bytes)),
            "data rate of this buffer");

        add_blank(kv, true);
        add_section(kv, "usrp_to_dada provenance", true);
        set_prov(kv, "USRP_CHAN", chan_str.str(),
            "UHD channels, in polarisation order");
        set_prov(kv, "USRP_PORTS", port_str.str(), "subdev(front panel) per pol");
        set_prov(kv, "USRP_FS", (boost::format("%.6f") % cfg.rate).str(),
            "actual RX rate reported by UHD, in Hz");
        // These three deliberately avoid containing FREQ or SOURCE: psrdada's
        // ascii_header_get matches keys by substring.
        set_prov(kv, "USRP_TUNE",
            (boost::format("%.6f") % (usrp->get_rx_freq(cfg.channels[0]) / 1e6)).str(),
            "actual RX centre frequency, in MHz");
        set_prov(kv, "USRP_CLK", clock_source, "clock source, as read back");
        set_prov(kv, "USRP_TIME", time_source, "time source, as read back");
        set_prov(kv, "OTW_FORMAT", "sc16", "over-the-wire sample format");
        if (nbit == 8)
            set_prov(kv, "BIT_SHIFT", std::to_string(cfg.shift),
                "int16 bits [shift+7:shift] kept");
        if (timestamp_calibration_time != 0)
            set_prov(kv, "USRP_TCAL",
                std::to_string(timestamp_calibration_time),
                "unix second at which the device time was aligned");

        // Overrides land on the existing entry, keeping its comment and its
        // place; genuinely new keys are appended under their own heading.
        HeaderKV added;
        for (const auto& e : extra) {
            const bool known = std::any_of(kv.begin(), kv.end(), [&](const HeaderEntry& x) {
                return !x.key.empty() && x.key == e.key;
            });
            if (known)
                set_kv(kv, e.key, e.value);
            else
                added.push_back(e);
        }
        if (!added.empty()) {
            add_blank(kv);
            add_section(kv, "from --header-file");
            for (const auto& e : added)
                set_kv(kv, e.key, e.value);
        }

        headers[si] = kv;

        // Fail now rather than one frame into the observation.
        if (!null_mode) {
            HeaderKV probe = kv;
            set_kv(probe, "UTC_START", "2000-01-01-00:00:00");
            set_kv(probe, "PICOSECONDS", "999999999999");
            set_kv(probe, "MJD_START", "61000.00000000000000");
            render_header(probe, writers[si]->header_bytes(), provenance);
        }
    }

    // ---- common start time ------------------------------------------------
    uhd::time_spec_t start_spec;
    if (vm.count("start-time")) {
        double unix_start = 0.0;
        try {
            unix_start = boost::lexical_cast<double>(start_reception);
        } catch (const boost::bad_lexical_cast&) {
            std::string s = start_reception;
            const size_t t_pos = s.find('T');
            if (t_pos != std::string::npos)
                s[t_pos] = ' ';
            const size_t z_pos = s.find('Z');
            if (z_pos != std::string::npos)
                s.erase(z_pos, 1);
            std::tm tm_buf{};
            std::istringstream is(s);
            is >> std::get_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
            if (is.fail())
                throw std::runtime_error("could not parse --start-time " + start_reception);
            unix_start = static_cast<double>(timegm(&tm_buf));
        }
        start_spec = uhd::time_spec_t(int64_t(unix_start), unix_start - int64_t(unix_start));
    } else {
        start_spec = uhd::time_spec_t(
            (int64_t)std::ceil(usrp->get_time_now().get_real_secs() + start_delay));
    }

    const double lead = start_spec.get_real_secs() - usrp->get_time_now().get_real_secs();
    if (lead < 0.2)
        throw std::runtime_error("start time is in the past or too close (lead "
                                 + std::to_string(lead) + " s)");
    std::cout << std::endl
              << boost::format("Starting all %u streams at %s (%.6f, in %.3f s)")
                     % n_streams % utc_string(start_spec) % start_spec.get_real_secs()
                     % lead
              << std::endl;

    // ---- go ---------------------------------------------------------------
    std::signal(SIGINT, &sig_int_handler);
    std::signal(SIGTERM, &sig_int_handler);

    std::vector<std::unique_ptr<StreamStats>> stats_v(n_streams);
    for (size_t si = 0; si < n_streams; si++)
        stats_v[si].reset(new StreamStats());

    // Issue every stream command from this thread, before any worker touches
    // its streamer, so that all streams share one start tick.
    for (size_t si = 0; si < n_streams; si++) {
        uhd::stream_cmd_t cmd(uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS);
        cmd.stream_now = false;
        cmd.time_spec  = start_spec;
        rx_streams[si]->issue_stream_cmd(cmd);
    }

    std::vector<std::thread> threads;
    for (size_t si = 0; si < n_streams; si++) {
        const unsigned long long nreq =
            total_time > 0 ? (unsigned long long)(total_time * cfgs[si].rate)
                           : (unsigned long long)total_num_samps;
        threads.emplace_back(rx_worker,
            std::cref(cfgs[si]),
            rx_streams[si],
            writers[si].get(),
            headers[si],
            spb,
            nbit,
            nreq,
            continue_on_bad_packet,
            provenance,
            max_gap_secs,
            priority,
            stat_stride,
            stats_v[si].get());
    }

    // ---- progress ---------------------------------------------------------
    const auto run_start = std::chrono::steady_clock::now();
    std::vector<unsigned long long> last_samps(n_streams, 0);
    std::vector<unsigned long long> last_clip(n_streams * MAX_POL, 0);
    std::vector<unsigned long long> last_scanned(n_streams, 0);
    auto last_update = run_start;

    while (!stop_signal_called) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Workers exit on their own once they reach their sample count.
        bool all_finished = true;
        for (size_t si = 0; si < n_streams; si++)
            if (!stats_v[si]->finished.load())
                all_finished = false;
        if (all_finished)
            break;

        const auto now = std::chrono::steady_clock::now();
        if (progress && (now - last_update) > std::chrono::seconds(1)) {
            const double dt = std::chrono::duration<double>(now - last_update).count();
            last_update     = now;
            std::lock_guard<std::mutex> lock(console_mutex);
            for (size_t si = 0; si < n_streams; si++) {
                const StreamCfg& cfg = cfgs[si];
                StreamStats& st      = *stats_v[si];
                const unsigned long long s = st.samps.load();
                const double msps          = (s - last_samps[si]) / dt / 1e6;
                last_samps[si]             = s;

                const unsigned long long sc  = st.scanned.load();
                const double dscanned        = double(sc - last_scanned[si]);
                last_scanned[si]             = sc;

                std::cout << boost::format("  %s %7.2f Msps") % cfg.key_str % msps;
                for (size_t p = 0; p < cfg.channels.size(); p++) {
                    const int peak = st.peak[p].exchange(0);
                    const unsigned long long c = st.clip[p].load();
                    const double clip_pct =
                        dscanned > 0 ? 100.0 * double(c - last_clip[si * MAX_POL + p])
                                           / dscanned
                                     : 0.0;
                    last_clip[si * MAX_POL + p] = c;
                    const double dbfs =
                        peak > 0 ? 20.0 * log10(double(peak) / 32767.0) : -99.0;
                    std::cout << boost::format("  p%u %5.1f dBFS %4.1f%% clip") % p % dbfs
                                     % clip_pct;
                }
                std::cout << boost::format("  drop %llu  ovf %llu") % st.dropped.load()
                                 % st.overflows.load();
                // Ring occupancy: full == the reader is the bottleneck, and
                // the Msps above will read zero because we are blocked on it.
                if (writers[si]) {
                    const uint64_t nb = writers[si]->nbufs();
                    if (nb > 0)
                        std::cout << boost::format("  ring %llu/%llu%s")
                                         % (unsigned long long)writers[si]->nfull()
                                         % (unsigned long long)nb
                                         % (writers[si]->nfull() >= nb ? " FULL" : "");
                }
                std::cout << std::endl;
            }
        }
    }

    stop_signal_called = true;

    // Give the workers a moment, and name any that are wedged instead of
    // hanging in join() with nothing on screen.
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            bool all = true;
            for (size_t si = 0; si < n_streams; si++)
                if (!stats_v[si]->finished.load())
                    all = false;
            if (all)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        std::string stuck;
        for (size_t si = 0; si < n_streams; si++)
            if (!stats_v[si]->finished.load())
                stuck += (stuck.empty() ? "" : ", ") + cfgs[si].key_str;
        if (!stuck.empty())
            note("Still shutting down: " + stuck
                 + ".  Press Ctrl-C again to exit immediately (the ring buffers "
                   "will not be closed cleanly).");
    }

    for (auto& t : threads)
        if (t.joinable())
            t.join();

    for (size_t si = 0; si < n_streams; si++)
        if (writers[si])
            writers[si]->close();

    // ---- summary ----------------------------------------------------------
    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - run_start).count();
    std::cout << std::endl;
    for (size_t si = 0; si < n_streams; si++) {
        StreamStats& st = *stats_v[si];
        std::cout << boost::format("%s: %llu samples, %llu zero-filled, %llu overflows")
                         % cfgs[si].key_str % st.samps.load() % st.dropped.load()
                         % st.overflows.load()
                  << std::endl;
        if (st.started.load())
            std::cout << boost::format("      first sample at %s + %.09f s")
                             % utc_string(st.first_time) % st.first_time.get_frac_secs()
                      << std::endl;
    }
    if (stats)
        std::cout << boost::format("Elapsed %.3f s") % elapsed << std::endl;

    if (fatal_error) {
        std::cout << std::endl << "Aborted." << std::endl;
        return EXIT_FAILURE;
    }
    std::cout << std::endl << "Done!" << std::endl << std::endl;
    return EXIT_SUCCESS;
}
