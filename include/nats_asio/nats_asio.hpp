/*
MIT License

Copyright (c) 2019 Vladislav Troinich

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
                                                          copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <fmt/format.h>

#ifdef __linux__
#include <sys/socket.h>  // For SO_BUSY_POLL
#endif

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/co_spawn.hpp>
#include <asio/connect.hpp>
#include <asio/detached.hpp>
#include <asio/strand.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl/context.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <asio/use_awaitable.hpp>
#include <array>
#include <atomic>
#include <charconv>
#include <concepts>
#include <concurrentqueue/moodycamel/concurrentqueue.h>
#include <gtl/lru_cache.hpp>
#include <iomanip>
#include <istream>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <simdjson.h>
#include <sstream>
#include <string>
#include <stringzilla/stringzilla.hpp>
#include <utility>

#include "interface.hpp"

namespace nats_asio {

namespace ssl = asio::ssl;

namespace protocol {
    constexpr string_view crlf = "\r\n";
    constexpr string_view pub_fmt = "PUB {} {} {}\r\n";
    constexpr string_view hpub_fmt = "HPUB {} {} {} {}\r\n";      // subject reply hdr_len total_len
    constexpr string_view hpub_no_reply_fmt = "HPUB {} {} {}\r\n"; // subject hdr_len total_len
    constexpr string_view sub_fmt = "SUB {} {} {}\r\n";
    constexpr string_view unsub_fmt = "UNSUB {}\r\n";
    constexpr string_view connect_fmt = "CONNECT {}\r\n";
    constexpr string_view pong = "PONG\r\n";
    constexpr string_view nats_hdr_line = "NATS/1.0\r\n";

    constexpr string_view op_msg = "MSG";
    constexpr string_view op_hmsg = "HMSG";
    constexpr string_view op_ping = "PING";
    constexpr string_view op_pong = "PONG";
    constexpr string_view op_ok = "+OK";
    constexpr string_view op_err = "-ERR";
    constexpr string_view op_info = "INFO";
    constexpr string_view delim = " ";
}

// ============================================================================
// Fast Integer Parsing - Uses std::from_chars (no exceptions, ~20% faster)
// ============================================================================

// Parse integer from string_view without exceptions
// Returns true on success, false on parse error
template<typename T>
inline bool parse_int(string_view sv, T& out) {
    if (sv.empty()) return false;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && ptr == sv.data() + sv.size();
}

// Parse integer with default value on failure
template<typename T>
inline T parse_int_or(string_view sv, T default_val) {
    T result;
    return parse_int(sv, result) ? result : default_val;
}

// ============================================================================
// Fast JSON Parsing - Uses simdjson (~10x faster than nlohmann::json)
// ============================================================================

// Thread-local simdjson parser for parsing server responses
// simdjson is ~10x faster than nlohmann::json for parsing
class fast_json {
public:
    // Parse JSON from string_view, returns true on success
    bool parse(string_view sv) {
        m_padded = simdjson::padded_string(sv.data(), sv.size());
        auto result = m_parser.parse(m_padded);
        if (result.error()) {
            m_error = simdjson::error_message(result.error());
            return false;
        }
        m_doc = std::move(result.value());
        m_error.clear();
        return true;
    }

    // Get string value at path (returns empty on error)
    std::string_view get_string(std::string_view key) const {
        auto result = m_doc[key].get_string();
        return result.error() ? std::string_view{} : result.value();
    }

    // Get nested string value (e.g., "config", "deliver_subject")
    std::string_view get_string(std::string_view key1, std::string_view key2) const {
        auto obj = m_doc[key1];
        if (obj.error()) return {};
        auto result = obj[key2].get_string();
        return result.error() ? std::string_view{} : result.value();
    }

    // Get int64 value (returns default_val on error)
    int64_t get_int(std::string_view key, int64_t default_val = 0) const {
        auto result = m_doc[key].get_int64();
        return result.error() ? default_val : result.value();
    }

    // Get nested int64 value
    int64_t get_int(std::string_view key1, std::string_view key2, int64_t default_val = 0) const {
        auto obj = m_doc[key1];
        if (obj.error()) return default_val;
        auto result = obj[key2].get_int64();
        return result.error() ? default_val : result.value();
    }

    // Get uint64 value (returns default_val on error)
    uint64_t get_uint(std::string_view key, uint64_t default_val = 0) const {
        auto result = m_doc[key].get_uint64();
        return result.error() ? default_val : result.value();
    }

    // Get nested uint64 value
    uint64_t get_uint(std::string_view key1, std::string_view key2, uint64_t default_val = 0) const {
        auto obj = m_doc[key1];
        if (obj.error()) return default_val;
        auto result = obj[key2].get_uint64();
        return result.error() ? default_val : result.value();
    }

    // Get bool value (returns default_val on error)
    bool get_bool(std::string_view key, bool default_val = false) const {
        auto result = m_doc[key].get_bool();
        return result.error() ? default_val : result.value();
    }

    // Check if key exists
    bool contains(std::string_view key) const {
        return !m_doc[key].error();
    }

    // Check if nested key exists
    bool contains(std::string_view key1, std::string_view key2) const {
        auto obj = m_doc[key1];
        if (obj.error()) return false;
        return !obj[key2].error();
    }

    // Get error description from last failed "error" field
    std::string_view get_error_description() const {
        auto err = m_doc["error"];
        if (err.error()) return {};
        auto desc = err["description"].get_string();
        return desc.error() ? std::string_view{} : desc.value();
    }

    // Get raw document for complex access patterns
    const simdjson::dom::element& doc() const { return m_doc; }

    // Get parse error message
    const std::string& error() const { return m_error; }

    // Iterate over array of strings (e.g., for keys list)
    template<typename Func>
    bool for_each_string_in_array(std::string_view key, Func&& func) const {
        auto arr = m_doc[key].get_array();
        if (arr.error()) return false;
        for (auto elem : arr.value()) {
            auto str = elem.get_string();
            if (!str.error()) {
                func(str.value());
            }
        }
        return true;
    }

private:
    simdjson::dom::parser m_parser;
    simdjson::dom::element m_doc;
    simdjson::padded_string m_padded;
    std::string m_error;
};

// ============================================================================
// Buffer Pool - Lock-free pool using moodycamel::ConcurrentQueue
// ============================================================================

// Lock-free buffer pool for reusing string and vector buffers
// Uses moodycamel::ConcurrentQueue for high-performance concurrent access
template<typename T>
class buffer_pool {
public:
    explicit buffer_pool(size_t initial_capacity = 32, size_t default_buffer_size = 256)
        : m_pool(initial_capacity), m_default_size(default_buffer_size), m_current_size(0) {}

    ~buffer_pool() = default;

    // Non-copyable, non-movable (pools are typically singletons or connection-owned)
    buffer_pool(const buffer_pool&) = delete;
    buffer_pool& operator=(const buffer_pool&) = delete;

    // Acquire a buffer from the pool (or create new if empty)
    T acquire() {
        T buf;
        if (m_pool.try_dequeue(buf)) {
            m_current_size.fetch_sub(1, std::memory_order_relaxed);
            return buf;
        }
        // Pool empty, create new buffer with reserved capacity
        if constexpr (requires { buf.reserve(m_default_size); }) {
            buf.reserve(m_default_size);
        }
        return buf;
    }

    // Return a buffer to the pool for reuse
    void release(T&& buf) {
        // Clear but keep capacity
        if constexpr (requires { buf.clear(); }) {
            buf.clear();
        }
        // Limit pool growth to prevent unbounded memory
        size_t current = m_current_size.load(std::memory_order_relaxed);
        if (current < m_max_pool_size) {
            if (m_pool.enqueue(std::move(buf))) {
                m_current_size.fetch_add(1, std::memory_order_relaxed);
            }
        }
        // If pool is full, buffer is simply destroyed (memory returned to allocator)
    }

    // Get approximate pool size (for diagnostics - not exact due to lock-free nature)
    size_t size() const {
        return m_current_size.load(std::memory_order_relaxed);
    }

    void set_max_pool_size(size_t max_size) {
        m_max_pool_size = max_size;
    }

private:
    moodycamel::ConcurrentQueue<T> m_pool;
    size_t m_default_size;
    std::atomic<size_t> m_current_size;
    size_t m_max_pool_size = 1024;  // Limit to prevent unbounded growth
};

// RAII handle that automatically returns buffer to pool
template<typename T>
class pooled_buffer {
public:
    pooled_buffer(buffer_pool<T>& pool) : m_pool(&pool), m_buffer(pool.acquire()) {}

    ~pooled_buffer() {
        if (m_pool) {
            m_pool->release(std::move(m_buffer));
        }
    }

    // Move-only
    pooled_buffer(pooled_buffer&& other) noexcept
        : m_pool(other.m_pool), m_buffer(std::move(other.m_buffer)) {
        other.m_pool = nullptr;
    }

    pooled_buffer& operator=(pooled_buffer&& other) noexcept {
        if (this != &other) {
            if (m_pool) {
                m_pool->release(std::move(m_buffer));
            }
            m_pool = other.m_pool;
            m_buffer = std::move(other.m_buffer);
            other.m_pool = nullptr;
        }
        return *this;
    }

    pooled_buffer(const pooled_buffer&) = delete;
    pooled_buffer& operator=(const pooled_buffer&) = delete;

    // Access the underlying buffer
    T& get() { return m_buffer; }
    const T& get() const { return m_buffer; }
    T* operator->() { return &m_buffer; }
    const T* operator->() const { return &m_buffer; }
    T& operator*() { return m_buffer; }
    const T& operator*() const { return m_buffer; }

private:
    buffer_pool<T>* m_pool;
    T m_buffer;
};

// Convenience type aliases
using string_pool = buffer_pool<std::string>;
using char_vector_pool = buffer_pool<std::vector<char>>;
using const_buffer_vector_pool = buffer_pool<std::vector<asio::const_buffer>>;

using pooled_string = pooled_buffer<std::string>;
using pooled_char_vector = pooled_buffer<std::vector<char>>;
using pooled_const_buffer_vector = pooled_buffer<std::vector<asio::const_buffer>>;

// Shared buffer pools (can be used globally or per-connection)
struct buffer_pools {
    string_pool strings{64, 256};           // For command strings, subjects
    string_pool large_strings{32, 4096};    // For header serialization
    char_vector_pool payloads{32, 1024};    // For message payloads
    const_buffer_vector_pool buffer_vectors{64, 4};  // For async_write buffers
};

// ============================================================================
// Write Queue - Lock-free queue for coalescing multiple writes
// ============================================================================

// Thread-safe write queue that buffers messages for batched sending
// Uses lock-free queue for high-performance concurrent access
class write_queue {
public:
    write_queue(size_t initial_capacity = 1024)
        : m_queue(initial_capacity), m_pending_bytes(0),
          m_flush_interval(std::chrono::microseconds(1000)),  // 1ms default
          m_max_pending_bytes(64 * 1024) {}  // 64KB default

    // Queue a pre-formatted NATS message (e.g., "PUB subject len\r\npayload\r\n")
    // Returns false if queue is full (shouldn't happen with unbounded queue)
    bool enqueue(std::string&& data) {
        size_t size = data.size();
        if (m_queue.enqueue(std::move(data))) {
            m_pending_bytes.fetch_add(size, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    // Dequeue all pending messages into output buffer
    // Returns number of messages dequeued
    size_t dequeue_all(std::string& output) {
        std::string item;
        size_t count = 0;
        size_t bytes = 0;

        while (m_queue.try_dequeue(item)) {
            bytes += item.size();
            output += item;
            ++count;
        }

        if (bytes > 0) {
            m_pending_bytes.fetch_sub(bytes, std::memory_order_relaxed);
        }
        return count;
    }

    // Get approximate pending bytes (for flush threshold checking)
    size_t pending_bytes() const noexcept {
        return m_pending_bytes.load(std::memory_order_relaxed);
    }

    // Check if queue should be flushed (exceeds threshold)
    bool should_flush() const noexcept {
        return m_pending_bytes.load(std::memory_order_relaxed) >= m_max_pending_bytes;
    }

    // Check if queue is empty
    bool empty() const noexcept {
        return m_pending_bytes.load(std::memory_order_relaxed) == 0;
    }

    // Configuration
    void set_flush_interval(std::chrono::microseconds interval) {
        m_flush_interval = interval;
    }

    std::chrono::microseconds flush_interval() const noexcept {
        return m_flush_interval;
    }

    void set_max_pending_bytes(size_t max_bytes) {
        m_max_pending_bytes = max_bytes;
    }

    size_t max_pending_bytes() const noexcept {
        return m_max_pending_bytes;
    }

private:
    moodycamel::ConcurrentQueue<std::string> m_queue;
    std::atomic<size_t> m_pending_bytes;
    std::chrono::microseconds m_flush_interval;
    size_t m_max_pending_bytes;
};

// ============================================================================
// Pre-formatted PUB Template - Caches command prefix for repeated subjects
// ============================================================================

// Caches "PUB subject len\r\n" for subjects with fixed-size payloads
// Eliminates fmt::format overhead for high-frequency publishing
class pub_template {
public:
    pub_template() = default;

    // Create template for subject with fixed payload size
    pub_template(string_view subject, size_t payload_size) {
        m_prefix = fmt::format("PUB {} {}\r\n", subject, payload_size);
        m_payload_size = payload_size;
    }

    // Create template with reply-to for request patterns
    pub_template(string_view subject, string_view reply_to, size_t payload_size) {
        m_prefix = fmt::format("PUB {} {} {}\r\n", subject, reply_to, payload_size);
        m_payload_size = payload_size;
    }

    // Get the cached prefix
    const std::string& prefix() const { return m_prefix; }
    string_view prefix_view() const { return m_prefix; }

    // Get spans for scatter-gather write: [prefix, payload, crlf]
    std::array<std::span<const char>, 3> get_iov(std::span<const char> payload) const {
        return {{
            std::span<const char>(m_prefix.data(), m_prefix.size()),
            payload,
            std::span<const char>("\r\n", 2)
        }};
    }

    // Build complete message into buffer (for batching)
    void append_to(std::string& out, std::span<const char> payload) const {
        out += m_prefix;
        out.append(payload.data(), payload.size());
        out += "\r\n";
    }

    size_t payload_size() const { return m_payload_size; }
    bool valid() const { return !m_prefix.empty(); }

private:
    std::string m_prefix;
    size_t m_payload_size = 0;
};

// Thread-safe LRU cache of pub_templates keyed by subject+size
// Uses gtl::lru_cache with mutex for proper LRU eviction
class pub_template_cache {
public:
    pub_template_cache(size_t max_entries = 1024)
        : m_max_entries(max_entries), m_cache(std::make_unique<cache_t>(max_entries)) {}

    // Get or create template for subject with payload size
    // Note: Returns by value since LRU cache may evict entries
    pub_template get(string_view subject, size_t payload_size) {
        auto key = make_key(subject, payload_size);

        std::lock_guard<std::mutex> lock(m_mutex);
        if (auto cached = m_cache->get(key)) {
            return *cached;
        }

        pub_template tpl(subject, payload_size);
        m_cache->insert(key, tpl);
        return tpl;
    }

    // Get or create template with reply-to
    pub_template get(string_view subject, string_view reply_to, size_t payload_size) {
        auto key = make_key_with_reply(subject, reply_to, payload_size);

        std::lock_guard<std::mutex> lock(m_mutex);
        if (auto cached = m_cache->get(key)) {
            return *cached;
        }

        pub_template tpl(subject, reply_to, payload_size);
        m_cache->insert(key, tpl);
        return tpl;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(m_mutex);
        // Workaround for GTL lru_cache::clear() bug - recreate the cache
        m_cache = std::make_unique<cache_t>(m_max_entries);
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cache->size();
    }

private:
    using cache_t = gtl::lru_cache<std::string, pub_template>;

    static std::string make_key(string_view subject, size_t payload_size) {
        return fmt::format("{}:{}", subject, payload_size);
    }

    static std::string make_key_with_reply(string_view subject, string_view reply_to, size_t payload_size) {
        return fmt::format("{}:{}:{}", subject, reply_to, payload_size);
    }

    size_t m_max_entries;
    mutable std::mutex m_mutex;
    std::unique_ptr<cache_t> m_cache;
};

// ============================================================================
// Subject Builders - Efficient subject string construction
// ============================================================================

// Fast subject building using string concatenation (faster than fmt::format for simple cases)
namespace subjects {

// Build KV subject: $KV.{bucket}.{key}
inline std::string kv_subject(string_view bucket, string_view key) {
    std::string result;
    result.reserve(4 + bucket.size() + 1 + key.size());  // "$KV." + bucket + "." + key
    result += "$KV.";
    result.append(bucket.data(), bucket.size());
    result += '.';
    result.append(key.data(), key.size());
    return result;
}

// Build KV wildcard subject: $KV.{bucket}.>
inline std::string kv_wildcard(string_view bucket) {
    std::string result;
    result.reserve(4 + bucket.size() + 2);  // "$KV." + bucket + ".>"
    result += "$KV.";
    result.append(bucket.data(), bucket.size());
    result += ".>";
    return result;
}

// Build KV prefix: $KV.{bucket}.
inline std::string kv_prefix(string_view bucket) {
    std::string result;
    result.reserve(4 + bucket.size() + 1);  // "$KV." + bucket + "."
    result += "$KV.";
    result.append(bucket.data(), bucket.size());
    result += '.';
    return result;
}

// Build KV stream name: KV_{bucket}
inline std::string kv_stream_name(string_view bucket) {
    std::string result;
    result.reserve(3 + bucket.size());  // "KV_" + bucket
    result += "KV_";
    result.append(bucket.data(), bucket.size());
    return result;
}

// Build JetStream direct get subject: $JS.API.DIRECT.GET.{stream}
inline std::string js_direct_get(string_view stream) {
    std::string result;
    result.reserve(20 + stream.size());  // "$JS.API.DIRECT.GET." + stream
    result += "$JS.API.DIRECT.GET.";
    result.append(stream.data(), stream.size());
    return result;
}

// Build JetStream stream info subject: $JS.API.STREAM.INFO.{stream}
inline std::string js_stream_info(string_view stream) {
    std::string result;
    result.reserve(20 + stream.size());  // "$JS.API.STREAM.INFO." + stream
    result += "$JS.API.STREAM.INFO.";
    result.append(stream.data(), stream.size());
    return result;
}

// Build JetStream consumer info subject: $JS.API.CONSUMER.INFO.{stream}.{consumer}
inline std::string js_consumer_info(string_view stream, string_view consumer) {
    std::string result;
    result.reserve(22 + stream.size() + 1 + consumer.size());
    result += "$JS.API.CONSUMER.INFO.";
    result.append(stream.data(), stream.size());
    result += '.';
    result.append(consumer.data(), consumer.size());
    return result;
}

// Build JetStream consumer delete subject: $JS.API.CONSUMER.DELETE.{stream}.{consumer}
inline std::string js_consumer_delete(string_view stream, string_view consumer) {
    std::string result;
    result.reserve(24 + stream.size() + 1 + consumer.size());
    result += "$JS.API.CONSUMER.DELETE.";
    result.append(stream.data(), stream.size());
    result += '.';
    result.append(consumer.data(), consumer.size());
    return result;
}

// Build JetStream consumer fetch subject: $JS.API.CONSUMER.MSG.NEXT.{stream}.{consumer}
inline std::string js_consumer_fetch(string_view stream, string_view consumer) {
    std::string result;
    result.reserve(26 + stream.size() + 1 + consumer.size());
    result += "$JS.API.CONSUMER.MSG.NEXT.";
    result.append(stream.data(), stream.size());
    result += '.';
    result.append(consumer.data(), consumer.size());
    return result;
}

// Build JetStream durable consumer create: $JS.API.CONSUMER.DURABLE.CREATE.{stream}.{consumer}
inline std::string js_consumer_durable_create(string_view stream, string_view consumer) {
    std::string result;
    result.reserve(32 + stream.size() + 1 + consumer.size());
    result += "$JS.API.CONSUMER.DURABLE.CREATE.";
    result.append(stream.data(), stream.size());
    result += '.';
    result.append(consumer.data(), consumer.size());
    return result;
}

// Build JetStream ephemeral consumer create: $JS.API.CONSUMER.CREATE.{stream}
inline std::string js_consumer_create(string_view stream) {
    std::string result;
    result.reserve(24 + stream.size());
    result += "$JS.API.CONSUMER.CREATE.";
    result.append(stream.data(), stream.size());
    return result;
}

}  // namespace subjects

// Cache for KV bucket prefixes - avoids repeated string construction
// Thread-safe LRU cache keyed by bucket name
class kv_prefix_cache {
public:
    // Cached prefixes for a single bucket
    struct bucket_prefixes {
        std::string kv_prefix;       // $KV.{bucket}.
        std::string stream_name;     // KV_{bucket}
        std::string direct_get;      // $JS.API.DIRECT.GET.KV_{bucket}
        std::string stream_info;     // $JS.API.STREAM.INFO.KV_{bucket}
    };

    kv_prefix_cache(size_t max_entries = 64)
        : m_max_entries(max_entries), m_cache(std::make_unique<cache_t>(max_entries)) {}

    // Get or create prefixes for bucket (returns by value - small struct)
    bucket_prefixes get(string_view bucket) {
        std::string key(bucket);

        std::lock_guard<std::mutex> lock(m_mutex);
        if (auto cached = m_cache->get(key)) {
            return *cached;
        }

        bucket_prefixes prefixes;
        prefixes.kv_prefix = subjects::kv_prefix(bucket);
        prefixes.stream_name = subjects::kv_stream_name(bucket);
        prefixes.direct_get = subjects::js_direct_get(prefixes.stream_name);
        prefixes.stream_info = subjects::js_stream_info(prefixes.stream_name);

        m_cache->insert(key, prefixes);
        return prefixes;
    }

    // Build full KV subject using cached prefix
    std::string kv_subject(string_view bucket, string_view key) {
        auto p = get(bucket);
        std::string result;
        result.reserve(p.kv_prefix.size() + key.size());
        result += p.kv_prefix;
        result.append(key.data(), key.size());
        return result;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_cache = std::make_unique<cache_t>(m_max_entries);
    }

private:
    using cache_t = gtl::lru_cache<std::string, bucket_prefixes>;

    size_t m_max_entries;
    mutable std::mutex m_mutex;
    std::unique_ptr<cache_t> m_cache;
};

// ============================================================================

// Generate unique inbox for request-reply
inline std::string generate_inbox() {
    static std::atomic<uint64_t> counter{0};
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return fmt::format("_INBOX.{}.{}", now, counter++);
}

// Serialize headers to NATS format: "NATS/1.0\r\nKey: Value\r\n...\r\n"
inline std::string serialize_headers(const headers_t& headers) {
    std::string result(protocol::nats_hdr_line);
    for (const auto& [key, value] : headers) {
        result += key;
        result += ": ";
        result += value;
        result += "\r\n";
    }
    result += "\r\n";  // Empty line terminates headers
    return result;
}

// Serialize headers into existing buffer (avoids allocation)
inline void serialize_headers_to(std::string& out, const headers_t& headers) {
    out.clear();
    out += protocol::nats_hdr_line;
    for (const auto& [key, value] : headers) {
        out += key;
        out += ": ";
        out += value;
        out += "\r\n";
    }
    out += "\r\n";
}

// Parse headers from NATS format using StringZilla for SIMD-accelerated search
inline headers_t parse_headers(string_view data) {
    namespace sz = ashvardanian::stringzilla;
    headers_t headers;

    sz::string_view sz_data(data.data(), data.size());
    constexpr sz::string_view crlf("\r\n", 2);
    constexpr char colon = ':';

    // Skip "NATS/1.0\r\n" prefix
    auto pos = sz_data.find(crlf);
    if (pos == sz::string_view::npos) return headers;
    sz_data = sz_data.substr(pos + 2);

    // Parse each header line until empty line
    while (!sz_data.empty() && sz_data != crlf) {
        pos = sz_data.find(crlf);
        if (pos == sz::string_view::npos) break;

        auto line = sz_data.substr(0, pos);
        sz_data = sz_data.substr(pos + 2);

        if (line.empty()) break;  // Empty line = end of headers

        auto colon_pos = line.find(colon);
        if (colon_pos != sz::string_view::npos) {
            auto key = line.substr(0, colon_pos);
            auto value = line.substr(colon_pos + 1);
            // Trim leading space from value
            if (!value.empty() && value[0] == ' ') {
                value = value.substr(1);
            }
            headers.emplace_back(std::string(key.data(), key.size()),
                                 std::string(value.data(), value.size()));
        }
    }
    return headers;
}

// Parse headers without copying using StringZilla - returns string_views into original data
// WARNING: Returned views are only valid as long as the input data is valid!
inline headers_view_t parse_headers_view(string_view data) {
    namespace sz = ashvardanian::stringzilla;
    headers_view_t headers;

    sz::string_view sz_data(data.data(), data.size());
    constexpr sz::string_view crlf("\r\n", 2);
    constexpr char colon = ':';

    // Skip "NATS/1.0\r\n" prefix
    auto pos = sz_data.find(crlf);
    if (pos == sz::string_view::npos) return headers;
    sz_data = sz_data.substr(pos + 2);

    // Parse each header line until empty line
    while (!sz_data.empty() && sz_data != crlf) {
        pos = sz_data.find(crlf);
        if (pos == sz::string_view::npos) break;

        auto line = sz_data.substr(0, pos);
        sz_data = sz_data.substr(pos + 2);

        if (line.empty()) break;

        auto colon_pos = line.find(colon);
        if (colon_pos != sz::string_view::npos) {
            auto key = line.substr(0, colon_pos);
            auto value = line.substr(colon_pos + 1);
            if (!value.empty() && value[0] == ' ') {
                value = value.substr(1);
            }
            // Convert sz::string_view to std::string_view
            headers.emplace_back(string_view(key.data(), key.size()),
                                 string_view(value.data(), value.size()));
        }
    }
    return headers;
}

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;
using string_view = std::string_view;
using raw_socket = asio::ip::tcp::socket;
using ssl_socket = asio::ssl::stream<asio::ip::tcp::socket>;

template <class Socket>
auto& get_lowest_layer(Socket& s) {
    if constexpr (requires { s.lowest_layer(); }) {
        return s.lowest_layer();
    } else {
        return s;
    }
}

template <typename T>
concept SslSocketType = std::same_as<T, ssl_socket>;

template <typename T>
concept RawSocketType = std::same_as<T, raw_socket>;

template <class Socket>
struct uni_socket {
    using socket_type = Socket;
    using endpoint_type = tcp::endpoint;
    using executor_type = typename Socket::executor_type;

    uni_socket(aio& io, ssl::context& ctx) requires SslSocketType<Socket> : m_socket(io, ctx) {}

    uni_socket(aio& io) requires RawSocketType<Socket> : m_socket(io) {}

    uni_socket(const uni_socket&) = delete;
    uni_socket& operator=(const uni_socket&) = delete;
    uni_socket(uni_socket&&) = default;
    uni_socket& operator=(uni_socket&&) = default;

    asio::awaitable<void> async_connect(const endpoint_type& endpoint);
    asio::awaitable<void> async_handshake();
    asio::awaitable<void> async_shutdown();

    template <class Buf, class Separator>
    asio::awaitable<void> async_read_until(Buf& buf, const Separator& separator) {
        co_await asio::async_read_until(m_socket, buf, separator);
    }

    template <class Buf>
    asio::awaitable<void> async_read_until_raw(Buf& buf) {
        co_await asio::async_read_until(get_lowest_layer(m_socket), buf, std::string(protocol::crlf));
    }

    template <class Buf, class Transfer>
    asio::awaitable<void> async_read(Buf& buf, const Transfer& completion) {
        co_await asio::async_read(m_socket, buf, completion);
    }

    template <class Buf, class Transfer>
    asio::awaitable<void> async_write(const Buf& buf, const Transfer& completion) {
        co_await asio::async_write(m_socket, buf, completion);
    }

    template <typename MutableBufferSequence, typename CompletionToken>
    auto async_read_some(const MutableBufferSequence& buffers, CompletionToken&& token) {
        return m_socket.async_read_some(buffers, std::forward<CompletionToken>(token));
    }

    template <typename ConstBufferSequence, typename CompletionToken>
    auto async_write_some(const ConstBufferSequence& buffers, CompletionToken&& token) {
        return m_socket.async_write_some(buffers, std::forward<CompletionToken>(token));
    }

    void close(asio::error_code& ec);
    void close() {
        asio::error_code ec;
        close(ec);
    }

    bool is_open() const;

    void cancel(asio::error_code& ec) {
        lowest_layer().cancel(ec);
    }

    void cancel() {
        asio::error_code ec;
        cancel(ec);
    }

    bool is_connected() const {
        if (!is_open())
            return false;
        asio::error_code ec;
        lowest_layer().remote_endpoint(ec);
        return !ec;
    }

    void shutdown(asio::socket_base::shutdown_type what, asio::error_code& ec) {
        lowest_layer().shutdown(what, ec);
    }

    void shutdown(asio::socket_base::shutdown_type what) {
        asio::error_code ec;
        shutdown(what, ec);
        if (ec)
            throw asio::system_error(ec);
    }

    void bind(const endpoint_type& endpoint, asio::error_code& ec) {
        lowest_layer().bind(endpoint, ec);
    }

    void bind(const endpoint_type& endpoint) {
        asio::error_code ec;
        bind(endpoint, ec);
        if (ec)
            throw asio::system_error(ec);
    }

    void non_blocking(bool mode, asio::error_code& ec) {
        lowest_layer().non_blocking(mode, ec);
    }

    void non_blocking(bool mode) {
        asio::error_code ec;
        non_blocking(mode, ec);
        if (ec)
            throw asio::system_error(ec);
    }

    std::size_t available(asio::error_code& ec) const {
        return lowest_layer().available(ec);
    }

    std::size_t available() const {
        return lowest_layer().available();
    }

    auto native_handle() {
        return lowest_layer().native_handle();
    }

    endpoint_type remote_endpoint(asio::error_code& ec) const {
        return lowest_layer().remote_endpoint(ec);
    }

    endpoint_type remote_endpoint() const {
        return lowest_layer().remote_endpoint();
    }

    endpoint_type local_endpoint(asio::error_code& ec) const {
        return lowest_layer().local_endpoint(ec);
    }

    endpoint_type local_endpoint() const {
        return lowest_layer().local_endpoint();
    }

    template <typename Option>
    void set_option(const Option& option, asio::error_code& ec) {
        lowest_layer().set_option(option, ec);
    }

    template <typename Option>
    void set_option(const Option& option) {
        asio::error_code ec;
        set_option(option, ec);
        if (ec)
            throw asio::system_error(ec);
    }

    template <typename Option>
    void get_option(Option& option, asio::error_code& ec) const {
        lowest_layer().get_option(option, ec);
    }

    template <typename Option>
    void get_option(Option& option) const {
        lowest_layer().get_option(option);
    }

    Socket& socket() {
        return m_socket;
    }
    const Socket& socket() const {
        return m_socket;
    }

    auto& lowest_layer() {
        return get_lowest_layer(m_socket);
    }
    const auto& lowest_layer() const {
        return get_lowest_layer(m_socket);
    }

    executor_type get_executor() {
        return lowest_layer().get_executor();
    }

private:
    Socket m_socket;
};

// Specializations for raw_socket
template <>
inline void uni_socket<raw_socket>::close(asio::error_code& ec) {
    m_socket.close(ec);
}

template <>
inline bool uni_socket<raw_socket>::is_open() const {
    return m_socket.is_open();
}

template <>
inline asio::awaitable<void> uni_socket<raw_socket>::async_handshake() {
    co_return; // No-op for raw socket
}

template <>
inline asio::awaitable<void> uni_socket<raw_socket>::async_shutdown() {
    co_return; // No-op for raw socket
}

template <>
inline asio::awaitable<void> uni_socket<raw_socket>::async_connect(const endpoint_type& endpoint) {
    co_await m_socket.async_connect(endpoint);
    co_return;
}

// Specializations for ssl_socket
template <>
inline void uni_socket<ssl_socket>::close(asio::error_code& ec) {
    m_socket.lowest_layer().close(ec);
}

template <>
inline bool uni_socket<ssl_socket>::is_open() const {
    return m_socket.lowest_layer().is_open();
}

template <>
inline asio::awaitable<void> uni_socket<ssl_socket>::async_handshake() {
    co_await m_socket.async_handshake(ssl::stream_base::client);
    co_return;
}

template <>
inline asio::awaitable<void> uni_socket<ssl_socket>::async_shutdown() {
    co_await m_socket.async_shutdown();
    co_return;
}

template <>
inline asio::awaitable<void> uni_socket<ssl_socket>::async_connect(const endpoint_type& endpoint) {
    co_await m_socket.lowest_layer().async_connect(endpoint);
    co_return;
}

struct parser_observer {
    virtual ~parser_observer() = default;
    virtual asio::awaitable<void> on_ping() = 0;
    virtual asio::awaitable<void> on_pong() = 0;
    virtual asio::awaitable<void> on_ok() = 0;
    virtual asio::awaitable<void> on_error(string_view err) = 0;
    virtual asio::awaitable<void> on_info(string_view info) = 0;
    virtual asio::awaitable<void> on_message(string_view subject, string_view sid,
                                             optional<string_view> reply_to, std::size_t n) = 0;
    // HMSG: message with headers
    virtual asio::awaitable<void> on_hmessage(string_view subject, string_view sid,
                                              optional<string_view> reply_to,
                                              std::size_t header_len, std::size_t total_len) = 0;
    virtual asio::awaitable<void> consumed(std::size_t n) = 0;
};

struct protocol_parser {
    static asio::awaitable<status> parse_header(std::string& header, std::istream& is,
                                                parser_observer& observer) {
        if (!std::getline(is, header)) {
            co_return status(error_code::protocol_error);
        }

        if (header.size() < 3) {
            co_return status(error_code::invalid_header);
        }

        if (header.back() != '\r') {
            co_return status(error_code::protocol_error);
        }

        header.pop_back();
        auto v = string_view(header);

        if (v.empty()) {
            co_return status(error_code::protocol_error);
        }

        switch (v[0]) {
            case 'H': // HMSG
                if (v.starts_with(protocol::op_hmsg) && v.size() > protocol::op_hmsg.size() && v[protocol::op_hmsg.size()] == ' ') {
                    auto info = v.substr(protocol::op_hmsg.size() + 1);
                    auto results = split_sv(info, protocol::delim);

                    // HMSG subject sid [reply] hdr_len total_len
                    if (results.size() < 4 || results.size() > 5) {
                        co_return status(error_code::invalid_message);
                    }

                    bool has_reply = results.size() == 5;
                    std::size_t hdr_idx = has_reply ? 3 : 2;
                    std::size_t total_idx = has_reply ? 4 : 3;
                    std::size_t hdr_len = 0, total_len = 0;

                    if (!parse_int(results[hdr_idx], hdr_len) ||
                        !parse_int(results[total_idx], total_len)) {
                        co_return status(error_code::parse_error);
                    }

                    if (has_reply) {
                        co_await observer.on_hmessage(results[0], results[1], results[2], hdr_len, total_len);
                    } else {
                        co_await observer.on_hmessage(results[0], results[1], optional<string_view>(), hdr_len, total_len);
                    }

                    co_await observer.consumed(total_len + 2);
                    co_return status();
                }
                break;

            case 'M': // MSG
                if (v.starts_with(protocol::op_msg) && v.size() > protocol::op_msg.size() && v[protocol::op_msg.size()] == ' ') {
                    auto info = v.substr(protocol::op_msg.size() + 1);
                    auto results = split_sv(info, protocol::delim);

                    if (results.size() < 3 || results.size() > 4) {
                        co_return status(error_code::invalid_message);
                    }

                    bool reply_to = results.size() == 4;
                    std::size_t bytes_id = reply_to ? 3 : 2;
                    std::size_t bytes_n = 0;

                    if (!parse_int(results[bytes_id], bytes_n)) {
                        co_return status(error_code::parse_error);
                    }

                    if (reply_to) {
                        co_await observer.on_message(results[0], results[1], results[2], bytes_n);
                    } else {
                        co_await observer.on_message(results[0], results[1],
                                                     optional<string_view>(), bytes_n);
                    }

                    co_await observer.consumed(bytes_n + 2);
                    co_return status();
                }
                break;

            case 'I': // INFO
                if (v.starts_with(protocol::op_info)) {
                    auto info_msg = (v.size() > protocol::op_info.size()) ? v.substr(protocol::op_info.size() + 1) : string_view{};
                    co_await observer.on_info(info_msg);
                    co_return status();
                }
                break;

            case 'P': // PING, PONG
                if (v == protocol::op_ping) {
                    co_await observer.on_ping();
                    co_return status();
                } else if (v == protocol::op_pong) {
                    co_await observer.on_pong();
                    co_return status();
                }
                break;

            case '+': // +OK
                if (v == protocol::op_ok) {
                    co_await observer.on_ok();
                    co_return status();
                }
                break;

            case '-': // -ERR
                if (v.starts_with(protocol::op_err)) {
                    auto err_msg = (v.size() > protocol::op_err.size()) ? v.substr(protocol::op_err.size() + 1) : string_view{};
                    co_await observer.on_error(err_msg);
                    co_return status();
                }
                break;
        }

        co_return status(error_code::invalid_message);
    }

    // SIMD-accelerated string splitting using StringZilla
    static std::vector<string_view> split_sv(string_view str, string_view delims = " ") {
        std::vector<string_view> output;
        output.reserve(4); // Typical NATS headers have 3-4 tokens

        namespace sz = ashvardanian::stringzilla;
        sz::string_view sz_str(str.data(), str.size());
        sz::string_view sz_delim(delims.data(), delims.size());

        // Use StringZilla's SIMD-accelerated split - returns iterable range
        for (auto part : sz_str.split(sz_delim)) {
            if (!part.empty()) {
                output.emplace_back(part.data(), part.size());
            }
        }

        return output;
    }
};

class subscription : public isubscription {
public:
    subscription(uint64_t sid, const on_message_cb& cb, uint32_t max_msgs = 0)
        : m_cancel(false), m_cb(cb), m_sid(sid), m_max_messages(max_msgs), m_message_count(0) {}

    // Constructor with headers callback for JetStream messages
    subscription(uint64_t sid, const on_message_with_headers_cb& headers_cb, uint32_t max_msgs = 0)
        : m_cancel(false), m_headers_cb(headers_cb), m_sid(sid), m_max_messages(max_msgs), m_message_count(0) {}

    // Constructor with zero-copy callback (references internal buffers)
    subscription(uint64_t sid, const on_message_zero_copy_cb& zero_copy_cb, uint32_t max_msgs = 0)
        : m_cancel(false), m_zero_copy_cb(zero_copy_cb), m_sid(sid), m_max_messages(max_msgs), m_message_count(0) {}

    subscription(const subscription&) = delete;
    subscription& operator=(const subscription&) = delete;

    void cancel() noexcept override {
        m_cancel.store(true);
    }

    [[nodiscard]] uint64_t sid() noexcept override {
        return m_sid;
    }

    [[nodiscard]] uint32_t max_messages() const noexcept override {
        return m_max_messages;
    }

    [[nodiscard]] uint32_t message_count() const noexcept override {
        return m_message_count.load();
    }

    [[nodiscard]] bool is_cancelled() const noexcept {
        return m_cancel.load();
    }

    [[nodiscard]] const on_message_cb& callback() const noexcept {
        return m_cb;
    }

    [[nodiscard]] bool has_headers_callback() const noexcept {
        return m_headers_cb != nullptr;
    }

    [[nodiscard]] const on_message_with_headers_cb& headers_callback() const noexcept {
        return m_headers_cb;
    }

    [[nodiscard]] bool has_zero_copy_callback() const noexcept {
        return m_zero_copy_cb != nullptr;
    }

    [[nodiscard]] const on_message_zero_copy_cb& zero_copy_callback() const noexcept {
        return m_zero_copy_cb;
    }

    // Increment message count and return true if should auto-unsubscribe
    [[nodiscard]] bool increment_and_check() noexcept {
        if (m_max_messages == 0) {
            return false;  // Unlimited
        }
        uint32_t count = ++m_message_count;
        return count >= m_max_messages;
    }

private:
    std::atomic<bool> m_cancel;
    on_message_cb m_cb;
    on_message_with_headers_cb m_headers_cb;
    on_message_zero_copy_cb m_zero_copy_cb;
    uint64_t m_sid;
    uint32_t m_max_messages;
    std::atomic<uint32_t> m_message_count;
};

using subscription_sptr = std::shared_ptr<subscription>;

// Forward declaration
template <class SocketType>
class connection;

// JetStream subscription implementation
template <class SocketType>
class js_subscription : public ijs_subscription {
public:
    js_subscription(connection<SocketType>* conn, const js_consumer_info& info,
                    isubscription_sptr underlying_sub)
        : m_conn(conn), m_info(info), m_underlying_sub(std::move(underlying_sub)),
          m_active(true) {}

    js_subscription(const js_subscription&) = delete;
    js_subscription& operator=(const js_subscription&) = delete;

    [[nodiscard]] const js_consumer_info& info() const noexcept override {
        return m_info;
    }

    void stop() noexcept override {
        m_active = false;
        if (m_underlying_sub) {
            m_underlying_sub->cancel();
        }
    }

    [[nodiscard]] bool is_active() const noexcept override {
        return m_active.load();
    }

    [[nodiscard]] asio::awaitable<status> ack(const js_message& msg) override {
        return send_ack(msg, "+ACK");
    }

    [[nodiscard]] asio::awaitable<status> ack_batch(
        const std::vector<js_message>& messages) override {
        return send_ack_batch(messages, "+ACK");
    }

    [[nodiscard]] asio::awaitable<status> nak(const js_message& msg,
                                               std::chrono::milliseconds delay) override {
        if (delay.count() > 0) {
            // NAK with delay in nanoseconds
            auto delay_ns = delay.count() * 1000000LL;
            return send_ack(msg, fmt::format("-NAK {{\"delay\":{}}}", delay_ns));
        }
        return send_ack(msg, "-NAK");
    }

    [[nodiscard]] asio::awaitable<status> in_progress(const js_message& msg) override {
        return send_ack(msg, "+WPI");  // Work in Progress
    }

    [[nodiscard]] asio::awaitable<status> term(const js_message& msg) override {
        return send_ack(msg, "+TERM");
    }

private:
    asio::awaitable<status> send_ack(const js_message& msg, std::string_view ack_body);
    asio::awaitable<status> send_ack_batch(const std::vector<js_message>& messages,
                                           std::string_view ack_body);

    connection<SocketType>* m_conn;
    js_consumer_info m_info;
    isubscription_sptr m_underlying_sub;
    std::atomic<bool> m_active;
};

template <class SocketType>
using js_subscription_sptr = std::shared_ptr<js_subscription<SocketType>>;

// KV watcher implementation
template <class SocketType>
class kv_watcher : public ikv_watcher {
public:
    kv_watcher(const std::string& bucket, const std::string& key_filter,
               isubscription_sptr underlying_sub)
        : m_bucket(bucket), m_key_filter(key_filter),
          m_underlying_sub(std::move(underlying_sub)), m_active(true) {}

    kv_watcher(const kv_watcher&) = delete;
    kv_watcher& operator=(const kv_watcher&) = delete;

    void stop() noexcept override {
        m_active = false;
        if (m_underlying_sub) {
            m_underlying_sub->cancel();
        }
    }

    [[nodiscard]] bool is_active() const noexcept override {
        return m_active.load();
    }

    [[nodiscard]] const std::string& bucket() const noexcept override {
        return m_bucket;
    }

    [[nodiscard]] const std::string& key_filter() const noexcept override {
        return m_key_filter;
    }

private:
    std::string m_bucket;
    std::string m_key_filter;
    isubscription_sptr m_underlying_sub;
    std::atomic<bool> m_active;
};

template <class SocketType>
using kv_watcher_sptr = std::shared_ptr<kv_watcher<SocketType>>;

// Validate KV key - returns error message if invalid, empty string if valid
inline std::string validate_kv_key(string_view key) {
    if (key.empty()) {
        return "key cannot be empty";
    }

    for (char c : key) {
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            return fmt::format("key contains invalid whitespace character: '{}'", key);
        }
        if (c == '*') {
            return fmt::format("key contains invalid wildcard '*': '{}'", key);
        }
        if (c == '>') {
            return fmt::format("key contains invalid wildcard '>': '{}'", key);
        }
    }

    return {};  // Valid
}

// Validate KV bucket name
inline std::string validate_kv_bucket(string_view bucket) {
    if (bucket.empty()) {
        return "bucket name cannot be empty";
    }

    for (char c : bucket) {
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            return fmt::format("bucket name contains invalid whitespace: '{}'", bucket);
        }
        if (c == '.' || c == '*' || c == '>') {
            return fmt::format("bucket name contains invalid character '{}': '{}'", c, bucket);
        }
    }

    return {};  // Valid
}

// Helper to parse KV entry from a message
inline kv_entry parse_kv_entry_from_message(const message& msg, string_view bucket) {
    kv_entry entry;
    entry.bucket = std::string(bucket);
    entry.value.assign(msg.payload.begin(), msg.payload.end());
    entry.op = kv_entry::operation::put;

    // Extract key from subject: $KV.{bucket}.{key}
    // Subject format: $KV.bucket.key or $KV.bucket.key.with.dots
    std::string prefix = subjects::kv_prefix(bucket);
    if (msg.subject.size() > prefix.size() && msg.subject.substr(0, prefix.size()) == prefix) {
        entry.key = msg.subject.substr(prefix.size());
    }

    // Parse metadata from headers
    for (const auto& [key, value] : msg.headers) {
        if (key == "Nats-Sequence") {
            parse_int(value, entry.revision);
        } else if (key == "Nats-Time-Stamp") {
            std::tm tm = {};
            std::istringstream ss(value);
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
            entry.created = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        } else if (key == "KV-Operation") {
            if (value == "DEL") {
                entry.op = kv_entry::operation::del;
            } else if (value == "PURGE") {
                entry.op = kv_entry::operation::purge;
            }
        }
    }

    return entry;
}

// Helper to parse JetStream message metadata from headers
inline js_message parse_js_message_metadata(const message& msg) {
    js_message js_msg;
    js_msg.msg = msg;

    for (const auto& [key, value] : msg.headers) {
        if (key == "Nats-Stream") {
            js_msg.stream = value;
        } else if (key == "Nats-Consumer") {
            js_msg.consumer = value;
        } else if (key == "Nats-Sequence") {
            // Format: "stream_seq consumer_seq"
            auto space_pos = value.find(' ');
            if (space_pos != std::string::npos) {
                parse_int(string_view(value.data(), space_pos), js_msg.stream_sequence);
                parse_int(string_view(value.data() + space_pos + 1, value.size() - space_pos - 1), js_msg.consumer_sequence);
            }
        } else if (key == "Nats-Num-Delivered") {
            parse_int(value, js_msg.num_delivered);
        } else if (key == "Nats-Num-Pending") {
            parse_int(value, js_msg.num_pending);
        } else if (key == "Nats-Time-Stamp") {
            // Parse ISO 8601 timestamp (simplified)
            // Format: 2021-08-15T23:24:24.123456789Z
            std::tm tm = {};
            std::istringstream ss(value);
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
            auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
            js_msg.timestamp = tp;
        }
    }

    return js_msg;
}

// Helper to convert enum to string using magic_enum
// Strips trailing underscore for C++ keyword workarounds (explicit_, new_)
template<typename E>
inline std::string enum_to_nats_string(E value) {
    auto name = magic_enum::enum_name(value);
    std::string result(name);
    // Strip trailing underscore (used for C++ keywords like explicit_, new_)
    if (!result.empty() && result.back() == '_') {
        result.pop_back();
    }
    return result;
}

// Convenience aliases for JetStream enums
inline std::string js_ack_policy_to_string(js_ack_policy policy) {
    return enum_to_nats_string(policy);
}

inline std::string js_replay_policy_to_string(js_replay_policy policy) {
    return enum_to_nats_string(policy);
}

inline std::string js_deliver_policy_to_string(js_deliver_policy policy) {
    return enum_to_nats_string(policy);
}

template <class SocketType>
class connection : public iconnection, public parser_observer {
public:
    connection(aio& io, const on_connected_cb& connected_cb,
               const on_disconnected_cb& disconnected_cb, const on_error_cb& error_cb,
               const std::shared_ptr<ssl::context>& ctx)
        : m_sid(0), m_max_payload(0), m_io(io), m_is_connected(false), m_stop_flag(false),
          m_connected_cb(connected_cb), m_disconnected_cb(disconnected_cb), m_error_cb(error_cb),
          m_ssl_ctx(ctx), m_socket(io, *ctx.get()) {}

    connection(aio& io, const on_connected_cb& connected_cb,
               const on_disconnected_cb& disconnected_cb, const on_error_cb& error_cb)
        : m_sid(0), m_max_payload(0), m_io(io), m_is_connected(false), m_stop_flag(false),
          m_connected_cb(connected_cb), m_disconnected_cb(disconnected_cb), m_error_cb(error_cb),
          m_socket(io) {}

    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    virtual void start(const connect_config& conf) override {
        asio::co_spawn(
            asio::make_strand(m_io), [this, conf]() -> awaitable<void> { return run(conf); },
            asio::detached);
    }

    virtual void stop() noexcept override {
        m_stop_flag = true;
    }
    [[nodiscard]] virtual bool is_connected() noexcept override {
        return m_is_connected;
    }

    virtual asio::awaitable<status> publish(string_view subject, std::span<const char> payload,
                                            optional<string_view> reply_to) override {
        if (!m_is_connected) {
            co_return status(error_code::not_connected);
        }

        if (m_max_payload > 0 && payload.size() > m_max_payload) {
            co_return status(error_code::message_too_large);
        }

        // Use pooled buffers to reduce allocations
        pooled_const_buffer_vector buffers(m_pools.buffer_vectors);
        pooled_string header(m_pools.strings);

        if (reply_to.has_value()) {
            *header = fmt::format(fmt::runtime(protocol::pub_fmt), subject, reply_to.value(), payload.size());
        } else {
            *header = fmt::format(fmt::runtime(protocol::pub_fmt), subject, "", payload.size());
        }

        buffers->emplace_back(asio::buffer(header->data(), header->size()));
        buffers->emplace_back(asio::buffer(payload.data(), payload.size()));
        buffers->emplace_back(asio::buffer(protocol::crlf.data(), protocol::crlf.size()));

        try {
            co_await asio::async_write(m_socket, *buffers, asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return status(e.code().message());
        }

        co_return status();
    }

    // Write pre-formatted NATS protocol data directly
    virtual asio::awaitable<status> write_raw(std::span<const char> data) override {
        if (!m_is_connected) {
            co_return status(error_code::not_connected);
        }

        try {
            co_await asio::async_write(m_socket, asio::buffer(data.data(), data.size()), asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return status(e.code().message());
        }

        co_return status();
    }

    // Write multiple buffers using scatter-gather I/O (writev)
    virtual asio::awaitable<status> write_raw_iov(
        std::span<const std::span<const char>> buffers) override {
        if (!m_is_connected) {
            co_return status(error_code::not_connected);
        }

        if (buffers.empty()) {
            co_return status();
        }

        // Convert spans to ASIO buffers for scatter-gather write
        std::vector<asio::const_buffer> asio_buffers;
        asio_buffers.reserve(buffers.size());
        for (const auto& buf : buffers) {
            asio_buffers.emplace_back(asio::buffer(buf.data(), buf.size()));
        }

        try {
            co_await asio::async_write(m_socket, asio_buffers, asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return status(e.code().message());
        }

        co_return status();
    }

    // =========================================================================
    // Write Coalescing Implementation
    // =========================================================================

    // Queue a publish for batched sending (fire-and-forget)
    virtual status publish_queued(string_view subject, std::span<const char> payload,
                                  optional<string_view> reply_to = {}) override {
        if (!m_is_connected) {
            return status(error_code::not_connected);
        }

        if (m_max_payload > 0 && payload.size() > m_max_payload) {
            return status(error_code::message_too_large);
        }

        // Format the complete NATS message
        std::string msg;
        if (reply_to.has_value() && !reply_to->empty()) {
            msg = fmt::format("PUB {} {} {}\r\n", subject, *reply_to, payload.size());
        } else {
            msg = fmt::format("PUB {} {}\r\n", subject, payload.size());
        }
        msg.append(payload.data(), payload.size());
        msg += "\r\n";

        // Enqueue the message
        if (!m_write_queue.enqueue(std::move(msg))) {
            return status(error_code::operation_failed, "write queue full");
        }

        // Start auto-flush coroutine if not running and interval > 0
        if (m_write_queue.flush_interval().count() > 0 &&
            !m_flush_running.exchange(true, std::memory_order_acq_rel)) {
            asio::co_spawn(m_io, auto_flush_loop(), asio::detached);
        }

        return status();
    }

    // Flush the write queue - sends all queued messages in a single write
    virtual asio::awaitable<status> flush() override {
        if (!m_is_connected) {
            co_return status(error_code::not_connected);
        }

        if (m_write_queue.empty()) {
            co_return status();
        }

        // Dequeue all pending messages
        std::string batch;
        batch.reserve(m_write_queue.pending_bytes() + 256);
        m_write_queue.dequeue_all(batch);

        if (batch.empty()) {
            co_return status();
        }

        // Write all messages in a single syscall
        try {
            co_await asio::async_write(m_socket, asio::buffer(batch), asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return status(e.code().message());
        }

        co_return status();
    }

    // Configure write coalescing behavior
    virtual void set_write_coalescing(std::chrono::microseconds flush_interval,
                                      size_t max_pending_bytes) override {
        m_write_queue.set_flush_interval(flush_interval);
        m_write_queue.set_max_pending_bytes(max_pending_bytes);
    }

    // Get number of bytes currently queued for writing
    [[nodiscard]] virtual size_t pending_bytes() const noexcept override {
        return m_write_queue.pending_bytes();
    }

    virtual asio::awaitable<status> unsubscribe(const isubscription_sptr& p) override {
        co_return co_await unsubscribe_impl(p);
    }

    virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_cb cb, subscribe_options opts = {}) override {
        if (!m_is_connected) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(error_code::not_connected)};
        }

        auto sid = next_sid();
        std::string payload = opts.queue_group.has_value()
                                  ? fmt::format(fmt::runtime(protocol::sub_fmt), subject, opts.queue_group.value(), sid)
                                  : fmt::format(fmt::runtime(protocol::sub_fmt), subject, "", sid);

        try {
            co_await asio::async_write(m_socket, asio::buffer(payload), asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(e.code().message())};
        }

        auto sub = std::make_shared<subscription>(sid, cb, opts.max_messages);
        m_subs.emplace(sid, sub);

        co_return std::pair<isubscription_sptr, status>{sub, status()};
    }

    // Subscribe with headers callback (for JetStream HMSG messages)
    virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_with_headers_cb cb, subscribe_options opts = {}) override {
        if (!m_is_connected) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(error_code::not_connected)};
        }

        auto sid = next_sid();
        std::string payload = opts.queue_group.has_value()
                                  ? fmt::format(fmt::runtime(protocol::sub_fmt), subject, opts.queue_group.value(), sid)
                                  : fmt::format(fmt::runtime(protocol::sub_fmt), subject, "", sid);

        try {
            co_await asio::async_write(m_socket, asio::buffer(payload), asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(e.code().message())};
        }

        auto sub = std::make_shared<subscription>(sid, cb, opts.max_messages);
        m_subs.emplace(sid, sub);

        co_return std::pair<isubscription_sptr, status>{sub, status()};
    }

    // Zero-copy subscribe - callback receives references to internal buffers
    virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_zero_copy_cb cb, subscribe_options opts = {}) override {
        if (!m_is_connected) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(error_code::not_connected)};
        }

        auto sid = next_sid();
        std::string payload = opts.queue_group.has_value()
                                  ? fmt::format(fmt::runtime(protocol::sub_fmt), subject, opts.queue_group.value(), sid)
                                  : fmt::format(fmt::runtime(protocol::sub_fmt), subject, "", sid);

        try {
            co_await asio::async_write(m_socket, asio::buffer(payload), asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(e.code().message())};
        }

        auto sub = std::make_shared<subscription>(sid, cb, opts.max_messages);
        m_subs.emplace(sid, sub);

        co_return std::pair<isubscription_sptr, status>{sub, status()};
    }

    // Publish with headers (HPUB)
    virtual asio::awaitable<status> publish(string_view subject, std::span<const char> payload,
                                            const headers_t& headers,
                                            optional<string_view> reply_to = {}) override {
        if (!m_is_connected) {
            co_return status(error_code::not_connected);
        }

        // Use pooled buffers
        pooled_string hdr_data(m_pools.large_strings);
        serialize_headers_to(*hdr_data, headers);
        std::size_t hdr_len = hdr_data->size();
        std::size_t total_len = hdr_len + payload.size();

        if (m_max_payload > 0 && total_len > m_max_payload) {
            co_return status(error_code::message_too_large);
        }

        pooled_const_buffer_vector buffers(m_pools.buffer_vectors);
        pooled_string cmd(m_pools.strings);

        if (reply_to.has_value()) {
            *cmd = fmt::format(fmt::runtime(protocol::hpub_fmt), subject, reply_to.value(), hdr_len, total_len);
        } else {
            *cmd = fmt::format(fmt::runtime(protocol::hpub_no_reply_fmt), subject, hdr_len, total_len);
        }

        buffers->emplace_back(asio::buffer(cmd->data(), cmd->size()));
        buffers->emplace_back(asio::buffer(hdr_data->data(), hdr_data->size()));
        buffers->emplace_back(asio::buffer(payload.data(), payload.size()));
        buffers->emplace_back(asio::buffer(protocol::crlf.data(), protocol::crlf.size()));

        try {
            co_await asio::async_write(m_socket, *buffers, asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return status(e.code().message());
        }

        co_return status();
    }

    // Request-reply pattern
    virtual asio::awaitable<std::pair<message, status>> request(
        string_view subject, std::span<const char> payload,
        std::chrono::milliseconds timeout) override {
        co_return co_await request_impl(subject, payload, {}, timeout);
    }

    // Request-reply with headers
    virtual asio::awaitable<std::pair<message, status>> request(
        string_view subject, std::span<const char> payload, const headers_t& headers,
        std::chrono::milliseconds timeout) override {
        co_return co_await request_impl(subject, payload, headers, timeout);
    }

    // JetStream publish
    virtual asio::awaitable<std::pair<js_pub_ack, status>> js_publish(
        string_view subject, std::span<const char> payload,
        std::chrono::milliseconds timeout, bool wait_for_ack) override {
        co_return co_await js_publish_impl(subject, payload, {}, timeout, wait_for_ack);
    }

    // JetStream publish with headers
    virtual asio::awaitable<std::pair<js_pub_ack, status>> js_publish(
        string_view subject, std::span<const char> payload, const headers_t& headers,
        std::chrono::milliseconds timeout, bool wait_for_ack) override {
        co_return co_await js_publish_impl(subject, payload, headers, timeout, wait_for_ack);
    }

    // JetStream fire-and-forget publish (no ack, no timeout)
    virtual asio::awaitable<status> js_publish_async(
        string_view subject, std::span<const char> payload) override {
        co_return co_await publish(subject, payload, std::nullopt);
    }

    // JetStream fire-and-forget publish with headers (no ack, no timeout)
    virtual asio::awaitable<status> js_publish_async(
        string_view subject, std::span<const char> payload, const headers_t& headers) override {
        co_return co_await publish(subject, payload, headers, std::nullopt);
    }

    // JetStream subscribe (push consumer)
    virtual asio::awaitable<std::pair<ijs_subscription_sptr, status>>
    js_subscribe(const js_consumer_config& config, on_js_message_cb cb) override {
        co_return co_await js_subscribe_impl(config, std::move(cb));
    }

    // JetStream fetch (pull consumer)
    virtual asio::awaitable<std::pair<std::vector<js_message>, status>>
    js_fetch(string_view stream, string_view consumer, uint32_t batch,
             std::chrono::milliseconds timeout) override {
        co_return co_await js_fetch_impl(stream, consumer, batch, timeout);
    }

    // Get consumer info
    virtual asio::awaitable<std::pair<nats_asio::js_consumer_info, status>>
    js_get_consumer_info(string_view stream, string_view consumer) override {
        co_return co_await js_consumer_info_impl(stream, consumer);
    }

    // Delete a consumer
    virtual asio::awaitable<status>
    js_delete_consumer(string_view stream, string_view consumer) override {
        co_return co_await js_delete_consumer_impl(stream, consumer);
    }

    // KV put
    virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_put(string_view bucket, string_view key, std::span<const char> value,
           std::chrono::milliseconds timeout) override {
        co_return co_await kv_put_impl(bucket, key, value, timeout);
    }

    // KV get
    virtual asio::awaitable<std::pair<kv_entry, status>>
    kv_get(string_view bucket, string_view key,
           std::chrono::milliseconds timeout) override {
        co_return co_await kv_get_impl(bucket, key, timeout);
    }

    // KV delete
    virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_delete(string_view bucket, string_view key,
              std::chrono::milliseconds timeout) override {
        co_return co_await kv_delete_impl(bucket, key, timeout);
    }

    // KV purge (delete and clear history)
    virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_purge(string_view bucket, string_view key,
             std::chrono::milliseconds timeout) override {
        co_return co_await kv_purge_impl(bucket, key, timeout);
    }

    // KV create (only if key doesn't exist or was deleted)
    virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_create(string_view bucket, string_view key, std::span<const char> value,
              std::chrono::milliseconds timeout) override {
        co_return co_await kv_create_impl(bucket, key, value, timeout);
    }

    // KV update (only if revision matches)
    virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_update(string_view bucket, string_view key, std::span<const char> value,
              uint64_t revision, std::chrono::milliseconds timeout) override {
        co_return co_await kv_update_impl(bucket, key, value, revision, timeout);
    }

    // KV keys (list all keys in bucket)
    virtual asio::awaitable<std::pair<std::vector<std::string>, status>>
    kv_keys(string_view bucket, std::chrono::milliseconds timeout) override {
        co_return co_await kv_keys_impl(bucket, timeout);
    }

    // KV history (get all revisions of a key)
    virtual asio::awaitable<std::pair<std::vector<kv_entry>, status>>
    kv_history(string_view bucket, string_view key, std::chrono::milliseconds timeout) override {
        co_return co_await kv_history_impl(bucket, key, timeout);
    }

    // KV revert (restore key to a previous revision)
    virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_revert(string_view bucket, string_view key, uint64_t revision,
              std::chrono::milliseconds timeout) override {
        co_return co_await kv_revert_impl(bucket, key, revision, timeout);
    }

    // KV watch
    virtual asio::awaitable<std::pair<ikv_watcher_sptr, status>>
    kv_watch(string_view bucket, on_kv_entry_cb cb, string_view key) override {
        co_return co_await kv_watch_impl(bucket, std::move(cb), key);
    }

private:
    // Shared state for request-reply pattern
    struct request_state {
        std::atomic<bool> received{false};
        message response;
    };

    // Request implementation
    asio::awaitable<std::pair<message, status>> request_impl(
        string_view subject, std::span<const char> payload,
        const headers_t& headers, std::chrono::milliseconds timeout) {

        if (!m_is_connected) {
            co_return std::pair<message, status>{{}, status(error_code::not_connected)};
        }

        // Generate unique inbox
        auto inbox = generate_inbox();

        // Create shared state for response
        auto state = std::make_shared<request_state>();

        // Subscribe to inbox with auto-unsubscribe after 1 message
        auto [sub, sub_status] = co_await subscribe(
            inbox,
            [state](string_view subj, optional<string_view> reply,
                   std::span<const char> data) -> asio::awaitable<void> {
                if (!state->received.exchange(true)) {
                    state->response.subject = std::string(subj);
                    if (reply) state->response.reply_to = std::string(*reply);
                    state->response.payload.assign(data.begin(), data.end());
                }
                co_return;
            },
            {.max_messages = 1});

        if (sub_status.failed()) {
            co_return std::pair<message, status>{{}, sub_status};
        }

        // Publish request with reply-to inbox
        status pub_status;
        if (headers.empty()) {
            pub_status = co_await publish(subject, payload, inbox);
        } else {
            pub_status = co_await publish(subject, payload, headers, inbox);
        }

        if (pub_status.failed()) {
            co_await unsubscribe(sub);
            co_return std::pair<message, status>{{}, pub_status};
        }

        // Wait for response with timeout using polling
        asio::steady_timer timer(co_await asio::this_coro::executor);
        auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!state->received.load()) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                co_await unsubscribe(sub);
                co_return std::pair<message, status>{{}, status(error_code::request_timeout)};
            }

            // Poll every 5ms
            timer.expires_after(std::chrono::milliseconds(5));
            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
            if (ec && ec != asio::error::operation_aborted) {
                co_await unsubscribe(sub);
                co_return std::pair<message, status>{{}, status(ec.message())};
            }
        }

        co_return std::pair<message, status>{std::move(state->response), status()};
    }

    // JetStream publish implementation
    asio::awaitable<std::pair<js_pub_ack, status>> js_publish_impl(
        string_view subject, std::span<const char> payload,
        const headers_t& headers, std::chrono::milliseconds timeout, bool wait_for_ack) {

        // Fire-and-forget mode: just publish without waiting for reply
        if (!wait_for_ack) {
            status pub_status;
            if (headers.empty()) {
                pub_status = co_await publish(subject, payload, optional<string_view>{});
            } else {
                pub_status = co_await publish(subject, payload, headers, optional<string_view>{});
            }
            co_return std::pair<js_pub_ack, status>{{}, pub_status};
        }

        // Use request-reply pattern for acknowledged publish
        auto [response, req_status] = co_await request_impl(subject, payload, headers, timeout);

        if (req_status.failed()) {
            co_return std::pair<js_pub_ack, status>{{}, req_status};
        }

        // Parse JetStream pub ack from response using simdjson
        js_pub_ack ack;
        fast_json parser;
        string_view payload_sv(response.payload.data(), response.payload.size());

        if (!parser.parse(payload_sv)) {
            co_return std::pair<js_pub_ack, status>{{}, status(fmt::format("failed to parse pub ack: {}", parser.error()))};
        }

        if (parser.contains("error")) {
            auto err_desc = parser.get_error_description();
            co_return std::pair<js_pub_ack, status>{{}, status(err_desc.empty() ? "unknown JetStream error" : std::string(err_desc))};
        }

        if (parser.contains("stream")) {
            ack.stream = std::string(parser.get_string("stream"));
        }
        ack.sequence = parser.get_uint("seq", 0);
        if (parser.contains("domain")) {
            ack.domain = std::string(parser.get_string("domain"));
        }
        ack.duplicate = parser.get_bool("duplicate", false);

        co_return std::pair<js_pub_ack, status>{std::move(ack), status()};
    }

    // Create consumer via JetStream API
    asio::awaitable<std::pair<nats_asio::js_consumer_info, status>> create_consumer(
        const js_consumer_config& config, const std::string& deliver_subject) {

        using nlohmann::json;

        // Build consumer config JSON
        json consumer_config;
        consumer_config["deliver_subject"] = deliver_subject;
        consumer_config["ack_policy"] = js_ack_policy_to_string(config.ack);
        consumer_config["replay_policy"] = js_replay_policy_to_string(config.replay);
        consumer_config["deliver_policy"] = js_deliver_policy_to_string(config.deliver);
        consumer_config["ack_wait"] = config.ack_wait.count() * 1000000000LL;  // nanoseconds
        consumer_config["max_ack_pending"] = static_cast<int64_t>(config.max_ack_pending);

        if (config.durable_name) {
            consumer_config["durable_name"] = *config.durable_name;
        }
        if (config.filter_subject) {
            consumer_config["filter_subject"] = *config.filter_subject;
        }
        if (config.deliver_group) {
            consumer_config["deliver_group"] = *config.deliver_group;
        }
        if (config.max_deliver > 0) {
            consumer_config["max_deliver"] = static_cast<int64_t>(config.max_deliver);
        }
        if (config.opt_start_seq) {
            consumer_config["opt_start_seq"] = static_cast<int64_t>(*config.opt_start_seq);
        }
        if (config.flow_control) {
            consumer_config["flow_control"] = true;
        }
        if (config.idle_heartbeat.count() > 0) {
            consumer_config["idle_heartbeat"] = config.idle_heartbeat.count() * 1000000LL;  // nanoseconds
        }

        json req;
        req["stream_name"] = config.stream;
        req["config"] = consumer_config;

        auto payload_str = req.dump();
        std::span<const char> payload(payload_str.data(), payload_str.size());

        // API subject depends on whether it's durable or ephemeral
        std::string api_subject;
        if (config.durable_name) {
            api_subject = subjects::js_consumer_durable_create(config.stream, *config.durable_name);
        } else {
            api_subject = subjects::js_consumer_create(config.stream);
        }

        auto [response, req_status] = co_await request_impl(api_subject, payload, {},
                                                            std::chrono::seconds(5));

        if (req_status.failed()) {
            co_return std::pair<nats_asio::js_consumer_info, status>{{}, req_status};
        }

        // Parse response using simdjson
        nats_asio::js_consumer_info info;
        fast_json parser;
        string_view resp_sv(response.payload.data(), response.payload.size());

        if (!parser.parse(resp_sv)) {
            co_return std::pair<nats_asio::js_consumer_info, status>{
                {}, status(fmt::format("failed to parse consumer info: {}", parser.error()))};
        }

        if (parser.contains("error")) {
            auto err_desc = parser.get_error_description();
            auto err_code = parser.get_int("error", "err_code", 0);
            co_return std::pair<nats_asio::js_consumer_info, status>{
                {}, status(fmt::format("{} (code: {})",
                    err_desc.empty() ? "unknown JetStream error" : std::string(err_desc), err_code))};
        }

        auto stream_name = parser.get_string("stream_name");
        info.stream = stream_name.empty() ? config.stream : std::string(stream_name);
        auto name = parser.get_string("name");
        info.name = name.empty() ? config.durable_name.value_or("") : std::string(name);
        auto deliver_subj = parser.get_string("config", "deliver_subject");
        info.deliver_subject = deliver_subj.empty() ? deliver_subject : std::string(deliver_subj);
        info.delivered_stream_seq = parser.get_uint("delivered", "stream_seq", 0);
        info.delivered_consumer_seq = parser.get_uint("delivered", "consumer_seq", 0);
        info.num_pending = parser.get_uint("num_pending", 0);
        info.num_ack_pending = parser.get_uint("num_ack_pending", 0);
        info.num_redelivered = parser.get_uint("num_redelivered", 0);

        co_return std::pair<nats_asio::js_consumer_info, status>{std::move(info), status()};
    }

    // JetStream subscribe implementation (push consumer)
    asio::awaitable<std::pair<ijs_subscription_sptr, status>> js_subscribe_impl(
        const js_consumer_config& config, on_js_message_cb cb) {

        if (!m_is_connected) {
            co_return std::pair<ijs_subscription_sptr, status>{nullptr, status(error_code::not_connected)};
        }

        // Generate delivery subject if not provided
        std::string deliver_subject;
        if (config.deliver_subject) {
            deliver_subject = *config.deliver_subject;
        } else {
            deliver_subject = generate_inbox();
        }

        // Create or bind to consumer
        auto [consumer_info, create_status] = co_await create_consumer(config, deliver_subject);
        if (create_status.failed()) {
            co_return std::pair<ijs_subscription_sptr, status>{nullptr, create_status};
        }

        // Create the JetStream subscription that will hold state
        auto js_sub = std::make_shared<js_subscription<SocketType>>(this, consumer_info, nullptr);

        // Subscribe to delivery subject using headers callback for HMSG
        on_message_with_headers_cb headers_callback =
            [js_sub, cb = std::move(cb)](const message& msg) -> asio::awaitable<void> {
                if (!js_sub->is_active()) {
                    co_return;
                }

                // Parse JetStream metadata from headers
                auto js_msg = parse_js_message_metadata(msg);

                // Call user callback
                co_await cb(*js_sub, js_msg);
            };

        auto [underlying_sub, sub_status] = co_await subscribe(
            deliver_subject,
            std::move(headers_callback),
            subscribe_options{
                .queue_group = config.deliver_group
                    ? optional<string_view>(*config.deliver_group)
                    : std::nullopt
            });

        if (sub_status.failed()) {
            co_return std::pair<ijs_subscription_sptr, status>{nullptr, sub_status};
        }

        // Update js_subscription with the underlying subscription
        js_sub = std::make_shared<js_subscription<SocketType>>(this, consumer_info, underlying_sub);

        co_return std::pair<ijs_subscription_sptr, status>{js_sub, status()};
    }

    // JetStream fetch implementation (pull consumer)
    asio::awaitable<std::pair<std::vector<js_message>, status>> js_fetch_impl(
        string_view stream, string_view consumer, uint32_t batch,
        std::chrono::milliseconds timeout) {

        if (!m_is_connected) {
            co_return std::pair<std::vector<js_message>, status>{{}, status(error_code::not_connected)};
        }

        using nlohmann::json;

        // Build fetch request
        json req;
        req["batch"] = batch;
        req["expires"] = timeout.count() * 1000000LL;  // nanoseconds

        auto payload_str = req.dump();
        std::span<const char> payload(payload_str.data(), payload_str.size());

        // Generate inbox for responses
        auto inbox = generate_inbox();

        // Shared state for collecting messages
        struct fetch_state {
            std::atomic<uint32_t> received{0};
            std::atomic<bool> done{false};
            std::vector<js_message> messages;
            std::mutex mutex;
        };
        auto state = std::make_shared<fetch_state>();

        // Subscribe to inbox using headers callback for HMSG
        on_message_with_headers_cb headers_callback =
            [state, batch](const message& msg) -> asio::awaitable<void> {
                // Check for end of batch marker (status header indicates no messages or timeout)
                // JetStream sends Status: 408 Request Timeout or Status: 404 No Messages
                for (const auto& [key, value] : msg.headers) {
                    if (key == "Status") {
                        if (value == "408" || value == "404") {
                            state->done.store(true);
                            co_return;
                        }
                    }
                }

                // Check for empty payload (also indicates end of batch)
                if (msg.payload.empty()) {
                    state->done.store(true);
                    co_return;
                }

                auto js_msg = parse_js_message_metadata(msg);

                {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    state->messages.push_back(std::move(js_msg));
                }

                if (++state->received >= batch) {
                    state->done.store(true);
                }
                co_return;
            };

        auto [sub, sub_status] = co_await subscribe(inbox, std::move(headers_callback));

        if (sub_status.failed()) {
            co_return std::pair<std::vector<js_message>, status>{{}, sub_status};
        }

        // Send fetch request
        auto api_subject = subjects::js_consumer_fetch(stream, consumer);
        auto pub_status = co_await publish(api_subject, payload, inbox);

        if (pub_status.failed()) {
            co_await unsubscribe(sub);
            co_return std::pair<std::vector<js_message>, status>{{}, pub_status};
        }

        // Wait for messages with timeout
        asio::steady_timer timer(co_await asio::this_coro::executor);
        auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!state->done.load()) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                break;  // Timeout - return what we have
            }

            timer.expires_after(std::chrono::milliseconds(10));
            co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
        }

        co_await unsubscribe(sub);

        std::vector<js_message> result;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            result = std::move(state->messages);
        }

        co_return std::pair<std::vector<js_message>, status>{std::move(result), status()};
    }

    // Get consumer info implementation
    asio::awaitable<std::pair<nats_asio::js_consumer_info, status>> js_consumer_info_impl(
        string_view stream, string_view consumer) {

        if (!m_is_connected) {
            co_return std::pair<nats_asio::js_consumer_info, status>{{}, status(error_code::not_connected)};
        }

        auto api_subject = subjects::js_consumer_info(stream, consumer);
        auto [response, req_status] = co_await request_impl(api_subject, {}, {}, std::chrono::seconds(5));

        if (req_status.failed()) {
            co_return std::pair<nats_asio::js_consumer_info, status>{{}, req_status};
        }

        // Parse response using simdjson
        nats_asio::js_consumer_info info;
        fast_json parser;
        string_view resp_sv(response.payload.data(), response.payload.size());

        if (!parser.parse(resp_sv)) {
            co_return std::pair<nats_asio::js_consumer_info, status>{
                {}, status(fmt::format("failed to parse consumer info: {}", parser.error()))};
        }

        if (parser.contains("error")) {
            auto err_desc = parser.get_error_description();
            co_return std::pair<nats_asio::js_consumer_info, status>{
                {}, status(err_desc.empty() ? "unknown JetStream error" : std::string(err_desc))};
        }

        auto stream_name = parser.get_string("stream_name");
        info.stream = stream_name.empty() ? std::string(stream) : std::string(stream_name);
        auto name = parser.get_string("name");
        info.name = name.empty() ? std::string(consumer) : std::string(name);
        info.deliver_subject = std::string(parser.get_string("config", "deliver_subject"));
        info.delivered_stream_seq = parser.get_uint("delivered", "stream_seq", 0);
        info.delivered_consumer_seq = parser.get_uint("delivered", "consumer_seq", 0);
        info.num_pending = parser.get_uint("num_pending", 0);
        info.num_ack_pending = parser.get_uint("num_ack_pending", 0);
        info.num_redelivered = parser.get_uint("num_redelivered", 0);

        co_return std::pair<nats_asio::js_consumer_info, status>{std::move(info), status()};
    }

    // Delete consumer implementation
    asio::awaitable<status> js_delete_consumer_impl(string_view stream, string_view consumer) {
        if (!m_is_connected) {
            co_return status(error_code::not_connected);
        }

        auto api_subject = subjects::js_consumer_delete(stream, consumer);
        auto [response, req_status] = co_await request_impl(api_subject, {}, {}, std::chrono::seconds(5));

        if (req_status.failed()) {
            co_return req_status;
        }

        // Parse response using simdjson
        fast_json parser;
        string_view resp_sv(response.payload.data(), response.payload.size());

        if (!parser.parse(resp_sv)) {
            co_return status(fmt::format("failed to parse delete response: {}", parser.error()));
        }

        if (parser.contains("error")) {
            auto err_desc = parser.get_error_description();
            co_return status(err_desc.empty() ? "unknown JetStream error" : std::string(err_desc));
        }

        if (!parser.get_bool("success", false)) {
            co_return status(error_code::operation_failed, "consumer deletion failed");
        }

        co_return status();
    }

    // KV put implementation
    asio::awaitable<std::pair<uint64_t, status>> kv_put_impl(
        string_view bucket, string_view key, std::span<const char> value,
        std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status(error_code::not_connected)};
        }

        // KV subject format: $KV.{bucket}.{key} (using cached prefix)
        std::string subject = m_kv_cache.kv_subject(bucket, key);

        // Use JetStream publish to get acknowledgment with sequence number
        auto [ack, pub_status] = co_await js_publish_impl(subject, value, {}, timeout, true);

        if (pub_status.failed()) {
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV get implementation
    asio::awaitable<std::pair<kv_entry, status>> kv_get_impl(
        string_view bucket, string_view key,
        std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<kv_entry, status>{{}, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<kv_entry, status>{{}, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<kv_entry, status>{{}, status(error_code::not_connected)};
        }

        using nlohmann::json;

        // Get cached prefixes for this bucket
        auto prefixes = m_kv_cache.get(bucket);

        // Request body with the subject (key)
        std::string kv_subject = m_kv_cache.kv_subject(bucket, key);
        json req;
        req["last_by_subj"] = kv_subject;

        auto payload_str = req.dump();
        std::span<const char> payload(payload_str.data(), payload_str.size());

        auto [response, req_status] = co_await request_impl(prefixes.direct_get, payload, {}, timeout);

        if (req_status.failed()) {
            co_return std::pair<kv_entry, status>{{}, req_status};
        }

        // Check for error status in headers
        for (const auto& [hdr_key, hdr_val] : response.headers) {
            if (hdr_key == "Status") {
                if (hdr_val == "404") {
                    co_return std::pair<kv_entry, status>{{}, status(error_code::key_not_found)};
                }
                co_return std::pair<kv_entry, status>{{}, status(fmt::format("error: {}", hdr_val))};
            }
        }

        // Parse entry from response
        kv_entry entry;
        entry.bucket = std::string(bucket);
        entry.key = std::string(key);
        entry.value.assign(response.payload.begin(), response.payload.end());

        // Extract metadata from headers
        for (const auto& [hdr_key, hdr_val] : response.headers) {
            if (hdr_key == "Nats-Sequence") {
                parse_int(hdr_val, entry.revision);
            } else if (hdr_key == "Nats-Time-Stamp") {
                // Parse timestamp (simplified)
                std::tm tm = {};
                std::istringstream ss(hdr_val);
                ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
                entry.created = std::chrono::system_clock::from_time_t(std::mktime(&tm));
            } else if (hdr_key == "KV-Operation") {
                if (hdr_val == "DEL") {
                    entry.op = kv_entry::operation::del;
                } else if (hdr_val == "PURGE") {
                    entry.op = kv_entry::operation::purge;
                }
            }
        }

        // If this is a delete marker, return not found
        if (entry.op != kv_entry::operation::put) {
            co_return std::pair<kv_entry, status>{{}, status(error_code::key_not_found)};
        }

        co_return std::pair<kv_entry, status>{std::move(entry), status()};
    }

    // KV delete implementation
    asio::awaitable<std::pair<uint64_t, status>> kv_delete_impl(
        string_view bucket, string_view key,
        std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status(error_code::not_connected)};
        }

        // KV subject format: $KV.{bucket}.{key} (using cached prefix)
        std::string subject = m_kv_cache.kv_subject(bucket, key);

        // Publish empty payload with KV-Operation: DEL header
        headers_t headers = {{"KV-Operation", "DEL"}};
        std::span<const char> empty_payload;

        auto [ack, pub_status] = co_await js_publish_impl(subject, empty_payload, headers, timeout, true);

        if (pub_status.failed()) {
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV purge implementation - deletes key and clears all history
    asio::awaitable<std::pair<uint64_t, status>> kv_purge_impl(
        string_view bucket, string_view key,
        std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status(error_code::not_connected)};
        }

        // KV subject format: $KV.{bucket}.{key} (using cached prefix)
        std::string subject = m_kv_cache.kv_subject(bucket, key);

        // Publish empty payload with KV-Operation: PURGE and Nats-Rollup: sub headers
        // Nats-Rollup: sub tells the server to remove all previous messages for this subject
        headers_t headers = {
            {"KV-Operation", "PURGE"},
            {"Nats-Rollup", "sub"}
        };
        std::span<const char> empty_payload;

        auto [ack, pub_status] = co_await js_publish_impl(subject, empty_payload, headers, timeout, true);

        if (pub_status.failed()) {
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV create implementation - creates only if key doesn't exist or was deleted
    asio::awaitable<std::pair<uint64_t, status>> kv_create_impl(
        string_view bucket, string_view key, std::span<const char> value,
        std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status(error_code::not_connected)};
        }

        // KV subject format: $KV.{bucket}.{key} (using cached prefix)
        std::string subject = m_kv_cache.kv_subject(bucket, key);

        // Use Nats-Expected-Last-Subject-Sequence: 0 to ensure key doesn't exist
        headers_t headers = {{"Nats-Expected-Last-Subject-Sequence", "0"}};

        auto [ack, pub_status] = co_await js_publish_impl(subject, value, headers, timeout, true);

        if (pub_status.failed()) {
            // Check if it's a sequence mismatch error (key already exists)
            if (pub_status.error().find("wrong last sequence") != std::string::npos) {
                co_return std::pair<uint64_t, status>{0, status(error_code::already_exists)};
            }
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV update implementation - updates only if revision matches
    asio::awaitable<std::pair<uint64_t, status>> kv_update_impl(
        string_view bucket, string_view key, std::span<const char> value,
        uint64_t revision, std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }

        if (revision == 0) {
            co_return std::pair<uint64_t, status>{0, status(error_code::invalid_argument, "revision must be > 0 for update, use create for new keys")};
        }

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status(error_code::not_connected)};
        }

        // KV subject format: $KV.{bucket}.{key} (using cached prefix)
        std::string subject = m_kv_cache.kv_subject(bucket, key);

        // Use Nats-Expected-Last-Subject-Sequence to ensure revision matches
        headers_t headers = {{"Nats-Expected-Last-Subject-Sequence", std::to_string(revision)}};

        auto [ack, pub_status] = co_await js_publish_impl(subject, value, headers, timeout, true);

        if (pub_status.failed()) {
            // Check if it's a sequence mismatch error
            if (pub_status.error().find("wrong last sequence") != std::string::npos) {
                co_return std::pair<uint64_t, status>{0, status(error_code::revision_mismatch)};
            }
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV keys implementation - list all keys in a bucket
    asio::awaitable<std::pair<std::vector<std::string>, status>> kv_keys_impl(
        string_view bucket, std::chrono::milliseconds timeout) {

        // Validate bucket
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<std::vector<std::string>, status>{{}, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<std::vector<std::string>, status>{{}, status(error_code::not_connected)};
        }

        // Get cached prefixes for this bucket
        auto prefixes = m_kv_cache.get(bucket);

        // Request body with subjects filter to get all subjects (use nlohmann for building)
        nlohmann::json req;
        req["subjects_filter"] = subjects::kv_wildcard(bucket);

        auto payload_str = req.dump();
        std::span<const char> payload(payload_str.data(), payload_str.size());

        auto [response, req_status] = co_await request_impl(prefixes.stream_info, payload, {}, timeout);

        if (req_status.failed()) {
            co_return std::pair<std::vector<std::string>, status>{{}, req_status};
        }

        // Parse response using simdjson
        std::vector<std::string> keys;
        fast_json parser;
        string_view resp_sv(response.payload.data(), response.payload.size());

        if (!parser.parse(resp_sv)) {
            co_return std::pair<std::vector<std::string>, status>{{}, status(fmt::format("failed to parse response: {}", parser.error()))};
        }

        // Check for error
        if (parser.contains("error")) {
            auto err_desc = parser.get_error_description();
            co_return std::pair<std::vector<std::string>, status>{{}, status(err_desc.empty() ? "stream info failed" : std::string(err_desc))};
        }

        // Extract keys from state.subjects map using cached prefix
        // Format: { "state": { "subjects": { "$KV.bucket.key1": 1, "$KV.bucket.key2": 1, ... } } }
        const auto& prefix = prefixes.kv_prefix;
        auto state = parser.doc()["state"];
        if (!state.error()) {
            auto subjects = state["subjects"];
            if (!subjects.error()) {
                auto obj = subjects.get_object();
                if (!obj.error()) {
                    for (auto [subject_key, count] : obj.value()) {
                        std::string_view subject(subject_key);
                        // Strip the $KV.bucket. prefix to get the key name
                        if (subject.size() > prefix.size() && subject.substr(0, prefix.size()) == prefix) {
                            keys.emplace_back(subject.substr(prefix.size()));
                        }
                    }
                }
            }
        }

        co_return std::pair<std::vector<std::string>, status>{std::move(keys), status()};
    }

    // KV history implementation - get all revisions of a key
    asio::awaitable<std::pair<std::vector<kv_entry>, status>> kv_history_impl(
        string_view bucket, string_view key, std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<std::vector<kv_entry>, status>{{}, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<std::vector<kv_entry>, status>{{}, status(err)};
        }

        if (!m_is_connected) {
            co_return std::pair<std::vector<kv_entry>, status>{{}, status(error_code::not_connected)};
        }

        using nlohmann::json;

        // Get cached prefixes and build subject
        auto prefixes = m_kv_cache.get(bucket);
        std::string kv_subject = m_kv_cache.kv_subject(bucket, key);

        std::vector<kv_entry> history;
        uint64_t last_seq = 0;

        // Iterate through all messages for this subject
        while (true) {
            json req;
            if (last_seq == 0) {
                // Get first message by subject
                req["next_by_subj"] = kv_subject;
            } else {
                // Get next message after last_seq
                req["seq"] = last_seq + 1;
                req["next_by_subj"] = kv_subject;
            }

            auto payload_str = req.dump();
            std::span<const char> payload(payload_str.data(), payload_str.size());

            auto [response, req_status] = co_await request_impl(prefixes.direct_get, payload, {}, timeout);

            if (req_status.failed()) {
                // Check if it's "no message found" - that means we've reached the end
                if (req_status.error().find("no message") != std::string::npos) {
                    break;
                }
                co_return std::pair<std::vector<kv_entry>, status>{{}, req_status};
            }

            // Check for error in response headers (404 means no more messages)
            bool is_error = false;
            for (const auto& [hdr_key, hdr_val] : response.headers) {
                if (hdr_key == "Status" && hdr_val.find("404") != std::string::npos) {
                    is_error = true;
                    break;
                }
            }
            if (is_error) {
                break;  // No more messages
            }

            // Parse the response as a KV entry
            kv_entry entry;
            entry.bucket = std::string(bucket);
            entry.key = std::string(key);
            entry.value.assign(response.payload.begin(), response.payload.end());
            entry.op = kv_entry::operation::put;

            // Parse metadata from headers
            for (const auto& [hdr_key, hdr_val] : response.headers) {
                if (hdr_key == "Nats-Sequence") {
                    parse_int(hdr_val, entry.revision);
                    last_seq = entry.revision;
                } else if (hdr_key == "Nats-Time-Stamp") {
                    std::tm tm = {};
                    std::istringstream ss(hdr_val);
                    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
                    entry.created = std::chrono::system_clock::from_time_t(std::mktime(&tm));
                } else if (hdr_key == "KV-Operation") {
                    if (hdr_val == "DEL") {
                        entry.op = kv_entry::operation::del;
                    } else if (hdr_val == "PURGE") {
                        entry.op = kv_entry::operation::purge;
                    }
                }
            }

            history.push_back(std::move(entry));

            // Safety check to avoid infinite loop
            if (history.size() > 10000) {
                break;
            }
        }

        co_return std::pair<std::vector<kv_entry>, status>{std::move(history), status()};
    }

    // KV revert implementation - restore key to a previous revision
    asio::awaitable<std::pair<uint64_t, status>> kv_revert_impl(
        string_view bucket, string_view key, uint64_t revision,
        std::chrono::milliseconds timeout) {

        // Validate bucket and key
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }
        if (auto err = validate_kv_key(key); !err.empty()) {
            co_return std::pair<uint64_t, status>{0, status(err)};
        }

        if (revision == 0) {
            co_return std::pair<uint64_t, status>{0, status(error_code::invalid_argument, "revision must be > 0")};
        }

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status(error_code::not_connected)};
        }

        using nlohmann::json;

        // Get cached prefixes for this bucket
        auto prefixes = m_kv_cache.get(bucket);

        // Request the message at the specific sequence number
        json req;
        req["seq"] = revision;

        auto payload_str = req.dump();
        std::span<const char> payload(payload_str.data(), payload_str.size());

        auto [response, req_status] = co_await request_impl(prefixes.direct_get, payload, {}, timeout);

        if (req_status.failed()) {
            co_return std::pair<uint64_t, status>{0, status(fmt::format("failed to get revision {}: {}", revision, req_status.error()))};
        }

        // Check for error in response headers (404 means revision not found)
        for (const auto& [hdr_key, hdr_val] : response.headers) {
            if (hdr_key == "Status" && hdr_val.find("404") != std::string::npos) {
                co_return std::pair<uint64_t, status>{0, status(fmt::format("revision {} not found", revision))};
            }
        }

        // Verify this message is for the correct key by checking subject
        std::string expected_subject = m_kv_cache.kv_subject(bucket, key);
        if (response.subject != expected_subject) {
            co_return std::pair<uint64_t, status>{0, status(fmt::format("revision {} is not for key '{}'", revision, key))};
        }

        // Check if the revision was a delete/purge operation
        for (const auto& [hdr_key, hdr_val] : response.headers) {
            if (hdr_key == "KV-Operation") {
                if (hdr_val == "DEL" || hdr_val == "PURGE") {
                    co_return std::pair<uint64_t, status>{0, status(fmt::format("revision {} is a {} marker, cannot revert to it", revision, hdr_val))};
                }
            }
        }

        // Now put the value back as a new revision (reuse expected_subject)
        const std::string& subject = expected_subject;
        std::span<const char> value_span(response.payload.data(), response.payload.size());

        auto [ack, pub_status] = co_await js_publish_impl(subject, value_span, {}, timeout, true);

        if (pub_status.failed()) {
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV watch implementation
    asio::awaitable<std::pair<ikv_watcher_sptr, status>> kv_watch_impl(
        string_view bucket, on_kv_entry_cb cb, string_view key) {

        // Validate bucket
        if (auto err = validate_kv_bucket(bucket); !err.empty()) {
            co_return std::pair<ikv_watcher_sptr, status>{nullptr, status(err)};
        }
        // Validate key if provided (empty key means watch all)
        if (!key.empty()) {
            if (auto err = validate_kv_key(key); !err.empty()) {
                co_return std::pair<ikv_watcher_sptr, status>{nullptr, status(err)};
            }
        }

        if (!m_is_connected) {
            co_return std::pair<ikv_watcher_sptr, status>{nullptr, status(error_code::not_connected)};
        }

        std::string bucket_str(bucket);
        std::string key_filter_str(key);

        // Get cached prefixes
        auto prefixes = m_kv_cache.get(bucket);

        // Build filter subject: $KV.{bucket}.{key} or $KV.{bucket}.> for all keys
        std::string filter_subject;
        if (key.empty()) {
            filter_subject = subjects::kv_wildcard(bucket);
        } else {
            filter_subject = m_kv_cache.kv_subject(bucket, key);
        }

        // KV stream name (from cache)
        const std::string& stream_name = prefixes.stream_name;

        // Generate delivery subject for push consumer
        std::string deliver_subject = generate_inbox();

        // Create ephemeral push consumer for watching
        js_consumer_config config;
        config.stream = stream_name;
        config.filter_subject = filter_subject;
        config.deliver_subject = deliver_subject;
        config.ack = js_ack_policy::none;  // No ack needed for watch
        config.deliver = js_deliver_policy::last_per_subject;  // Start from current state
        config.replay = js_replay_policy::instant;

        // Create the consumer
        auto [consumer_info, create_status] = co_await create_consumer(config, deliver_subject);
        if (create_status.failed()) {
            co_return std::pair<ikv_watcher_sptr, status>{nullptr, create_status};
        }

        // Subscribe to delivery subject using headers callback
        on_message_with_headers_cb headers_callback =
            [bucket_str, cb = std::move(cb)](const message& msg) -> asio::awaitable<void> {
                // Parse KV entry from message
                auto entry = parse_kv_entry_from_message(msg, bucket_str);
                // Call user callback
                co_await cb(entry);
            };

        auto [underlying_sub, sub_status] = co_await subscribe(
            deliver_subject,
            std::move(headers_callback),
            subscribe_options{});

        if (sub_status.failed()) {
            co_return std::pair<ikv_watcher_sptr, status>{nullptr, sub_status};
        }

        // Create the watcher
        auto watcher = std::make_shared<kv_watcher<SocketType>>(
            bucket_str, key_filter_str, underlying_sub);

        co_return std::pair<ikv_watcher_sptr, status>{watcher, status()};
    }

    awaitable<void> on_ping() override {
        co_await asio::async_write(m_socket, asio::buffer(protocol::pong.data(), protocol::pong.size()),
                                   use_awaitable);
        co_return;
    }

    awaitable<void> on_pong() override {
        co_return;
    }

    awaitable<void> on_ok() override {
        co_return;
    }

    awaitable<void> on_error(string_view err) override {
        if (m_error_cb) {
            co_await m_error_cb(*this, err);
        }
        co_return;
    }

    awaitable<void> on_info(string_view info) override {
        fast_json parser;
        if (parser.parse(info)) {
            if (parser.contains("max_payload")) {
                m_max_payload = parser.get_uint("max_payload", m_max_payload);
            }
        } else {
            std::string err_msg = fmt::format("failed to parse INFO from server: {}", parser.error());
            co_await on_error(err_msg);
        }
        co_return;
    }

    awaitable<void> on_message(string_view subject, string_view sid_str,
                               optional<string_view> reply_to, std::size_t n) override {
        // Signed arithmetic: result can be negative if buffer already has enough data
        auto bytes_to_transfer = static_cast<std::ptrdiff_t>(n + 2) - static_cast<std::ptrdiff_t>(m_buf.size());

        if (bytes_to_transfer > 0) {
            co_await asio::async_read(m_socket, m_buf,
                                      asio::transfer_at_least(static_cast<std::size_t>(bytes_to_transfer)),
                                      use_awaitable);
        }

        std::size_t sid_u = 0;
        if (!parse_int(sid_str, sid_u)) {
            co_return;
        }

        auto it = m_subs.find(sid_u);
        if (it == m_subs.end()) {
            co_return;
        }

        if (it->second->is_cancelled()) {
            co_await unsubscribe_impl(it->second);
            co_return;  // Don't deliver message to cancelled subscription
        }

        auto b = m_buf.data();
        std::span<const char> payload_span(static_cast<const char*>(b.data()), n);

        // Check callback type and dispatch appropriately
        if (it->second->has_zero_copy_callback()) {
            // Zero-copy path for MSG (no headers)
            message_view msg_view;
            msg_view.subject = subject;
            if (reply_to) {
                msg_view.reply_to = *reply_to;
            }
            // No headers for regular MSG
            msg_view.payload = payload_span;

            co_await it->second->zero_copy_callback()(msg_view);
        } else {
            co_await it->second->callback()(subject, reply_to.has_value() ? reply_to : std::nullopt, payload_span);
        }

        // Check for auto-unsubscribe after delivering message
        if (it->second->increment_and_check()) {
            co_await unsubscribe_impl(it->second);
        }
        co_return;
    }

    awaitable<void> on_hmessage(string_view subject, string_view sid_str,
                                optional<string_view> reply_to,
                                std::size_t header_len, std::size_t total_len) override {
        // Calculate payload length (total_len includes headers)
        std::size_t payload_len = total_len - header_len;

        // Ensure we have enough data in buffer
        auto bytes_to_transfer = static_cast<std::ptrdiff_t>(total_len + 2) - static_cast<std::ptrdiff_t>(m_buf.size());
        if (bytes_to_transfer > 0) {
            co_await asio::async_read(m_socket, m_buf,
                                      asio::transfer_at_least(static_cast<std::size_t>(bytes_to_transfer)),
                                      use_awaitable);
        }

        std::size_t sid_u = 0;
        if (!parse_int(sid_str, sid_u)) {
            co_return;
        }

        auto it = m_subs.find(sid_u);
        if (it == m_subs.end()) {
            co_return;
        }

        if (it->second->is_cancelled()) {
            co_await unsubscribe_impl(it->second);
            co_return;
        }

        // Extract headers and payload from buffer
        auto b = m_buf.data();
        const char* data_ptr = static_cast<const char*>(b.data());

        // Headers are from data_ptr[0] to data_ptr[header_len]
        // Payload is from data_ptr[header_len] to data_ptr[total_len]
        std::span<const char> headers_span(data_ptr, header_len);
        std::span<const char> payload_span(data_ptr + header_len, payload_len);

        // Check callback type and dispatch appropriately
        if (it->second->has_zero_copy_callback()) {
            // Zero-copy path - no allocations, references internal buffers
            message_view msg_view;
            msg_view.subject = subject;
            if (reply_to) {
                msg_view.reply_to = *reply_to;
            }
            string_view headers_sv(headers_span.data(), headers_span.size());
            msg_view.headers = parse_headers_view(headers_sv);
            msg_view.payload = payload_span;

            co_await it->second->zero_copy_callback()(msg_view);
        } else if (it->second->has_headers_callback()) {
            // Build full message with headers (copies data)
            message msg;
            msg.subject = std::string(subject);
            if (reply_to) {
                msg.reply_to = std::string(*reply_to);
            }
            // Convert span to string_view for parse_headers
            string_view headers_sv(headers_span.data(), headers_span.size());
            msg.headers = parse_headers(headers_sv);
            msg.payload.assign(payload_span.begin(), payload_span.end());

            co_await it->second->headers_callback()(msg);
        } else {
            // Fall back to regular callback (without headers)
            co_await it->second->callback()(subject, reply_to.has_value() ? reply_to : std::nullopt, payload_span);
        }

        // Check for auto-unsubscribe after delivering message
        if (it->second->increment_and_check()) {
            co_await unsubscribe_impl(it->second);
        }
        co_return;
    }

    awaitable<void> consumed(std::size_t n) override {
        m_buf.consume(n);
        co_return;
    }

    awaitable<status> do_connect(const connect_config& conf) {
        try {
            tcp::resolver res(m_io);
            auto results =
                co_await res.async_resolve(conf.address, std::to_string(conf.port), use_awaitable);

            co_await asio::async_connect(m_socket.lowest_layer(), results, use_awaitable);

            // Disable Nagle's algorithm for lower latency
            m_socket.lowest_layer().set_option(asio::ip::tcp::no_delay(true));

#ifdef __linux__
            // Enable busy polling for lower latency (Linux only)
            // Spins for up to 50 microseconds before blocking
            int busy_poll_us = 50;
            ::setsockopt(m_socket.lowest_layer().native_handle(), SOL_SOCKET,
                        SO_BUSY_POLL, &busy_poll_us, sizeof(busy_poll_us));
#endif

            // Socket buffer tuning for throughput
            if (conf.send_buffer_size > 0) {
                m_socket.lowest_layer().set_option(
                    asio::socket_base::send_buffer_size(conf.send_buffer_size));
            }
            if (conf.recv_buffer_size > 0) {
                m_socket.lowest_layer().set_option(
                    asio::socket_base::receive_buffer_size(conf.recv_buffer_size));
            }

            co_await asio::async_read_until(m_socket, m_buf, std::string(protocol::crlf), use_awaitable);

            std::string header;
            std::istream is(&m_buf);
            auto s = co_await protocol_parser::parse_header(header, is, *this);
            if (s.failed()) {
                co_return s;
            }

            co_await m_socket.async_handshake();

            auto info = prepare_info(conf);
            co_await asio::async_write(m_socket, asio::buffer(info));

            co_return status{};
        } catch (const std::system_error& e) {
            co_return status(e.what());
        }
    }

    asio::awaitable<void> run(const connect_config& conf) {
        std::string header;
        uint32_t retry_delay_ms = conf.retry_initial_delay_ms;
        uint32_t retry_count = 0;

        for (;;) {
            if (m_stop_flag) {
                co_return;
            }

            if (!m_is_connected) {
                auto s = co_await do_connect(conf);
                if (s.failed()) {
                    // Check max attempts (0 = unlimited)
                    if (conf.retry_max_attempts > 0 && retry_count >= conf.retry_max_attempts) {
                        if (m_error_cb) {
                            co_await m_error_cb(*this, "max reconnection attempts reached");
                        }
                        co_return;
                    }

                    asio::steady_timer timer(co_await asio::this_coro::executor);
                    timer.expires_after(std::chrono::milliseconds(retry_delay_ms));
                    co_await timer.async_wait(asio::use_awaitable);

                    // Exponential backoff: double delay, cap at max
                    retry_delay_ms = std::min(retry_delay_ms * 2, conf.retry_max_delay_ms);
                    retry_count++;
                    continue;
                }

                // Reset backoff on successful connection
                retry_delay_ms = conf.retry_initial_delay_ms;
                retry_count = 0;

                m_is_connected = true;
                if (m_connected_cb) {
                    co_await m_connected_cb(*this);
                }
            }

            bool should_disconnect = false;
            try {
                co_await asio::async_read_until(m_socket, m_buf, std::string(protocol::crlf), asio::use_awaitable);

                std::istream is(&m_buf);
                auto s = co_await protocol_parser::parse_header(header, is, *this);
                if (s.failed()) {
                    continue;
                }
            } catch (const std::system_error&) {
                should_disconnect = true;
            }

            if (should_disconnect) {
                m_is_connected = false;
                asio::error_code ec;
                m_socket.close(ec);

                if (m_disconnected_cb) {
                    co_await m_disconnected_cb(*this);
                }
            }
        }
        co_return;
    }

    awaitable<status> unsubscribe_impl(const isubscription_sptr& p) {
        auto sid = p->sid();
        auto it = m_subs.find(sid);

        if (it == m_subs.end()) {
            co_return status(fmt::format("subscription not found {}", sid));
        }
        m_subs.erase(it);

        std::string unsub_payload(fmt::format(fmt::runtime(protocol::unsub_fmt), sid));
        try {
            co_await asio::async_write(m_socket, asio::buffer(unsub_payload), use_awaitable);
            co_return status{};
        } catch (const std::system_error& e) {
            co_return status(e.what());
        }
    }

    // Background coroutine for automatic write queue flushing
    asio::awaitable<void> auto_flush_loop() {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        while (m_is_connected && !m_stop_flag) {
            auto interval = m_write_queue.flush_interval();
            if (interval.count() == 0) {
                // Auto-flush disabled, stop the loop
                break;
            }

            // Wait for flush interval or until threshold exceeded
            timer.expires_after(interval);
            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));

            if (ec || !m_is_connected || m_stop_flag) {
                break;
            }

            // Flush if there's pending data
            if (!m_write_queue.empty()) {
                co_await flush();
            }

            // If queue is empty and no new data, we can stop the loop
            // It will be restarted when new data is queued
            if (m_write_queue.empty()) {
                break;
            }
        }

        m_flush_running.store(false, std::memory_order_release);
        co_return;
    }

    std::string prepare_info(const connect_config& o) {
        constexpr auto name = "nats_asio";
        constexpr auto lang = "cpp";
        constexpr auto version = "0.0.1";
        using nlohmann::json;
        json j = {
            {"verbose", o.verbose}, {"pedantic", o.pedantic}, {"name", name},
            {"lang", lang},         {"version", version},
        };

        if (o.user.has_value()) {
            j["user"] = o.user.value();
        }

        if (o.password.has_value()) {
            j["pass"] = o.password.value();
        }

        if (o.token.has_value()) {
            j["auth_token"] = o.token.value();
        }

        auto info = j.dump();
        auto connect_data = fmt::format(fmt::runtime(protocol::connect_fmt), info);
        return connect_data;
    }

    uint64_t next_sid() {
        return m_sid++;
    }

    std::atomic<uint64_t> m_sid;
    std::size_t m_max_payload;
    aio& m_io;

    std::atomic<bool> m_is_connected;
    std::atomic<bool> m_stop_flag;

    std::unordered_map<uint64_t, subscription_sptr> m_subs;
    on_connected_cb m_connected_cb;
    on_disconnected_cb m_disconnected_cb;
    on_error_cb m_error_cb;

    asio::streambuf m_buf;

    // Buffer pools for reduced allocations in hot paths
    mutable buffer_pools m_pools;

    // Write coalescing queue
    write_queue m_write_queue;
    std::atomic<bool> m_flush_running{false};

    // KV prefix cache for efficient subject building
    mutable kv_prefix_cache m_kv_cache;

    std::shared_ptr<ssl::context> m_ssl_ctx;
    uni_socket<SocketType> m_socket;
};

// Implementation of js_subscription::send_ack (needs connection class to be complete)
template <class SocketType>
asio::awaitable<status> js_subscription<SocketType>::send_ack(
    const js_message& msg, std::string_view ack_body) {

    if (!m_active.load()) {
        co_return status(error_code::invalid_argument, "subscription is not active");
    }

    if (!msg.msg.reply_to) {
        co_return status(error_code::invalid_argument, "message has no reply subject for acknowledgment");
    }

    std::span<const char> payload(ack_body.data(), ack_body.size());
    co_return co_await m_conn->publish(*msg.msg.reply_to, payload, std::nullopt);
}

// Batch ack implementation - sends all acks in a single write
template <class SocketType>
asio::awaitable<status> js_subscription<SocketType>::send_ack_batch(
    const std::vector<js_message>& messages, std::string_view ack_body) {

    if (!m_active.load()) {
        co_return status(error_code::invalid_argument, "subscription is not active");
    }

    if (messages.empty()) {
        co_return status();
    }

    // Build batch of PUB commands: "PUB reply_to len\r\nack_body\r\n" for each message
    std::string batch;
    // Estimate size: each ack is roughly "PUB <reply_to> <len>\r\n<ack_body>\r\n"
    // reply_to is typically ~60 chars, overhead ~20 chars
    batch.reserve(messages.size() * (80 + ack_body.size()));

    size_t acked_count = 0;
    for (const auto& msg : messages) {
        if (!msg.msg.reply_to) {
            continue;  // Skip messages without reply subject
        }
        // Format: PUB <reply_to> <payload_len>\r\n<payload>\r\n
        batch += fmt::format("PUB {} {}\r\n", *msg.msg.reply_to, ack_body.size());
        batch += ack_body;
        batch += "\r\n";
        ++acked_count;
    }

    if (acked_count == 0) {
        co_return status(error_code::invalid_argument, "no messages had reply subjects for acknowledgment");
    }

    // Send all acks in a single write
    std::span<const char> batch_span(batch.data(), batch.size());
    co_return co_await m_conn->write_raw(batch_span);
}

inline void load_certificates(const ssl_config& conf, ssl::context& ctx) {
    ctx.set_options(ssl::context::tls_client);

    if (conf.verify) {
        ctx.set_verify_mode(ssl::verify_peer);
    } else {
        ctx.set_verify_mode(ssl::verify_none);
    }

    if (!conf.cert.empty()) {
        ctx.use_certificate(asio::buffer(conf.cert.data(), conf.cert.size()),
                            ssl::context::file_format::pem);
    }

    if (!conf.ca.empty()) {
        ctx.add_certificate_authority(asio::buffer(conf.ca.data(), conf.ca.size()));
    }

    if (!conf.key.empty()) {
        ctx.use_private_key(asio::buffer(conf.key.data(), conf.key.size()),
                            ssl::context::file_format::pem);
    }
}

inline iconnection_sptr create_connection(aio& io, const on_connected_cb& connected_cb,
                                          const on_disconnected_cb& disconnected_cb,
                                          const on_error_cb& error_cb,
                                          optional<ssl_config> ssl_conf) {
    if (ssl_conf.has_value()) {
        auto ssl_ctx = std::make_shared<ssl::context>(ssl::context::tlsv12_client);
        load_certificates(ssl_conf.value(), *ssl_ctx);
        return std::make_shared<connection<ssl_socket>>(io, connected_cb, disconnected_cb, error_cb,
                                                        ssl_ctx);
    } else {
        return std::make_shared<connection<raw_socket>>(io, connected_cb, disconnected_cb,
                                                        error_cb);
    }
}

// Simplified connection factory - creates connection and starts it
inline iconnection_sptr connect(aio& io, string_view address, uint16_t port) {
    // No-op callbacks for simplified usage
    auto noop_connected = [](iconnection&) -> asio::awaitable<void> { co_return; };
    auto noop_disconnected = [](iconnection&) -> asio::awaitable<void> { co_return; };
    auto noop_error = [](iconnection&, string_view) -> asio::awaitable<void> { co_return; };

    auto conn = create_connection(io, noop_connected, noop_disconnected, noop_error, std::nullopt);

    connect_config conf;
    conf.address = std::string(address);
    conf.port = port;
    conn->start(conf);

    return conn;
}

// Simplified connection factory with SSL
inline iconnection_sptr connect(aio& io, string_view address, uint16_t port, ssl_config ssl_conf) {
    auto noop_connected = [](iconnection&) -> asio::awaitable<void> { co_return; };
    auto noop_disconnected = [](iconnection&) -> asio::awaitable<void> { co_return; };
    auto noop_error = [](iconnection&, string_view) -> asio::awaitable<void> { co_return; };

    auto conn = create_connection(io, noop_connected, noop_disconnected, noop_error, ssl_conf);

    connect_config conf;
    conf.address = std::string(address);
    conf.port = port;
    conn->start(conf);

    return conn;
}

} // namespace nats_asio
