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

#include <asio/io_context.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace nats_asio {

using std::optional;
using std::string_view;

using aio = asio::io_context;

// Message headers (name-value pairs)
using headers_t = std::vector<std::pair<std::string, std::string>>;

// Zero-copy headers view (references internal buffer, valid only during callback)
using headers_view_t = std::vector<std::pair<std::string_view, std::string_view>>;

// Complete message with headers support
struct message {
    std::string subject;
    optional<std::string> reply_to;
    headers_t headers;
    std::vector<char> payload;
};

// Zero-copy message view (references internal buffers, valid only during callback)
// WARNING: All data is only valid for the duration of the callback!
struct message_view {
    std::string_view subject;
    optional<std::string_view> reply_to;
    headers_view_t headers;
    std::span<const char> payload;
};

// JetStream publish acknowledgment
struct js_pub_ack {
    std::string stream;
    uint64_t sequence = 0;
    optional<std::string> domain;
    bool duplicate = false;
};

// JetStream batch publish message
struct js_batch_message {
    std::string subject;
    std::vector<char> payload;
    headers_t headers;
};

// Forward declaration - js_batch_result defined after status class

// JetStream message with metadata
struct js_message {
    message msg;                                           // base message with payload/headers
    std::string stream;                                    // stream name
    std::string consumer;                                  // consumer name
    uint64_t stream_sequence = 0;                          // sequence in stream
    uint64_t consumer_sequence = 0;                        // sequence for this consumer
    uint64_t num_delivered = 0;                            // delivery attempt count
    uint64_t num_pending = 0;                              // remaining messages
    std::chrono::system_clock::time_point timestamp;       // message timestamp
};

// JetStream acknowledgment policy
enum class js_ack_policy { none, all, explicit_ };

// JetStream replay policy
enum class js_replay_policy { instant, original };

// JetStream deliver policy
enum class js_deliver_policy {
    all,                  // all messages
    last,                 // last message only
    new_,                 // only new messages
    by_start_sequence,    // from specific sequence
    by_start_time,        // from specific time
    last_per_subject      // last message per subject
};

// Consumer configuration
struct js_consumer_config {
    std::string stream;                                    // required: stream name
    optional<std::string> durable_name;                    // durable consumer name
    optional<std::string> filter_subject;                  // subject filter
    optional<std::string> deliver_subject;                 // for push consumers (auto-generated if empty)
    optional<std::string> deliver_group;                   // queue group for push consumers

    js_ack_policy ack = js_ack_policy::explicit_;
    std::chrono::seconds ack_wait{30};
    uint64_t max_deliver = 0;                              // 0 = unlimited
    uint64_t max_ack_pending = 1000;

    js_replay_policy replay = js_replay_policy::instant;
    js_deliver_policy deliver = js_deliver_policy::all;
    optional<uint64_t> opt_start_seq;
    optional<std::chrono::system_clock::time_point> opt_start_time;

    // Flow control
    bool flow_control = false;
    std::chrono::milliseconds idle_heartbeat{0};           // 0 = disabled
};

// Consumer info returned from server
struct js_consumer_info {
    std::string stream;
    std::string name;
    std::string deliver_subject;
    uint64_t num_pending = 0;
    uint64_t num_ack_pending = 0;
    uint64_t num_redelivered = 0;
    uint64_t delivered_stream_seq = 0;
    uint64_t delivered_consumer_seq = 0;
};

// Key/Value entry returned from get operations
struct kv_entry {
    std::string bucket;                                // bucket name
    std::string key;                                   // key name
    std::vector<char> value;                           // value data
    uint64_t revision = 0;                             // revision number (sequence)
    std::chrono::system_clock::time_point created;     // creation timestamp
    enum class operation { put, del, purge } op = operation::put;
};

// Forward declaration for KV watcher
struct ikv_watcher;
using ikv_watcher_sptr = std::shared_ptr<ikv_watcher>;

// Callback for KV watch events
using on_kv_entry_cb = std::function<asio::awaitable<void>(const kv_entry& entry)>;

// Error codes for type-safe error handling
enum class error_code {
    ok = 0,                    // No error

    // Connection errors (1xx)
    not_connected = 100,
    connection_closed = 101,
    connection_timeout = 102,

    // Protocol/parsing errors (2xx)
    protocol_error = 200,
    parse_error = 201,
    invalid_message = 202,
    invalid_header = 203,

    // Size/limit errors (3xx)
    message_too_large = 300,
    payload_too_large = 301,

    // Timeout errors (4xx)
    timeout = 400,
    request_timeout = 401,
    ack_timeout = 402,

    // Not found errors (5xx)
    not_found = 500,
    key_not_found = 501,
    stream_not_found = 502,
    consumer_not_found = 503,

    // Conflict errors (6xx)
    conflict = 600,
    already_exists = 601,
    revision_mismatch = 602,

    // Validation errors (7xx)
    invalid_argument = 700,
    invalid_subject = 701,
    invalid_key = 702,
    invalid_bucket = 703,

    // Operation errors (8xx)
    operation_failed = 800,
    permission_denied = 801,

    // Unknown/other (9xx)
    unknown = 999
};

// Convert error_code to string
inline const char* error_code_string(error_code code) noexcept {
    switch (code) {
        case error_code::ok: return "ok";
        case error_code::not_connected: return "not connected";
        case error_code::connection_closed: return "connection closed";
        case error_code::connection_timeout: return "connection timeout";
        case error_code::protocol_error: return "protocol error";
        case error_code::parse_error: return "parse error";
        case error_code::invalid_message: return "invalid message";
        case error_code::invalid_header: return "invalid header";
        case error_code::message_too_large: return "message too large";
        case error_code::payload_too_large: return "payload too large";
        case error_code::timeout: return "timeout";
        case error_code::request_timeout: return "request timeout";
        case error_code::ack_timeout: return "ack timeout";
        case error_code::not_found: return "not found";
        case error_code::key_not_found: return "key not found";
        case error_code::stream_not_found: return "stream not found";
        case error_code::consumer_not_found: return "consumer not found";
        case error_code::conflict: return "conflict";
        case error_code::already_exists: return "already exists";
        case error_code::revision_mismatch: return "revision mismatch";
        case error_code::invalid_argument: return "invalid argument";
        case error_code::invalid_subject: return "invalid subject";
        case error_code::invalid_key: return "invalid key";
        case error_code::invalid_bucket: return "invalid bucket";
        case error_code::operation_failed: return "operation failed";
        case error_code::permission_denied: return "permission denied";
        case error_code::unknown: return "unknown error";
    }
    return "unknown error";
}

// Status class for error handling - must be defined before ijs_subscription
class status {
public:
    // Success status
    status() = default;

    // Error with code only
    explicit status(error_code code) : m_code(code) {}

    // Error with code and additional message
    status(error_code code, const std::string& message)
        : m_code(code), m_message(message) {}

    // Legacy: error with string only (maps to unknown error code)
    status(const std::string& error)
        : m_code(error_code::unknown), m_message(error) {}

    ~status() = default;

    [[nodiscard]] bool failed() const noexcept {
        return m_code != error_code::ok;
    }

    [[nodiscard]] bool ok() const noexcept {
        return m_code == error_code::ok;
    }

    [[nodiscard]] error_code code() const noexcept {
        return m_code;
    }

    [[nodiscard]] std::string error() const {
        if (m_code == error_code::ok)
            return {};

        if (m_message.has_value() && !m_message->empty()) {
            return *m_message;
        }
        return error_code_string(m_code);
    }

    // Check for specific error categories
    [[nodiscard]] bool is_connection_error() const noexcept {
        int c = static_cast<int>(m_code);
        return c >= 100 && c < 200;
    }

    [[nodiscard]] bool is_protocol_error() const noexcept {
        int c = static_cast<int>(m_code);
        return c >= 200 && c < 300;
    }

    [[nodiscard]] bool is_timeout() const noexcept {
        int c = static_cast<int>(m_code);
        return c >= 400 && c < 500;
    }

    [[nodiscard]] bool is_not_found() const noexcept {
        int c = static_cast<int>(m_code);
        return c >= 500 && c < 600;
    }

    [[nodiscard]] bool is_conflict() const noexcept {
        int c = static_cast<int>(m_code);
        return c >= 600 && c < 700;
    }

private:
    error_code m_code = error_code::ok;
    optional<std::string> m_message;
};

// JetStream batch publish result (defined after status)
struct js_batch_result {
    std::vector<js_pub_ack> acks;      // Successful acks (in order)
    std::vector<status> errors;         // Errors for failed publishes (in order)
    size_t success_count = 0;
    size_t error_count = 0;
};

// KV watcher interface
struct ikv_watcher {
    virtual ~ikv_watcher() = default;

    // Stop watching
    virtual void stop() noexcept = 0;

    // Check if watcher is active
    [[nodiscard]] virtual bool is_active() const noexcept = 0;

    // Get the bucket being watched
    [[nodiscard]] virtual const std::string& bucket() const noexcept = 0;

    // Get the key filter (empty if watching all keys)
    [[nodiscard]] virtual const std::string& key_filter() const noexcept = 0;
};

// Callback for messages without headers (legacy)
using on_message_cb = std::function<asio::awaitable<void>(std::string_view subject,
                                                          std::optional<std::string_view> reply_to,
                                                          std::span<const char> payload)>;

// Callback for messages with headers (copies data into message struct)
using on_message_with_headers_cb = std::function<asio::awaitable<void>(const message& msg)>;

// Zero-copy callback for messages with headers (references internal buffers)
// WARNING: All data in message_view is only valid during callback execution!
// Do not store references or pointers to the data - copy if you need to keep it.
using on_message_zero_copy_cb = std::function<asio::awaitable<void>(const message_view& msg)>;

// Forward declaration
struct ijs_subscription;
using ijs_subscription_sptr = std::shared_ptr<ijs_subscription>;

// Callback for JetStream messages
using on_js_message_cb = std::function<asio::awaitable<void>(ijs_subscription& sub, const js_message& msg)>;

// JetStream subscription interface
struct ijs_subscription {
    virtual ~ijs_subscription() = default;

    [[nodiscard]] virtual const js_consumer_info& info() const noexcept = 0;

    virtual void stop() noexcept = 0;

    [[nodiscard]] virtual bool is_active() const noexcept = 0;

    // Acknowledge message (mark as processed)
    [[nodiscard]] virtual asio::awaitable<status> ack(const js_message& msg) = 0;

    // Batch acknowledge multiple messages (more efficient than individual acks)
    [[nodiscard]] virtual asio::awaitable<status> ack_batch(
        const std::vector<js_message>& messages) = 0;

    // Negative acknowledge (request redelivery)
    [[nodiscard]] virtual asio::awaitable<status> nak(const js_message& msg,
                                                       std::chrono::milliseconds delay = {}) = 0;

    // Mark as in-progress (extend ack deadline)
    [[nodiscard]] virtual asio::awaitable<status> in_progress(const js_message& msg) = 0;

    // Terminate processing (don't redeliver)
    [[nodiscard]] virtual asio::awaitable<status> term(const js_message& msg) = 0;
};

struct subscribe_options {
    optional<std::string_view> queue_group{};
    uint32_t max_messages = 0;  // 0 = unlimited, auto-unsubscribe after N messages
};

struct isubscription {
    virtual ~isubscription() = default;

    [[nodiscard]] virtual uint64_t sid() noexcept = 0;

    virtual void cancel() noexcept = 0;

    [[nodiscard]] virtual uint32_t max_messages() const noexcept = 0;

    [[nodiscard]] virtual uint32_t message_count() const noexcept = 0;
};
using isubscription_sptr = std::shared_ptr<isubscription>;

struct ssl_config {
    std::string key;      // PEM-encoded private key content
    std::string cert;     // PEM-encoded certificate content
    std::string ca;       // PEM-encoded CA certificate content
    bool required = false;
    bool verify = true;
};

// Connection statistics for monitoring and diagnostics
struct connection_stats {
    // Message counts
    std::atomic<uint64_t> msgs_sent{0};
    std::atomic<uint64_t> msgs_received{0};

    // Byte counts
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> bytes_received{0};

    // Connection state
    std::atomic<uint32_t> reconnect_count{0};
    std::chrono::steady_clock::time_point connected_at{};

    // Ping/pong statistics
    std::atomic<uint64_t> pings_sent{0};
    std::atomic<uint64_t> pongs_received{0};

    // Publish retry statistics
    std::atomic<uint64_t> publish_retries{0};
    std::atomic<uint64_t> publish_retry_failures{0};

    // Offline queue statistics
    std::atomic<uint64_t> offline_queued{0};
    std::atomic<uint64_t> offline_drained{0};
    std::atomic<uint64_t> offline_dropped{0};

    // Latency tracking (in microseconds) - uses simple histogram buckets
    // Buckets: <100us, <500us, <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, >=500ms
    static constexpr size_t LATENCY_BUCKETS = 9;
    std::atomic<uint64_t> latency_histogram[LATENCY_BUCKETS]{};
    std::atomic<uint64_t> latency_count{0};
    std::atomic<uint64_t> latency_sum_us{0};  // Sum for calculating average

    // Record a latency sample in microseconds
    void record_latency(uint64_t latency_us) noexcept {
        latency_count.fetch_add(1, std::memory_order_relaxed);
        latency_sum_us.fetch_add(latency_us, std::memory_order_relaxed);

        size_t bucket;
        if (latency_us < 100)        bucket = 0;
        else if (latency_us < 500)   bucket = 1;
        else if (latency_us < 1000)  bucket = 2;
        else if (latency_us < 5000)  bucket = 3;
        else if (latency_us < 10000) bucket = 4;
        else if (latency_us < 50000) bucket = 5;
        else if (latency_us < 100000) bucket = 6;
        else if (latency_us < 500000) bucket = 7;
        else                          bucket = 8;

        latency_histogram[bucket].fetch_add(1, std::memory_order_relaxed);
    }

    // Get approximate percentile latency in microseconds
    // Uses linear interpolation within buckets
    uint64_t latency_percentile(double percentile) const noexcept {
        uint64_t total = latency_count.load(std::memory_order_relaxed);
        if (total == 0) return 0;

        uint64_t target = static_cast<uint64_t>(total * percentile / 100.0);
        uint64_t cumulative = 0;

        static constexpr uint64_t bucket_limits[] = {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000};
        for (size_t i = 0; i < LATENCY_BUCKETS; ++i) {
            uint64_t count = latency_histogram[i].load(std::memory_order_relaxed);
            if (cumulative + count >= target) {
                // Return approximate value at mid-point of bucket
                return (i == 0) ? bucket_limits[0] / 2 : (bucket_limits[i-1] + bucket_limits[i]) / 2;
            }
            cumulative += count;
        }
        return bucket_limits[LATENCY_BUCKETS - 1];
    }

    // Convenience methods for common percentiles
    uint64_t latency_p50() const noexcept { return latency_percentile(50); }
    uint64_t latency_p95() const noexcept { return latency_percentile(95); }
    uint64_t latency_p99() const noexcept { return latency_percentile(99); }
    uint64_t latency_avg() const noexcept {
        uint64_t count = latency_count.load(std::memory_order_relaxed);
        return count > 0 ? latency_sum_us.load(std::memory_order_relaxed) / count : 0;
    }

    // Reset all counters
    void reset() noexcept {
        msgs_sent.store(0, std::memory_order_relaxed);
        msgs_received.store(0, std::memory_order_relaxed);
        bytes_sent.store(0, std::memory_order_relaxed);
        bytes_received.store(0, std::memory_order_relaxed);
        reconnect_count.store(0, std::memory_order_relaxed);
        pings_sent.store(0, std::memory_order_relaxed);
        pongs_received.store(0, std::memory_order_relaxed);
        publish_retries.store(0, std::memory_order_relaxed);
        publish_retry_failures.store(0, std::memory_order_relaxed);
        offline_queued.store(0, std::memory_order_relaxed);
        offline_drained.store(0, std::memory_order_relaxed);
        offline_dropped.store(0, std::memory_order_relaxed);
        latency_count.store(0, std::memory_order_relaxed);
        latency_sum_us.store(0, std::memory_order_relaxed);
        for (size_t i = 0; i < LATENCY_BUCKETS; ++i) {
            latency_histogram[i].store(0, std::memory_order_relaxed);
        }
    }

    // Get uptime since connected
    std::chrono::milliseconds uptime() const noexcept {
        if (connected_at == std::chrono::steady_clock::time_point{}) {
            return std::chrono::milliseconds{0};
        }
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - connected_at);
    }
};

struct connect_config {
    std::string address;
    uint16_t port;

    bool verbose = false;
    bool pedantic = false;

    optional<std::string> user;
    optional<std::string> password;
    optional<std::string> token;

    // Exponential backoff configuration for reconnection
    uint32_t retry_initial_delay_ms = 1000;  // Initial delay in milliseconds
    uint32_t retry_max_delay_ms = 30000;     // Maximum delay cap in milliseconds
    uint32_t retry_max_attempts = 0;         // 0 = unlimited retries
    float retry_jitter_factor = 0.5f;        // Jitter factor (0.0-1.0), adds ±jitter% randomness

    // Socket buffer tuning (0 = use system defaults)
    uint32_t send_buffer_size = 0;     // SO_SNDBUF - send buffer size in bytes
    uint32_t recv_buffer_size = 0;     // SO_RCVBUF - receive buffer size in bytes

    // Ping/pong keep-alive configuration
    uint32_t ping_interval_ms = 30000;   // Ping interval in milliseconds (0 = disabled)
    uint32_t ping_timeout_ms = 10000;    // Pong timeout before connection is considered dead
    uint32_t max_pings_outstanding = 2;  // Max unanswered pings before disconnect

    // Circuit breaker configuration (0 = disabled)
    uint32_t circuit_breaker_threshold = 5;      // Errors before circuit opens
    uint32_t circuit_breaker_timeout_ms = 30000; // Time before half-open state
    uint32_t circuit_breaker_half_open_max = 3;  // Max requests in half-open state

    // Compression configuration
    uint32_t compression_threshold = 0;  // Min payload size for compression (0 = disabled)
    int compression_level = 3;           // zstd compression level (1-22, 3 = default)

    // Inbox pool size for request-reply pattern (0 = generate new each time)
    uint32_t inbox_pool_size = 64;

    // Publish retry configuration (0 = disabled)
    uint32_t publish_retry_max = 3;              // Max retry attempts (0 = no retry)
    uint32_t publish_retry_initial_ms = 100;     // Initial retry delay in ms
    uint32_t publish_retry_max_ms = 5000;        // Max retry delay in ms
    float publish_retry_multiplier = 2.0f;       // Backoff multiplier

    // Offline queue configuration (0 = disabled)
    uint32_t offline_queue_max_size = 10000;     // Max messages to queue when disconnected
    uint32_t offline_queue_max_bytes = 10485760; // Max bytes to queue (10MB default)

    // Backpressure configuration
    uint32_t backpressure_high_watermark = 1048576;  // High watermark in bytes (1MB) - start applying backpressure
    uint32_t backpressure_low_watermark = 262144;    // Low watermark in bytes (256KB) - resume normal operation
    uint32_t backpressure_max_wait_ms = 5000;        // Max time to wait for buffer to drain (0 = fail immediately)
};

// Circuit breaker state for monitoring
enum class circuit_state { closed, open, half_open };

struct circuit_breaker_stats {
    std::atomic<circuit_state> state{circuit_state::closed};
    std::atomic<uint32_t> failure_count{0};
    std::atomic<uint32_t> success_count{0};
    std::atomic<uint64_t> rejected_count{0};
    std::chrono::steady_clock::time_point last_failure_time{};
    std::chrono::steady_clock::time_point opened_at{};

    void reset() noexcept {
        state.store(circuit_state::closed, std::memory_order_relaxed);
        failure_count.store(0, std::memory_order_relaxed);
        success_count.store(0, std::memory_order_relaxed);
        rejected_count.store(0, std::memory_order_relaxed);
    }
};

struct iconnection {
    virtual ~iconnection() = default;

    virtual void start(const connect_config& conf) = 0;

    virtual void stop() noexcept = 0;

    [[nodiscard]] virtual bool is_connected() noexcept = 0;

    // Get connection statistics
    [[nodiscard]] virtual const connection_stats& stats() const noexcept = 0;

    // Graceful drain - stops accepting new operations, flushes pending writes,
    // waits for outstanding acks, then closes connection
    // Returns when drain is complete or timeout expires
    [[nodiscard]] virtual asio::awaitable<status> drain(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(30000)) = 0;

    // Check if connection is draining
    [[nodiscard]] virtual bool is_draining() const noexcept = 0;

    // Circuit breaker state
    [[nodiscard]] virtual const circuit_breaker_stats& circuit_breaker() const noexcept = 0;

    // Manually reset the circuit breaker to closed state
    virtual void reset_circuit_breaker() noexcept = 0;

    // Basic publish (no headers)
    [[nodiscard]] virtual asio::awaitable<status> publish(string_view subject, std::span<const char> payload,
                                                          optional<string_view> reply_to) = 0;

    // Publish with headers (HPUB)
    [[nodiscard]] virtual asio::awaitable<status> publish(string_view subject, std::span<const char> payload,
                                                          const headers_t& headers,
                                                          optional<string_view> reply_to = {}) = 0;

    // Write pre-formatted NATS protocol data directly (for high-performance batched publishing)
    // The data must be valid NATS protocol (e.g., multiple PUB commands)
    [[nodiscard]] virtual asio::awaitable<status> write_raw(std::span<const char> data) = 0;

    // Write multiple buffers using scatter-gather I/O (writev)
    // More efficient than concatenating buffers - avoids copying
    [[nodiscard]] virtual asio::awaitable<status> write_raw_iov(
        std::span<const std::span<const char>> buffers) = 0;

    // =========================================================================
    // Write Coalescing - Queue publishes for batched sending (reduces syscalls)
    // =========================================================================

    // Queue a publish for batched sending (fire-and-forget, no await needed)
    // Messages are buffered and flushed automatically or via flush()
    // Returns immediately - does not wait for network write
    virtual status publish_queued(string_view subject, std::span<const char> payload,
                                  optional<string_view> reply_to = {}) = 0;

    // Flush the write queue - sends all queued messages in a single write
    // Call this after a batch of publish_queued calls to ensure delivery
    [[nodiscard]] virtual asio::awaitable<status> flush() = 0;

    // Configure write coalescing behavior
    // flush_interval: auto-flush interval (0 = disabled, default 1ms)
    // max_pending: max queued bytes before auto-flush (default 64KB)
    virtual void set_write_coalescing(std::chrono::microseconds flush_interval,
                                      size_t max_pending_bytes) = 0;

    // Get number of bytes currently queued for writing
    [[nodiscard]] virtual size_t pending_bytes() const noexcept = 0;

    // =========================================================================
    // Backpressure - Wait for write buffer to drain before publishing
    // =========================================================================

    // Check if backpressure is currently active (pending bytes > high watermark)
    [[nodiscard]] virtual bool is_backpressure_active() const noexcept = 0;

    // Wait until pending bytes drop below low watermark or timeout expires
    // Returns ok if buffer drained, timeout error if max_wait exceeded
    [[nodiscard]] virtual asio::awaitable<status> wait_for_drain(
        std::chrono::milliseconds max_wait = std::chrono::milliseconds(5000)) = 0;

    // Publish with backpressure - waits for buffer space before publishing
    // If buffer is above high watermark, waits until it drops below low watermark
    [[nodiscard]] virtual asio::awaitable<status> publish_with_backpressure(
        string_view subject, std::span<const char> payload,
        optional<string_view> reply_to = {},
        std::chrono::milliseconds max_wait = std::chrono::milliseconds(5000)) = 0;

    // Request-reply pattern with timeout
    [[nodiscard]] virtual asio::awaitable<std::pair<message, status>> request(
        string_view subject, std::span<const char> payload,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Request-reply with headers
    [[nodiscard]] virtual asio::awaitable<std::pair<message, status>> request(
        string_view subject, std::span<const char> payload, const headers_t& headers,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // JetStream publish with acknowledgment
    // Set wait_for_ack=false for fire-and-forget (returns immediately, no ack)
    [[nodiscard]] virtual asio::awaitable<std::pair<js_pub_ack, status>> js_publish(
        string_view subject, std::span<const char> payload,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000),
        bool wait_for_ack = true) = 0;

    // JetStream publish with headers
    // Set wait_for_ack=false for fire-and-forget (returns immediately, no ack)
    [[nodiscard]] virtual asio::awaitable<std::pair<js_pub_ack, status>> js_publish(
        string_view subject, std::span<const char> payload, const headers_t& headers,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000),
        bool wait_for_ack = true) = 0;

    // JetStream fire-and-forget publish (no ack, no timeout)
    [[nodiscard]] virtual asio::awaitable<status> js_publish_async(
        string_view subject, std::span<const char> payload) = 0;

    // JetStream fire-and-forget publish with headers (no ack, no timeout)
    [[nodiscard]] virtual asio::awaitable<status> js_publish_async(
        string_view subject, std::span<const char> payload, const headers_t& headers) = 0;

    // JetStream batch publish - publishes multiple messages and collects all acks
    // More efficient than individual publishes for high-throughput scenarios
    // Messages are sent in parallel and acks are collected concurrently
    [[nodiscard]] virtual asio::awaitable<js_batch_result> js_publish_batch(
        const std::vector<js_batch_message>& messages,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // JetStream subscribe (push consumer) - creates/binds consumer and subscribes to delivery subject
    [[nodiscard]] virtual asio::awaitable<std::pair<ijs_subscription_sptr, status>>
    js_subscribe(const js_consumer_config& config, on_js_message_cb cb) = 0;

    // JetStream fetch (pull consumer) - fetch batch of messages on demand
    [[nodiscard]] virtual asio::awaitable<std::pair<std::vector<js_message>, status>>
    js_fetch(string_view stream, string_view consumer, uint32_t batch = 1,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Get consumer info
    [[nodiscard]] virtual asio::awaitable<std::pair<js_consumer_info, status>>
    js_get_consumer_info(string_view stream, string_view consumer) = 0;

    // Delete a consumer
    [[nodiscard]] virtual asio::awaitable<status>
    js_delete_consumer(string_view stream, string_view consumer) = 0;

    // Key/Value store operations

    // Put a value into KV bucket (returns revision number)
    [[nodiscard]] virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_put(string_view bucket, string_view key, std::span<const char> value,
           std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Get a value from KV bucket
    [[nodiscard]] virtual asio::awaitable<std::pair<kv_entry, status>>
    kv_get(string_view bucket, string_view key,
           std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Delete a key from KV bucket (returns revision number of delete marker)
    [[nodiscard]] virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_delete(string_view bucket, string_view key,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Purge a key from KV bucket (clears all history, then creates purge marker)
    // Unlike delete, purge removes all previous revisions of the key
    [[nodiscard]] virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_purge(string_view bucket, string_view key,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Create a key only if it doesn't exist or was deleted (returns revision number)
    // Fails if the key already exists with a value
    [[nodiscard]] virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_create(string_view bucket, string_view key, std::span<const char> value,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Update a key only if the current revision matches (optimistic concurrency)
    // Returns new revision number on success
    [[nodiscard]] virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_update(string_view bucket, string_view key, std::span<const char> value,
              uint64_t revision,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // List all keys in a KV bucket
    // Returns vector of key names (without the $KV.bucket. prefix)
    [[nodiscard]] virtual asio::awaitable<std::pair<std::vector<std::string>, status>>
    kv_keys(string_view bucket,
            std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Get full history for a key (all revisions)
    // Returns vector of kv_entry in chronological order (oldest first)
    [[nodiscard]] virtual asio::awaitable<std::pair<std::vector<kv_entry>, status>>
    kv_history(string_view bucket, string_view key,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Revert a key to a previous revision (gets value at revision, puts it as new value)
    // Returns new revision number on success
    [[nodiscard]] virtual asio::awaitable<std::pair<uint64_t, status>>
    kv_revert(string_view bucket, string_view key, uint64_t revision,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) = 0;

    // Watch a KV bucket for changes (optionally filter by key)
    // If key is empty, watches all keys in the bucket
    [[nodiscard]] virtual asio::awaitable<std::pair<ikv_watcher_sptr, status>>
    kv_watch(string_view bucket, on_kv_entry_cb cb, string_view key = {}) = 0;

    [[nodiscard]] virtual asio::awaitable<status> unsubscribe(const isubscription_sptr& p) = 0;

    [[nodiscard]] virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_cb cb, subscribe_options opts = {}) = 0;

    // Subscribe with headers support (for JetStream messages via HMSG)
    [[nodiscard]] virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_with_headers_cb cb, subscribe_options opts = {}) = 0;

    // Zero-copy subscribe - callback receives references to internal buffers
    // WARNING: Data is only valid during callback! Copy if you need to keep it.
    [[nodiscard]] virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_zero_copy_cb cb, subscribe_options opts = {}) = 0;
};
using iconnection_sptr = std::shared_ptr<iconnection>;

using on_connected_cb = std::function<asio::awaitable<void>(iconnection&)>;
using on_disconnected_cb = std::function<asio::awaitable<void>(iconnection&)>;
using on_error_cb = std::function<asio::awaitable<void>(iconnection&, string_view)>;

[[nodiscard]] iconnection_sptr create_connection(aio& io, const on_connected_cb& connected_cb,
                                   const on_disconnected_cb& disconnected_cb,
                                   const on_error_cb& error_cb, optional<ssl_config> ssl_conf);

// Simplified connection factory - creates connection and starts it
[[nodiscard]] iconnection_sptr connect(aio& io, string_view address, uint16_t port = 4222);

// Simplified connection factory with SSL
[[nodiscard]] iconnection_sptr connect(aio& io, string_view address, uint16_t port, ssl_config ssl_conf);

} // namespace nats_asio
