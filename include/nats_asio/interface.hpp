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

    // Socket buffer tuning (0 = use system defaults)
    uint32_t send_buffer_size = 0;     // SO_SNDBUF - send buffer size in bytes
    uint32_t recv_buffer_size = 0;     // SO_RCVBUF - receive buffer size in bytes
};

struct iconnection {
    virtual ~iconnection() = default;

    virtual void start(const connect_config& conf) = 0;

    virtual void stop() noexcept = 0;

    [[nodiscard]] virtual bool is_connected() noexcept = 0;

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
