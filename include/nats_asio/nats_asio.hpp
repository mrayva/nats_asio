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
#include <atomic>
#include <concepts>
#include <iomanip>
#include <istream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
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

// Parse headers from NATS format
inline headers_t parse_headers(string_view data) {
    headers_t headers;

    // Skip "NATS/1.0\r\n" prefix
    auto pos = data.find("\r\n");
    if (pos == string_view::npos) return headers;
    data = data.substr(pos + 2);

    // Parse each header line until empty line
    while (!data.empty() && data != "\r\n") {
        pos = data.find("\r\n");
        if (pos == string_view::npos) break;

        auto line = data.substr(0, pos);
        data = data.substr(pos + 2);

        if (line.empty()) break;  // Empty line = end of headers

        auto colon = line.find(':');
        if (colon != string_view::npos) {
            auto key = line.substr(0, colon);
            auto value = line.substr(colon + 1);
            // Trim leading space from value
            if (!value.empty() && value[0] == ' ') {
                value = value.substr(1);
            }
            headers.emplace_back(std::string(key), std::string(value));
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
            co_return status("can't get line");
        }

        if (header.size() < 3) {
            co_return status("too small header");
        }

        if (header.back() != '\r') {
            co_return status("unexpected len of server message");
        }

        header.pop_back();
        auto v = string_view(header);

        if (v.empty()) {
            co_return status("protocol violation from server");
        }

        switch (v[0]) {
            case 'H': // HMSG
                if (v.starts_with(protocol::op_hmsg) && v.size() > protocol::op_hmsg.size() && v[protocol::op_hmsg.size()] == ' ') {
                    auto info = v.substr(protocol::op_hmsg.size() + 1);
                    auto results = split_sv(info, protocol::delim);

                    // HMSG subject sid [reply] hdr_len total_len
                    if (results.size() < 4 || results.size() > 5) {
                        co_return status("unexpected HMSG format");
                    }

                    bool has_reply = results.size() == 5;
                    std::size_t hdr_idx = has_reply ? 3 : 2;
                    std::size_t total_idx = has_reply ? 4 : 3;
                    std::size_t hdr_len = 0, total_len = 0;

                    try {
                        hdr_len = static_cast<std::size_t>(std::stoll(results[hdr_idx].data(), nullptr, 10));
                        total_len = static_cast<std::size_t>(std::stoll(results[total_idx].data(), nullptr, 10));
                    } catch (const std::exception& e) {
                        co_return status(fmt::format("can't parse HMSG lengths: {}", e.what()));
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
                        co_return status("unexpected message format");
                    }

                    bool reply_to = results.size() == 4;
                    std::size_t bytes_id = reply_to ? 3 : 2;
                    std::size_t bytes_n = 0;

                    try {
                        bytes_n = static_cast<std::size_t>(
                            std::stoll(results[bytes_id].data(), nullptr, 10));
                    } catch (const std::exception& e) {
                        co_return status(fmt::format("can't parse int in headers: {}", e.what()));
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

        co_return status("unknown message");
    }

    static std::vector<string_view> split_sv(string_view str, string_view delims = " ") {
        std::vector<string_view> output;
        output.reserve(4); // Typical NATS headers have 3-4 tokens

        for (auto first = str.data(), second = str.data(), last = first + str.size();
             second != last && first != last; first = second + 1) {
            second = std::find_first_of(first, last, std::cbegin(delims), std::cend(delims));

            if (first != second) {
                output.emplace_back(first, second - first);
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

// Helper to parse KV entry from a message
inline kv_entry parse_kv_entry_from_message(const message& msg, string_view bucket) {
    kv_entry entry;
    entry.bucket = std::string(bucket);
    entry.value.assign(msg.payload.begin(), msg.payload.end());
    entry.op = kv_entry::operation::put;

    // Extract key from subject: $KV.{bucket}.{key}
    // Subject format: $KV.bucket.key or $KV.bucket.key.with.dots
    std::string prefix = fmt::format("$KV.{}.", bucket);
    if (msg.subject.size() > prefix.size() && msg.subject.substr(0, prefix.size()) == prefix) {
        entry.key = msg.subject.substr(prefix.size());
    }

    // Parse metadata from headers
    for (const auto& [key, value] : msg.headers) {
        if (key == "Nats-Sequence") {
            entry.revision = std::stoull(value);
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
                js_msg.stream_sequence = std::stoull(value.substr(0, space_pos));
                js_msg.consumer_sequence = std::stoull(value.substr(space_pos + 1));
            }
        } else if (key == "Nats-Num-Delivered") {
            js_msg.num_delivered = std::stoull(value);
        } else if (key == "Nats-Num-Pending") {
            js_msg.num_pending = std::stoull(value);
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

// Helper to convert ack_policy enum to string
inline std::string js_ack_policy_to_string(js_ack_policy policy) {
    switch (policy) {
        case js_ack_policy::none: return "none";
        case js_ack_policy::all: return "all";
        case js_ack_policy::explicit_: return "explicit";
    }
    return "explicit";
}

// Helper to convert replay_policy enum to string
inline std::string js_replay_policy_to_string(js_replay_policy policy) {
    switch (policy) {
        case js_replay_policy::instant: return "instant";
        case js_replay_policy::original: return "original";
    }
    return "instant";
}

// Helper to convert deliver_policy enum to string
inline std::string js_deliver_policy_to_string(js_deliver_policy policy) {
    switch (policy) {
        case js_deliver_policy::all: return "all";
        case js_deliver_policy::last: return "last";
        case js_deliver_policy::new_: return "new";
        case js_deliver_policy::by_start_sequence: return "by_start_sequence";
        case js_deliver_policy::by_start_time: return "by_start_time";
        case js_deliver_policy::last_per_subject: return "last_per_subject";
    }
    return "all";
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
            co_return status("not connected");
        }

        if (m_max_payload > 0 && payload.size() > m_max_payload) {
            co_return status("message size exceeds server limit");
        }

        std::vector<asio::const_buffer> buffers;
        std::string header;

        if (reply_to.has_value()) {
            header = fmt::format(fmt::runtime(protocol::pub_fmt), subject, reply_to.value(), payload.size());
        } else {
            header = fmt::format(fmt::runtime(protocol::pub_fmt), subject, "", payload.size());
        }

        buffers.emplace_back(asio::buffer(header.data(), header.size()));
        buffers.emplace_back(asio::buffer(payload.data(), payload.size()));
        buffers.emplace_back(asio::buffer(protocol::crlf.data(), protocol::crlf.size()));

        try {
            co_await asio::async_write(m_socket, buffers, asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return status(e.code().message());
        }

        co_return status();
    }

    virtual asio::awaitable<status> unsubscribe(const isubscription_sptr& p) override {
        co_return co_await unsubscribe_impl(p);
    }

    virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, on_message_cb cb, subscribe_options opts = {}) override {
        if (!m_is_connected) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status("not connected")};
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
                                                            status("not connected")};
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
            co_return status("not connected");
        }

        auto hdr_data = serialize_headers(headers);
        std::size_t hdr_len = hdr_data.size();
        std::size_t total_len = hdr_len + payload.size();

        if (m_max_payload > 0 && total_len > m_max_payload) {
            co_return status("message size exceeds server limit");
        }

        std::vector<asio::const_buffer> buffers;
        std::string cmd;

        if (reply_to.has_value()) {
            cmd = fmt::format(fmt::runtime(protocol::hpub_fmt), subject, reply_to.value(), hdr_len, total_len);
        } else {
            cmd = fmt::format(fmt::runtime(protocol::hpub_no_reply_fmt), subject, hdr_len, total_len);
        }

        buffers.emplace_back(asio::buffer(cmd.data(), cmd.size()));
        buffers.emplace_back(asio::buffer(hdr_data.data(), hdr_data.size()));
        buffers.emplace_back(asio::buffer(payload.data(), payload.size()));
        buffers.emplace_back(asio::buffer(protocol::crlf.data(), protocol::crlf.size()));

        try {
            co_await asio::async_write(m_socket, buffers, asio::use_awaitable);
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
        std::chrono::milliseconds timeout) override {
        co_return co_await js_publish_impl(subject, payload, {}, timeout);
    }

    // JetStream publish with headers
    virtual asio::awaitable<std::pair<js_pub_ack, status>> js_publish(
        string_view subject, std::span<const char> payload, const headers_t& headers,
        std::chrono::milliseconds timeout) override {
        co_return co_await js_publish_impl(subject, payload, headers, timeout);
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
            co_return std::pair<message, status>{{}, status("not connected")};
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
                co_return std::pair<message, status>{{}, status("request timeout")};
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
        const headers_t& headers, std::chrono::milliseconds timeout) {

        // Use request-reply pattern
        auto [response, req_status] = co_await request_impl(subject, payload, headers, timeout);

        if (req_status.failed()) {
            co_return std::pair<js_pub_ack, status>{{}, req_status};
        }

        // Parse JetStream pub ack from response
        js_pub_ack ack;
        try {
            using nlohmann::json;
            std::string payload_str(response.payload.begin(), response.payload.end());
            auto j = json::parse(payload_str);

            if (j.contains("error")) {
                auto err_msg = j["error"].value("description", "unknown JetStream error");
                co_return std::pair<js_pub_ack, status>{{}, status(err_msg)};
            }

            if (j.contains("stream")) {
                ack.stream = j["stream"].get<std::string>();
            }
            if (j.contains("seq")) {
                ack.sequence = j["seq"].get<uint64_t>();
            }
            if (j.contains("domain")) {
                ack.domain = j["domain"].get<std::string>();
            }
            if (j.contains("duplicate")) {
                ack.duplicate = j["duplicate"].get<bool>();
            }
        } catch (const std::exception& e) {
            co_return std::pair<js_pub_ack, status>{{}, status(fmt::format("failed to parse pub ack: {}", e.what()))};
        }

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
            api_subject = fmt::format("$JS.API.CONSUMER.DURABLE.CREATE.{}.{}",
                                      config.stream, *config.durable_name);
        } else {
            api_subject = fmt::format("$JS.API.CONSUMER.CREATE.{}", config.stream);
        }

        auto [response, req_status] = co_await request_impl(api_subject, payload, {},
                                                            std::chrono::seconds(5));

        if (req_status.failed()) {
            co_return std::pair<nats_asio::js_consumer_info, status>{{}, req_status};
        }

        // Parse response
        nats_asio::js_consumer_info info;
        try {
            std::string resp_str(response.payload.begin(), response.payload.end());
            auto j = json::parse(resp_str);

            if (j.contains("error")) {
                auto err_msg = j["error"].value("description", "unknown JetStream error");
                auto err_code = j["error"].value("err_code", 0);
                co_return std::pair<nats_asio::js_consumer_info, status>{
                    {}, status(fmt::format("{} (code: {})", err_msg, err_code))};
            }

            info.stream = j.value("stream_name", config.stream);
            info.name = j.value("name", config.durable_name.value_or(""));
            if (j.contains("config")) {
                info.deliver_subject = j["config"].value("deliver_subject", deliver_subject);
            }
            if (j.contains("delivered")) {
                info.delivered_stream_seq = j["delivered"].value("stream_seq", 0);
                info.delivered_consumer_seq = j["delivered"].value("consumer_seq", 0);
            }
            if (j.contains("num_pending")) {
                info.num_pending = j["num_pending"].get<uint64_t>();
            }
            if (j.contains("num_ack_pending")) {
                info.num_ack_pending = j["num_ack_pending"].get<uint64_t>();
            }
            if (j.contains("num_redelivered")) {
                info.num_redelivered = j["num_redelivered"].get<uint64_t>();
            }
        } catch (const std::exception& e) {
            co_return std::pair<nats_asio::js_consumer_info, status>{
                {}, status(fmt::format("failed to parse consumer info: {}", e.what()))};
        }

        co_return std::pair<nats_asio::js_consumer_info, status>{std::move(info), status()};
    }

    // JetStream subscribe implementation (push consumer)
    asio::awaitable<std::pair<ijs_subscription_sptr, status>> js_subscribe_impl(
        const js_consumer_config& config, on_js_message_cb cb) {

        if (!m_is_connected) {
            co_return std::pair<ijs_subscription_sptr, status>{nullptr, status("not connected")};
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
            co_return std::pair<std::vector<js_message>, status>{{}, status("not connected")};
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
        std::string api_subject = fmt::format("$JS.API.CONSUMER.MSG.NEXT.{}.{}", stream, consumer);
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
            co_return std::pair<nats_asio::js_consumer_info, status>{{}, status("not connected")};
        }

        using nlohmann::json;

        std::string api_subject = fmt::format("$JS.API.CONSUMER.INFO.{}.{}", stream, consumer);
        auto [response, req_status] = co_await request_impl(api_subject, {}, {}, std::chrono::seconds(5));

        if (req_status.failed()) {
            co_return std::pair<nats_asio::js_consumer_info, status>{{}, req_status};
        }

        nats_asio::js_consumer_info info;
        try {
            std::string resp_str(response.payload.begin(), response.payload.end());
            auto j = json::parse(resp_str);

            if (j.contains("error")) {
                auto err_msg = j["error"].value("description", "unknown JetStream error");
                co_return std::pair<nats_asio::js_consumer_info, status>{{}, status(err_msg)};
            }

            info.stream = j.value("stream_name", std::string(stream));
            info.name = j.value("name", std::string(consumer));
            if (j.contains("config")) {
                info.deliver_subject = j["config"].value("deliver_subject", "");
            }
            if (j.contains("delivered")) {
                info.delivered_stream_seq = j["delivered"].value("stream_seq", 0);
                info.delivered_consumer_seq = j["delivered"].value("consumer_seq", 0);
            }
            info.num_pending = j.value("num_pending", 0);
            info.num_ack_pending = j.value("num_ack_pending", 0);
            info.num_redelivered = j.value("num_redelivered", 0);
        } catch (const std::exception& e) {
            co_return std::pair<nats_asio::js_consumer_info, status>{
                {}, status(fmt::format("failed to parse consumer info: {}", e.what()))};
        }

        co_return std::pair<nats_asio::js_consumer_info, status>{std::move(info), status()};
    }

    // Delete consumer implementation
    asio::awaitable<status> js_delete_consumer_impl(string_view stream, string_view consumer) {
        if (!m_is_connected) {
            co_return status("not connected");
        }

        using nlohmann::json;

        std::string api_subject = fmt::format("$JS.API.CONSUMER.DELETE.{}.{}", stream, consumer);
        auto [response, req_status] = co_await request_impl(api_subject, {}, {}, std::chrono::seconds(5));

        if (req_status.failed()) {
            co_return req_status;
        }

        try {
            std::string resp_str(response.payload.begin(), response.payload.end());
            auto j = json::parse(resp_str);

            if (j.contains("error")) {
                auto err_msg = j["error"].value("description", "unknown JetStream error");
                co_return status(err_msg);
            }

            if (!j.value("success", false)) {
                co_return status("consumer deletion failed");
            }
        } catch (const std::exception& e) {
            co_return status(fmt::format("failed to parse delete response: {}", e.what()));
        }

        co_return status();
    }

    // KV put implementation
    asio::awaitable<std::pair<uint64_t, status>> kv_put_impl(
        string_view bucket, string_view key, std::span<const char> value,
        std::chrono::milliseconds timeout) {

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status("not connected")};
        }

        // KV subject format: $KV.{bucket}.{key}
        std::string subject = fmt::format("$KV.{}.{}", bucket, key);

        // Use JetStream publish to get acknowledgment with sequence number
        auto [ack, pub_status] = co_await js_publish_impl(subject, value, {}, timeout);

        if (pub_status.failed()) {
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV get implementation
    asio::awaitable<std::pair<kv_entry, status>> kv_get_impl(
        string_view bucket, string_view key,
        std::chrono::milliseconds timeout) {

        if (!m_is_connected) {
            co_return std::pair<kv_entry, status>{{}, status("not connected")};
        }

        using nlohmann::json;

        // Stream name for KV bucket: KV_{bucket}
        std::string stream_name = fmt::format("KV_{}", bucket);

        // Use direct get API: $JS.API.DIRECT.GET.{stream}
        std::string api_subject = fmt::format("$JS.API.DIRECT.GET.{}", stream_name);

        // Request body with the subject (key)
        std::string kv_subject = fmt::format("$KV.{}.{}", bucket, key);
        json req;
        req["last_by_subj"] = kv_subject;

        auto payload_str = req.dump();
        std::span<const char> payload(payload_str.data(), payload_str.size());

        auto [response, req_status] = co_await request_impl(api_subject, payload, {}, timeout);

        if (req_status.failed()) {
            co_return std::pair<kv_entry, status>{{}, req_status};
        }

        // Check for error status in headers
        for (const auto& [hdr_key, hdr_val] : response.headers) {
            if (hdr_key == "Status") {
                if (hdr_val == "404") {
                    co_return std::pair<kv_entry, status>{{}, status("key not found")};
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
                entry.revision = std::stoull(hdr_val);
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
            co_return std::pair<kv_entry, status>{{}, status("key not found")};
        }

        co_return std::pair<kv_entry, status>{std::move(entry), status()};
    }

    // KV delete implementation
    asio::awaitable<std::pair<uint64_t, status>> kv_delete_impl(
        string_view bucket, string_view key,
        std::chrono::milliseconds timeout) {

        if (!m_is_connected) {
            co_return std::pair<uint64_t, status>{0, status("not connected")};
        }

        // KV subject format: $KV.{bucket}.{key}
        std::string subject = fmt::format("$KV.{}.{}", bucket, key);

        // Publish empty payload with KV-Operation: DEL header
        headers_t headers = {{"KV-Operation", "DEL"}};
        std::span<const char> empty_payload;

        auto [ack, pub_status] = co_await js_publish_impl(subject, empty_payload, headers, timeout);

        if (pub_status.failed()) {
            co_return std::pair<uint64_t, status>{0, pub_status};
        }

        co_return std::pair<uint64_t, status>{ack.sequence, status()};
    }

    // KV watch implementation
    asio::awaitable<std::pair<ikv_watcher_sptr, status>> kv_watch_impl(
        string_view bucket, on_kv_entry_cb cb, string_view key) {

        if (!m_is_connected) {
            co_return std::pair<ikv_watcher_sptr, status>{nullptr, status("not connected")};
        }

        std::string bucket_str(bucket);
        std::string key_filter_str(key);

        // Build filter subject: $KV.{bucket}.{key} or $KV.{bucket}.> for all keys
        std::string filter_subject;
        if (key.empty()) {
            filter_subject = fmt::format("$KV.{}.>", bucket);
        } else {
            filter_subject = fmt::format("$KV.{}.{}", bucket, key);
        }

        // KV stream name is KV_{bucket}
        std::string stream_name = fmt::format("KV_{}", bucket);

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
        using nlohmann::json;
        std::string err_msg;
        try {
            auto j = json::parse(info);
            if (j.contains("max_payload") && j["max_payload"].is_number()) {
                m_max_payload = j["max_payload"].get<std::size_t>();
            }
        } catch (const json::exception& e) {
            err_msg = fmt::format("failed to parse INFO from server: {}", e.what());
        }

        if (!err_msg.empty()) {
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
        try {
            sid_u = static_cast<std::size_t>(std::stoll(sid_str.data(), nullptr, 10));
        } catch (const std::exception& e) {
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
        co_await it->second->callback()(subject, reply_to.has_value() ? reply_to : std::nullopt, payload_span);

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
        try {
            sid_u = static_cast<std::size_t>(std::stoll(sid_str.data(), nullptr, 10));
        } catch (const std::exception& e) {
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

        // Check if subscription has a headers callback
        if (it->second->has_headers_callback()) {
            // Build full message with headers
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

    std::shared_ptr<ssl::context> m_ssl_ctx;
    uni_socket<SocketType> m_socket;
};

// Implementation of js_subscription::send_ack (needs connection class to be complete)
template <class SocketType>
asio::awaitable<status> js_subscription<SocketType>::send_ack(
    const js_message& msg, std::string_view ack_body) {

    if (!m_active.load()) {
        co_return status("subscription is not active");
    }

    if (!msg.msg.reply_to) {
        co_return status("message has no reply subject for acknowledgment");
    }

    std::span<const char> payload(ack_body.data(), ack_body.size());
    co_return co_await m_conn->publish(*msg.msg.reply_to, payload, std::nullopt);
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
