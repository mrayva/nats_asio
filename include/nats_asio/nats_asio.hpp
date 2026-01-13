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
#include <concepts>
#include <istream>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>

#include "interface.hpp"

namespace nats_asio {

namespace ssl = asio::ssl;

namespace protocol {
    constexpr string_view crlf = "\r\n";
    constexpr string_view pub_fmt = "PUB {} {} {}\r\n";
    constexpr string_view sub_fmt = "SUB {} {} {}\r\n";
    constexpr string_view unsub_fmt = "UNSUB {}\r\n";
    constexpr string_view connect_fmt = "CONNECT {}\r\n";
    constexpr string_view pong = "PONG\r\n";

    constexpr string_view op_msg = "MSG";
    constexpr string_view op_hmsg = "HMSG";
    constexpr string_view op_ping = "PING";
    constexpr string_view op_pong = "PONG";
    constexpr string_view op_ok = "+OK";
    constexpr string_view op_err = "-ERR";
    constexpr string_view op_info = "INFO";
    constexpr string_view delim = " ";
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
    subscription(uint64_t sid, const on_message_cb& cb) : m_cancel(false), m_cb(cb), m_sid(sid) {}

    subscription(const subscription&) = delete;
    subscription& operator=(const subscription&) = delete;

    void cancel() noexcept override {
        m_cancel.store(true);
    }

    [[nodiscard]] uint64_t sid() noexcept override {
        return m_sid;
    }

    [[nodiscard]] bool is_cancelled() const noexcept {
        return m_cancel.load();
    }

    [[nodiscard]] const on_message_cb& callback() const noexcept {
        return m_cb;
    }

private:
    std::atomic<bool> m_cancel;
    on_message_cb m_cb;
    uint64_t m_sid;
};

using subscription_sptr = std::shared_ptr<subscription>;

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
    subscribe(string_view subject, optional<string_view> queue, on_message_cb cb) override {
        if (!m_is_connected) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status("not connected")};
        }

        auto sid = next_sid();
        std::string payload = queue.has_value()
                                  ? fmt::format(fmt::runtime(protocol::sub_fmt), subject, queue.value(), sid)
                                  : fmt::format(fmt::runtime(protocol::sub_fmt), subject, "", sid);

        try {
            co_await asio::async_write(m_socket, asio::buffer(payload), asio::use_awaitable);
        } catch (const std::system_error& e) {
            co_return std::pair<isubscription_sptr, status>{isubscription_sptr(),
                                                            status(e.code().message())};
        }

        auto sub = std::make_shared<subscription>(sid, cb);
        m_subs.emplace(sid, sub);

        co_return std::pair<isubscription_sptr, status>{sub, status()};
    }

private:
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

} // namespace nats_asio
