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
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace nats_asio {

using std::optional;
using std::string_view;

using aio = asio::io_context;

using on_message_cb = std::function<asio::awaitable<void>(std::string_view subject,
                                                          std::optional<std::string_view> reply_to,
                                                          std::span<const char> payload)>;

class status {
public:
    status() = default;

    status(const std::string& error) : m_error(error) {}

    ~status() = default;

    [[nodiscard]] bool failed() const noexcept {
        return m_error.has_value();
    }

    [[nodiscard]] std::string error() const {
        if (!m_error.has_value())
            return {};

        return m_error.value();
    }

private:
    optional<std::string> m_error;
};

struct isubscription {
    virtual ~isubscription() = default;

    [[nodiscard]] virtual uint64_t sid() noexcept = 0;

    virtual void cancel() noexcept = 0;
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
};

struct iconnection {
    virtual ~iconnection() = default;

    virtual void start(const connect_config& conf) = 0;

    virtual void stop() noexcept = 0;

    [[nodiscard]] virtual bool is_connected() noexcept = 0;

    [[nodiscard]] virtual asio::awaitable<status> publish(string_view subject, std::span<const char> payload,
                                                          optional<string_view> reply_to) = 0;

    [[nodiscard]] virtual asio::awaitable<status> unsubscribe(const isubscription_sptr& p) = 0;

    [[nodiscard]] virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, optional<string_view> queue, on_message_cb cb) = 0;
};
using iconnection_sptr = std::shared_ptr<iconnection>;

using on_connected_cb = std::function<asio::awaitable<void>(iconnection&)>;
using on_disconnected_cb = std::function<asio::awaitable<void>(iconnection&)>;
using on_error_cb = std::function<asio::awaitable<void>(iconnection&, string_view)>;

[[nodiscard]] iconnection_sptr create_connection(aio& io, const on_connected_cb& connected_cb,
                                   const on_disconnected_cb& disconnected_cb,
                                   const on_error_cb& error_cb, optional<ssl_config> ssl_conf);

} // namespace nats_asio
