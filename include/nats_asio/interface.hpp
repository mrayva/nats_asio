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

#include <asio/deadline_timer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/spawn.hpp>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace nats_asio {

using std::optional;
using std::string_view;

using aio = asio::io_context;
using ctx = asio::yield_context;

using on_message_cb = std::function<asio::awaitable<void>(std::string_view subject,
                                                          std::optional<std::string_view> reply_to,
                                                          const char* raw, std::size_t n)>;

} // namespace nats_asio

namespace nats_asio {

class status {
public:
    status() = default;

    status(const std::string& error) : m_error(error) {}

    virtual ~status() = default;

    bool failed() const {
        return m_error.has_value();
    }

    std::string error() const {
        if (!m_error.has_value())
            return {};

        return m_error.value();
    }

private:
    optional<std::string> m_error;
};

struct isubscription {
    virtual ~isubscription() = default;

    virtual uint64_t sid() = 0;

    virtual void cancel() = 0;
};
using isubscription_sptr = std::shared_ptr<isubscription>;

struct ssl_config {
    std::string ssl_key;
    std::string ssl_cert;
    std::string ssl_ca;
    std::string ssl_dh;
    bool ssl_required = false;
    bool ssl_verify = true;
};

struct connect_config {
    std::string address;
    uint16_t port;

    bool verbose = false;
    bool pedantic = false;

    optional<std::string> user;
    optional<std::string> password;
    optional<std::string> token;
};

struct iconnection {
    virtual ~iconnection() = default;

    virtual void start(const connect_config& conf) = 0;

    virtual void stop() = 0;

    virtual bool is_connected() = 0;

    virtual asio::awaitable<status> publish(string_view subject, const char* raw, std::size_t n,
                                            optional<string_view> reply_to) = 0;

    virtual asio::awaitable<status> unsubscribe(const isubscription_sptr& p) = 0;

    virtual asio::awaitable<std::pair<isubscription_sptr, status>>
    subscribe(string_view subject, optional<string_view> queue, on_message_cb cb) = 0;
};
using iconnection_sptr = std::shared_ptr<iconnection>;

// typedef std::function<void(iconnection&, ctx)> on_connected_cb;
// typedef std::function<void(iconnection&, ctx)> on_disconnected_cb;

using on_connected_cb = std::function<asio::awaitable<void>(iconnection&)>;
using on_disconnected_cb = std::function<asio::awaitable<void>(iconnection&)>;

iconnection_sptr create_connection(aio& io, const on_connected_cb& connected_cb,
                                   const on_disconnected_cb& disconnected_cb,
                                   optional<ssl_config> ssl_conf);

} // namespace nats_asio
