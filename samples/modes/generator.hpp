/*
MIT License

Copyright (c) 2019 Vladislav Troinich
Copyright (c) 2024-2026 mrayva

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

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

#include "../include/worker.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <chrono>

namespace nats_tool {

class generator : public worker {
public:
    generator(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              const nats_asio::iconnection_sptr& conn, const std::string& topic, int stats_interval,
              int publish_interval_ms)
        : worker(ioc, console, stats_interval), m_publish_interval_ms(publish_interval_ms),
          m_topic(topic), m_conn(conn) {
        if (m_publish_interval_ms >= 0) {
            asio::co_spawn(ioc, publish(), asio::detached);
        }
    }

    asio::awaitable<void> publish() {
        asio::steady_timer timer(co_await asio::this_coro::executor);
        const std::string msg("{\"value\": 123}");
        std::span<const char> payload_span(msg.data(), msg.size());

        for (;;) {
            auto s = co_await m_conn->publish(m_topic, payload_span, std::nullopt);

            if (s.failed()) {
                m_log->error("publish failed with error {}", s.error());
            } else {
                m_counter++;
            }

            timer.expires_after(std::chrono::milliseconds(m_publish_interval_ms));

            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));

            if (ec) {
                co_return;
            }
        }
    }

private:
    int m_publish_interval_ms;
    std::string m_topic;
    nats_asio::iconnection_sptr m_conn;
};

} // namespace nats_tool
