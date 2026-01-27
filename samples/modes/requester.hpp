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
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <memory>
#include <unistd.h>

namespace nats_tool {

class requester : public worker {
public:
    requester(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              nats_asio::iconnection_sptr conn, const std::string& topic,
              int stats_interval, int timeout_ms, const std::string& data,
              output_mode mode, const nats_asio::headers_t& headers = {})
        : worker(ioc, console, stats_interval), m_conn(std::move(conn)),
          m_topic(topic), m_timeout(std::chrono::milliseconds(timeout_ms)),
          m_data(data), m_output_mode(mode), m_headers(headers),
          m_stdin(ioc, ::dup(STDIN_FILENO)) {
        asio::co_spawn(ioc, run(), asio::detached);
    }

    asio::awaitable<void> run() {
        // Wait for connection
        while (!m_conn->is_connected()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        if (!m_data.empty()) {
            // Single request with provided data
            co_await send_request(m_data);
        } else {
            // Read requests from stdin
            asio::streambuf buf;
            for (;;) {
                auto [ec, bytes_read] = co_await asio::async_read_until(
                    m_stdin, buf, '\n', asio::as_tuple(asio::use_awaitable));

                if (ec) {
                    if (ec == asio::error::eof || ec == asio::error::not_found) {
                        break;
                    }
                    m_log->error("stdin read error: {}", ec.message());
                    break;
                }

                std::string line;
                std::istream is(&buf);
                std::getline(is, line);

                if (!line.empty()) {
                    co_await send_request(line);
                }
            }
        }

        m_log->info("Requester finished, {} requests sent", m_counter);
        m_ioc.stop();
        co_return;
    }

private:
    asio::awaitable<void> send_request(const std::string& payload) {
        std::span<const char> payload_span(payload.data(), payload.size());
        auto [reply, status] = m_headers.empty()
            ? co_await m_conn->request(m_topic, payload_span, m_timeout)
            : co_await m_conn->request(m_topic, payload_span, m_headers, m_timeout);

        if (status.failed()) {
            m_log->error("request failed: {}", status.error());
            co_return;
        }

        m_counter++;

        switch (m_output_mode) {
            case output_mode::raw:
                std::cout.write(reply.payload.data(), reply.payload.size());
                std::cout << std::endl;
                break;
            case output_mode::json: {
                nlohmann::json j;
                j["subject"] = reply.subject;
                if (reply.reply_to) j["reply_to"] = *reply.reply_to;
                j["payload"] = std::string(reply.payload.begin(), reply.payload.end());
                std::cout << j.dump() << std::endl;
                break;
            }
            case output_mode::normal:
            default:
                std::cout << "[" << reply.subject << "] ";
                std::cout.write(reply.payload.data(), reply.payload.size());
                std::cout << std::endl;
                break;
        }

        co_return;
    }

    nats_asio::iconnection_sptr m_conn;
    std::string m_topic;
    std::chrono::milliseconds m_timeout;
    std::string m_data;
    output_mode m_output_mode;
    nats_asio::headers_t m_headers;
    asio::posix::stream_descriptor m_stdin;
};

} // namespace nats_tool
