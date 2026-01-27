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

class kv_publisher : public worker {
public:
    kv_publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                 nats_asio::iconnection_sptr conn, const std::string& bucket,
                 int stats_interval, int max_in_flight, const std::string& separator,
                 int kv_timeout_ms)
        : worker(ioc, console, stats_interval), m_conn(std::move(conn)),
          m_bucket(bucket), m_in_flight(0), m_max_in_flight(max_in_flight),
          m_separator(separator), m_kv_timeout(std::chrono::milliseconds(kv_timeout_ms)),
          m_stdin(ioc, ::dup(STDIN_FILENO)) {
        asio::co_spawn(ioc, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        asio::streambuf buf;

        for (;;) {
            // Async read line from stdin
            auto [ec, bytes_read] = co_await asio::async_read_until(
                m_stdin, buf, '\n', asio::as_tuple(asio::use_awaitable));

            if (ec) {
                if (ec == asio::error::eof || ec == asio::error::not_found) {
                    break;  // EOF or no more data
                }
                m_log->error("stdin read error: {}", ec.message());
                break;
            }

            // Extract line from buffer (without newline)
            std::string line;
            std::istream is(&buf);
            std::getline(is, line);

            // Skip empty lines
            if (line.empty()) {
                continue;
            }

            // Parse key|value - find first separator
            auto sep_pos = line.find(m_separator);
            if (sep_pos == std::string::npos) {
                m_log->error("invalid line format, missing separator '{}': {}", m_separator, line);
                continue;
            }

            std::string key = line.substr(0, sep_pos);
            std::string value_part = line.substr(sep_pos + m_separator.size());

            if (key.empty()) {
                m_log->error("empty key in line: {}", line);
                continue;
            }

            // Check if this is a delete operation (value starts with separator)
            bool is_delete = false;
            if (value_part.size() >= m_separator.size() &&
                value_part.substr(0, m_separator.size()) == m_separator) {
                is_delete = true;
            }

            // Wait until connection is ready
            while (!m_conn->is_connected()) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(asio::use_awaitable);
            }

            // Backpressure: wait if too many operations in flight
            while (m_in_flight >= m_max_in_flight) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(5));
                co_await timer.async_wait(asio::use_awaitable);
            }

            m_in_flight++;

            // Capture data for async operation
            auto key_copy = std::make_shared<std::string>(std::move(key));
            auto value_copy = std::make_shared<std::string>(std::move(value_part));

            // Fire-and-forget: dispatch KV operation without waiting
            asio::co_spawn(
                m_ioc,
                [this, key_copy, value_copy, is_delete]() -> asio::awaitable<void> {
                    if (is_delete) {
                        auto [rev, s] = co_await m_conn->kv_delete(m_bucket, *key_copy, m_kv_timeout);
                        if (s.failed()) {
                            m_log->error("kv_delete failed for key '{}': {}", *key_copy, s.error());
                        } else {
                            m_counter++;
                            m_log->debug("deleted key '{}' rev={}", *key_copy, rev);
                        }
                    } else {
                        std::span<const char> value_span(value_copy->data(), value_copy->size());
                        auto [rev, s] = co_await m_conn->kv_put(m_bucket, *key_copy, value_span, m_kv_timeout);
                        if (s.failed()) {
                            m_log->error("kv_put failed for key '{}': {}", *key_copy, s.error());
                        } else {
                            m_counter++;
                            m_log->debug("put key '{}' rev={}", *key_copy, rev);
                        }
                    }
                    m_in_flight--;
                    co_return;
                },
                asio::detached);
        }

        // Wait for all in-flight operations to complete
        m_log->info("EOF reached, waiting for {} in-flight KV operations", m_in_flight.load());
        while (m_in_flight > 0) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(asio::use_awaitable);
        }

        m_log->info("All KV operations complete, stopping");
        m_ioc.stop();
        co_return;
    }

private:
    nats_asio::iconnection_sptr m_conn;
    std::string m_bucket;
    std::atomic<int> m_in_flight;
    int m_max_in_flight;
    std::string m_separator;
    std::chrono::milliseconds m_kv_timeout;
    asio::posix::stream_descriptor m_stdin;
};

} // namespace nats_tool
