/*
MIT License

Copyright (c) 2019 Vladislav Troinich

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

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

namespace nats_tool {

// Output mode for messages
enum class output_mode { none, normal, raw, json };

// Input format for publisher
enum class input_format { line, json, csv };

// Input configuration for publisher
struct input_config {
    input_format format = input_format::line;
    std::string subject_template;  // Template with {{field}} placeholders
    std::vector<std::string> payload_fields;  // Fields to include in payload (empty = all)
    std::vector<std::string> csv_headers;  // Header names for CSV input
};

// Base worker class with stats timer
class worker {
public:
    worker(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval)
        : m_stats_interval(stats_interval), m_counter(0), m_ioc(ioc), m_log(console) {
        if (m_stats_interval > 0) {
            asio::co_spawn(
                ioc, [this]() -> asio::awaitable<void> { return stats_timer(); }, asio::detached);
        }
    }

    asio::awaitable<void> stats_timer() {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        while (true) {
            timer.expires_after(std::chrono::seconds(m_stats_interval));

            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));

            if (ec) {
                co_return;
            }

            m_log->info("Stats: {} events/sec", m_counter / m_stats_interval);
            m_counter = 0;
        }
    }

protected:
    int m_stats_interval;
    std::size_t m_counter;
    asio::io_context& m_ioc;
    std::shared_ptr<spdlog::logger> m_log;
};

} // namespace nats_tool
