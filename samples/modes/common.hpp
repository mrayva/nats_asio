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

#include <asio/awaitable.hpp>
#include <asio/as_tuple.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <asio/use_awaitable.hpp>
#include <functional>
#include <future>
#include <istream>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <type_traits>

namespace nats_tool {

// Helper to run blocking work on a thread and await completion
// Prevents blocking the ASIO event loop
template<typename Func>
asio::awaitable<std::invoke_result_t<Func>> async_run_blocking(Func&& func) {
    using ResultType = std::invoke_result_t<Func>;

    auto executor = co_await asio::this_coro::executor;
    auto future = std::async(std::launch::async, std::forward<Func>(func));

    // Poll for completion using timer to avoid blocking the event loop
    asio::steady_timer timer(executor);
    while (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        timer.expires_after(std::chrono::milliseconds(1));
        co_await timer.async_wait(asio::use_awaitable);
    }

    co_return future.get();
}

// Reads newline-delimited lines from `stdin_stream` (already dup'd from
// STDIN_FILENO), invoking on_line(line) for each non-empty line, until EOF
// or an unrecoverable read error. Shared by requester/pubkv, which only
// differ in what they do with each line and what happens after the loop
// ends.
inline asio::awaitable<void> read_stdin_lines(
    asio::posix::stream_descriptor& stdin_stream, const std::shared_ptr<spdlog::logger>& log,
    std::function<asio::awaitable<void>(const std::string&)> on_line) {
    asio::streambuf buf;

    for (;;) {
        auto [ec, bytes_read] = co_await asio::async_read_until(
            stdin_stream, buf, '\n', asio::as_tuple(asio::use_awaitable));

        if (ec) {
            if (ec == asio::error::eof || ec == asio::error::not_found) {
                break;
            }
            log->error("stdin read error: {}", ec.message());
            break;
        }

        std::string line;
        std::istream is(&buf);
        std::getline(is, line);

        if (!line.empty()) {
            co_await on_line(line);
        }
    }
}

} // namespace nats_tool
