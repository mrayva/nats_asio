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

class kv_watcher_handler : public worker {
public:
    kv_watcher_handler(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                       int stats_interval, bool print_to_stdout)
        : worker(ioc, console, stats_interval), m_print_to_stdout(print_to_stdout) {}

    asio::awaitable<void> on_kv_entry(const nats_asio::kv_entry& entry) {
        m_counter++;

        if (m_print_to_stdout) {
            std::string op_str;
            switch (entry.op) {
                case nats_asio::kv_entry::operation::put: op_str = "PUT"; break;
                case nats_asio::kv_entry::operation::del: op_str = "DEL"; break;
                case nats_asio::kv_entry::operation::purge: op_str = "PURGE"; break;
            }
            std::cout << "[" << op_str << "] " << entry.bucket << "/" << entry.key
                      << " rev=" << entry.revision;
            if (entry.op == nats_asio::kv_entry::operation::put && !entry.value.empty()) {
                std::cout << " value=";
                std::cout.write(entry.value.data(), entry.value.size());
            }
            std::cout << std::endl;
        }

        co_return;
    }

private:
    bool m_print_to_stdout;
};

} // namespace nats_tool
