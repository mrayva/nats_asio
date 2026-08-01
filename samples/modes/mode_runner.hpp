/*
MIT License

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
#include "../include/zerialize_json.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/io_context.hpp>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>

namespace nats_tool {

// All the CLI-parsed values any single-connection mode's runner might need.
// A plain aggregate rather than per-mode constructor parameter lists, since
// each concrete runner only reads the handful of fields it actually uses -
// this is what replaces main()'s two 14-way if/else-if dispatch chains.
struct mode_context {
    int stats_interval = 1;

    // grub / js_grub
    std::string topic;
    std::string queue_group;
    uint32_t max_msgs = 0;
    output_mode out_mode = output_mode::none;
    std::string dump_file;
    std::string translate_cmd;
    bool show_timestamp = false;
    std::optional<binary_format> binary_fmt;
    std::size_t max_bad_messages = 0;
    double max_bad_percentage = 0.0;

    // JetStream
    std::string js_stream;
    std::string js_consumer;
    std::string js_durable;
    bool auto_ack = false;
    int batch_size = 10;
    int fetch_interval_ms = 100;

    // KV
    std::string kv_bucket;
    std::string kv_key;
    std::string kv_value;
    uint64_t kv_revision = 0;
    std::string kv_separator = "|";
    int kv_timeout_ms = 5000;
    int max_in_flight = 1000;
    bool print_to_stdout = false;

    // generator
    int publish_interval_ms = -1;

    // req/reply, both sourced from the generic --data/--timeout flags
    std::string data_or_stdin;
    int timeout_ms = 5000;
    nats_asio::headers_t headers;
    bool echo_mode = false;
};

// A single-connection CLI mode's behavior, replacing what used to be one
// branch in each of main()'s two dispatch chains. Only override the method(s)
// this mode actually needs:
//   - on_connected(): logic that must run once the connection is live -
//     subscribe, or a one-shot KV/JetStream request. Runs inside the
//     connection's on-connected coroutine callback.
//   - setup(): construction of a worker object whose own constructor spawns
//     its independent coroutine (generator, js_fetcher, kv_publisher,
//     requester, replier). Runs synchronously right after the connection is
//     created and started, before io_context::run().
class mode_runner {
public:
    mode_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                std::atomic<bool>& operation_failed)
        : m_ioc(ioc), m_console(std::move(console)), m_operation_failed(operation_failed) {}
    virtual ~mode_runner() = default;

    virtual asio::awaitable<void> on_connected(nats_asio::iconnection_sptr /*conn*/) {
        co_return;
    }

    virtual void setup(nats_asio::iconnection_sptr /*conn*/) {}

protected:
    // Common "operation failed, stop the event loop" path shared by most
    // one-shot modes' error handling.
    void fail(std::string_view message) {
        m_console->error("{}", message);
        m_operation_failed.store(true, std::memory_order_release);
        m_ioc.stop();
    }

    asio::io_context& m_ioc;
    std::shared_ptr<spdlog::logger> m_console;
    std::atomic<bool>& m_operation_failed;
};

} // namespace nats_tool
