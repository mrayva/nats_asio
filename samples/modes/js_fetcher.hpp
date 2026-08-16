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
#include "../include/zerialize_json.hpp"
#include "message_output.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <memory>
#include <chrono>

namespace nats_tool {

// JetStream fetcher using pull consumer
class js_fetcher : public worker {
public:
    js_fetcher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
               const nats_asio::iconnection_sptr& conn, const std::string& stream,
               const std::string& consumer, int stats_interval, bool print_to_stdout,
               int batch_size, int fetch_interval_ms,
               output_mode mode = output_mode::normal,
               std::optional<binary_format> format = std::nullopt,
               std::size_t max_bad_messages = 0, double max_bad_percentage = 0.0,
               const std::string& dump_file = {},
               const std::string& translate_cmd = {})
        : worker(ioc, console, stats_interval), m_conn(conn), m_stream(stream),
          m_consumer(consumer), m_print_to_stdout(print_to_stdout),
          m_batch_size(batch_size), m_fetch_interval_ms(fetch_interval_ms),
          m_output_mode(mode), m_format(format),
          m_deserializer_stats(max_bad_messages, max_bad_percentage),
          m_translate_cmd(translate_cmd), m_dump_writer(dump_file, console) {
        asio::co_spawn(ioc, fetch_loop(), asio::detached);
    }

    asio::awaitable<void> fetch_loop() {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        while (true) {
            if (!m_conn->is_connected()) {
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(asio::use_awaitable);
                continue;
            }

            auto [messages, s] = co_await m_conn->js_fetch(
                m_stream, m_consumer, m_batch_size, std::chrono::milliseconds(5000));

            if (s.failed()) {
                m_log->error("js_fetch failed: {}", s.error());
            } else {
                for (const auto& msg : messages) {
                    m_counter++;

                    if (m_print_to_stdout) {
                        std::ostream& out = m_dump_writer.stream();
                        const auto& payload = msg.msg.payload;
                        const auto& subject = msg.msg.subject;
                        std::span<const char> payload_span(payload.data(), payload.size());

                        std::string translated_storage;
                        auto output_payload = co_await apply_translate_if_configured(
                            m_translate_cmd, subject, payload_span, m_log, translated_storage);

                        std::string json_suffix_fields = fmt::format(
                            ",\"stream\":\"{}\",\"seq\":{}", msg.stream, msg.stream_sequence);
                        json_suffix_fields += format_headers_json_suffix(msg.msg.headers);

                        emit_message(out, m_output_mode, subject, output_payload, m_format,
                                    m_deserializer_stats, m_log, m_ioc, {}, json_suffix_fields);

                        if (m_output_mode == output_mode::normal) {
                            m_log->debug("stream={} consumer={} seq={}/{} delivered={}",
                                        msg.stream, msg.consumer, msg.stream_sequence,
                                        msg.consumer_sequence, msg.num_delivered);
                        }

                        m_dump_writer.on_message_written();
                    }

                    // Acknowledge the message
                    if (msg.msg.reply_to) {
                        auto ack_status = co_await m_conn->publish(
                            *msg.msg.reply_to, std::span<const char>("+ACK", 4), std::nullopt);
                        if (ack_status.failed()) {
                            m_log->error("ack failed: {}", ack_status.error());
                        }
                    }
                }

                if (messages.empty()) {
                    m_log->debug("No messages available");
                }
            }

            if (m_fetch_interval_ms > 0) {
                timer.expires_after(std::chrono::milliseconds(m_fetch_interval_ms));
                co_await timer.async_wait(asio::use_awaitable);
            }
        }
    }

private:
    nats_asio::iconnection_sptr m_conn;
    std::string m_stream;
    std::string m_consumer;
    bool m_print_to_stdout;
    int m_batch_size;
    int m_fetch_interval_ms;
    output_mode m_output_mode;
    std::optional<binary_format> m_format;
    deserializer_stats m_deserializer_stats;
    std::string m_translate_cmd;
    dump_file_writer m_dump_writer;
};

} // namespace nats_tool
