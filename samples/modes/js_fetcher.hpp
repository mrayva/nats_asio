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
#include "../include/string_utils.hpp"
#include "../include/zerialize_json.hpp"
#include "common.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <fstream>
#include <memory>
#include <chrono>
#include <iostream>

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
          m_translate_cmd(translate_cmd) {
        if (!dump_file.empty()) {
            m_dump_file = std::make_unique<std::ofstream>(dump_file, std::ios::binary);
            if (!m_dump_file->is_open()) {
                console->error("Failed to open dump file: {}", dump_file);
                m_dump_file.reset();
            }
        }
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
                        std::ostream* out = m_dump_file ? m_dump_file.get() : &std::cout;
                        const auto& payload = msg.msg.payload;
                        const auto& subject = msg.msg.subject;

                        // Apply translation if configured
                        std::string translated;
                        std::span<const char> output_payload(payload.data(), payload.size());
                        if (!m_translate_cmd.empty()) {
                            std::string subj_str(subject);
                            std::vector<char> payload_copy(payload.begin(), payload.end());
                            auto log = m_log;
                            std::string cmd = m_translate_cmd;
                            translated = co_await async_run_blocking([cmd, subj_str, payload_copy, log]() {
                                return translate_payload(cmd, subj_str, std::span<const char>(payload_copy), log);
                            });
                            output_payload = std::span<const char>(translated.data(), translated.size());
                        }

                        switch (m_output_mode) {
                            case output_mode::raw:
                                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                                *out << '\n';
                                break;
                            case output_mode::json: {
                                if (m_format) {
                                    auto json_result = deserialize_to_json(output_payload, *m_format, m_log);
                                    if (json_result) {
                                        m_deserializer_stats.record_success();
                                        *out << "{\"subject\":\"" << subject << "\""
                                             << ",\"stream\":\"" << msg.stream << "\""
                                             << ",\"seq\":" << msg.stream_sequence
                                             << ",\"payload\":" << *json_result << "}\n";
                                    } else {
                                        bool should_exit = m_deserializer_stats.record_failure();
                                        m_log->warn("Failed to deserialize message on subject '{}' (bad: {}/{}, {:.2f}%)",
                                                   std::string(subject),
                                                   m_deserializer_stats.bad_messages(),
                                                   m_deserializer_stats.total_messages(),
                                                   m_deserializer_stats.bad_percentage());
                                        if (should_exit) {
                                            m_log->error("Error threshold exceeded - exiting");
                                            m_ioc.stop();
                                        }
                                    }
                                    break;
                                }

                                std::string escaped;
                                escaped.reserve(output_payload.size());
                                for (char c : output_payload) {
                                    switch (c) {
                                        case '"': escaped += "\\\""; break;
                                        case '\\': escaped += "\\\\"; break;
                                        case '\n': escaped += "\\n"; break;
                                        case '\r': escaped += "\\r"; break;
                                        case '\t': escaped += "\\t"; break;
                                        default:
                                            if (static_cast<unsigned char>(c) < 32) {
                                                escaped += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                                            } else {
                                                escaped += c;
                                            }
                                    }
                                }
                                *out << "{\"subject\":\"" << subject << "\""
                                     << ",\"stream\":\"" << msg.stream << "\""
                                     << ",\"seq\":" << msg.stream_sequence
                                     << ",\"payload\":\"" << escaped << "\"}\n";
                                break;
                            }
                            case output_mode::normal:
                                *out << "[" << subject << "] ";
                                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                                *out << '\n';
                                m_log->debug("stream={} consumer={} seq={}/{} delivered={}",
                                            msg.stream, msg.consumer, msg.stream_sequence,
                                            msg.consumer_sequence, msg.num_delivered);
                                break;
                            case output_mode::none:
                                break;
                        }

                        if (m_dump_file) {
                            m_dump_file->flush();
                        }
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
    std::unique_ptr<std::ofstream> m_dump_file;
};

} // namespace nats_tool
