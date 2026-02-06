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
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <fstream>
#include <memory>
#include <chrono>
#include <ctime>

namespace nats_tool {

class grubber : public worker {
public:
    grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
            output_mode mode, const std::string& dump_file = {}, const std::string& translate_cmd = {},
            bool show_timestamp = false, std::optional<binary_format> format = std::nullopt,
            std::size_t max_bad_messages = 0, double max_bad_percentage = 0.0)
        : worker(ioc, console, stats_interval), m_output_mode(mode), m_translate_cmd(translate_cmd),
          m_show_timestamp(show_timestamp), m_format(format),
          m_deserializer_stats(max_bad_messages, max_bad_percentage) {
        if (!dump_file.empty()) {
            m_dump_file = std::make_unique<std::ofstream>(dump_file, std::ios::binary);
            if (!m_dump_file->is_open()) {
                console->error("Failed to open dump file: {}", dump_file);
                m_dump_file.reset();
            }
        }
    }

    asio::awaitable<void> on_message(nats_asio::string_view subject,
                                     nats_asio::optional<nats_asio::string_view> reply_to,
                                     std::span<const char> payload) {
        m_counter++;

        std::ostream* out = m_dump_file ? m_dump_file.get() : &std::cout;

        // Apply translation if configured (runs on background thread)
        std::string translated;
        std::span<const char> output_payload = payload;
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

        // Get timestamp if needed
        std::string timestamp_str;
        if (m_show_timestamp && m_output_mode != output_mode::none) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) % 1000;
            std::tm tm_now{};
            localtime_r(&time_t_now, &tm_now);
            timestamp_str = fmt::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
                tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday,
                tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec, static_cast<int>(ms.count()));
        }

        switch (m_output_mode) {
            case output_mode::raw:
                if (m_show_timestamp) {
                    *out << "[" << timestamp_str << "] ";
                }
                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                *out << '\n';
                break;
            case output_mode::json: {
                // If binary format specified, deserialize to JSON
                if (m_format) {
                    auto json_result = deserialize_to_json(output_payload, *m_format, m_log);
                    if (json_result) {
                        // Success - output the deserialized JSON
                        m_deserializer_stats.record_success();

                        // Wrap in envelope with metadata if needed
                        if (m_show_timestamp || reply_to) {
                            *out << "{";
                            if (m_show_timestamp) {
                                *out << "\"timestamp\":\"" << timestamp_str << "\",";
                            }
                            *out << "\"subject\":\"" << subject << "\"";
                            if (reply_to) {
                                *out << ",\"reply_to\":\"" << *reply_to << "\"";
                            }
                            *out << ",\"payload\":" << *json_result << "}\n";
                        } else {
                            // Just output the deserialized payload directly
                            *out << *json_result << '\n';
                        }
                    } else {
                        // Failed to deserialize - skip or exit
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

                // No binary format - escape payload as string (original behavior)
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
                *out << "{";
                if (m_show_timestamp) {
                    *out << "\"timestamp\":\"" << timestamp_str << "\",";
                }
                *out << "\"subject\":\"" << subject << "\"";
                if (reply_to) {
                    *out << ",\"reply_to\":\"" << *reply_to << "\"";
                }
                *out << ",\"payload\":\"" << escaped << "\"}\n";
                break;
            }
            case output_mode::normal:
                if (m_show_timestamp) {
                    *out << "[" << timestamp_str << "] ";
                }
                *out << "[" << subject << "] ";
                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                *out << '\n';
                break;
            case output_mode::none:
                break;
        }

        if (m_dump_file) {
            m_dump_file->flush();
        }

        co_return;
    }

private:
    output_mode m_output_mode;
    std::string m_translate_cmd;
    bool m_show_timestamp;
    std::optional<binary_format> m_format;
    deserializer_stats m_deserializer_stats;
    std::unique_ptr<std::ofstream> m_dump_file;
};

} // namespace nats_tool
