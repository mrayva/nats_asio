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
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <fstream>
#include <memory>

namespace nats_tool {

// JetStream subscriber using push consumer
class js_grubber : public worker {
public:
    js_grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
               output_mode mode, bool auto_ack, const std::string& dump_file = {},
               const std::string& translate_cmd = {},
               std::optional<binary_format> format = std::nullopt,
               std::size_t max_bad_messages = 0, double max_bad_percentage = 0.0)
        : worker(ioc, console, stats_interval), m_output_mode(mode), m_auto_ack(auto_ack),
          m_translate_cmd(translate_cmd), m_format(format),
          m_deserializer_stats(max_bad_messages, max_bad_percentage) {
        if (!dump_file.empty()) {
            m_dump_file = std::make_unique<std::ofstream>(dump_file, std::ios::binary);
            if (!m_dump_file->is_open()) {
                console->error("Failed to open dump file: {}", dump_file);
                m_dump_file.reset();
            }
        }
    }

    asio::awaitable<void> on_js_message(nats_asio::ijs_subscription& sub,
                                         const nats_asio::js_message& msg) {
        m_counter++;

        std::ostream* out = m_dump_file ? m_dump_file.get() : &std::cout;
        const auto& payload = msg.msg.payload;
        const auto& subject = msg.msg.subject;
        std::span<const char> payload_span(payload.data(), payload.size());

        std::string translated_storage;
        auto output_payload = co_await apply_translate_if_configured(
            m_translate_cmd, subject, payload_span, m_log, translated_storage);

        std::string json_suffix_fields =
            fmt::format(",\"stream\":\"{}\",\"seq\":{}", msg.stream, msg.stream_sequence);

        emit_message(*out, m_output_mode, subject, output_payload, m_format, m_deserializer_stats,
                    m_log, m_ioc, {}, json_suffix_fields);

        if (m_output_mode == output_mode::normal) {
            m_log->debug("stream={} consumer={} seq={}/{} delivered={}",
                        msg.stream, msg.consumer, msg.stream_sequence,
                        msg.consumer_sequence, msg.num_delivered);
        }

        if (m_dump_file) {
            m_dump_file->flush();
        }

        // Auto-acknowledge if enabled
        if (m_auto_ack) {
            auto s = co_await sub.ack(msg);
            if (s.failed()) {
                m_log->error("ack failed: {}", s.error());
            }
        }

        co_return;
    }

private:
    output_mode m_output_mode;
    bool m_auto_ack;
    std::string m_translate_cmd;
    std::optional<binary_format> m_format;
    deserializer_stats m_deserializer_stats;
    std::unique_ptr<std::ofstream> m_dump_file;
};

} // namespace nats_tool
