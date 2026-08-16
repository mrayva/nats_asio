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
          m_deserializer_stats(max_bad_messages, max_bad_percentage),
          m_dump_writer(dump_file, console) {}

    // Takes the zero-copy message_view (rather than separate subject/
    // reply_to/payload args) specifically to get at msg.headers -- the
    // legacy on_message_cb subscribe() overload this used to bind to has
    // no headers at all. subject/reply_to/payload/headers here alias the
    // connection's own read buffer and are only valid until this
    // coroutine's next real suspension point (see message_view's own
    // warning) -- matches the existing reply_to usage below, which reads
    // it after the same co_await for the same reason: apply_translate_if_
    // configured() only actually suspends when --translate is given, and
    // copies subject/payload to owned storage itself before it does.
    asio::awaitable<void> on_message(const nats_asio::message_view& msg) {
        m_counter++;

        std::ostream& out = m_dump_writer.stream();

        std::string translated_storage;
        auto output_payload = co_await apply_translate_if_configured(
            m_translate_cmd, msg.subject, msg.payload, m_log, translated_storage);

        // Timestamp, if requested, prefixes raw/normal lines and prepends the
        // JSON envelope's field list; reply_to/headers (if present) append to it.
        std::string line_prefix;
        std::string json_prefix_fields;
        if (m_show_timestamp && m_output_mode != output_mode::none) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) % 1000;
            std::tm tm_now{};
            localtime_r(&time_t_now, &tm_now);
            std::string timestamp_str = fmt::format(
                "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
                tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday,
                tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec, static_cast<int>(ms.count()));
            line_prefix = fmt::format("[{}] ", timestamp_str);
            json_prefix_fields = fmt::format("\"timestamp\":\"{}\",", timestamp_str);
        }
        std::string json_suffix_fields;
        if (msg.reply_to) {
            json_suffix_fields = fmt::format(",\"reply_to\":\"{}\"", *msg.reply_to);
        }
        if (msg.headers.has_data()) {
            json_suffix_fields += format_headers_json_suffix(msg.headers.get());
        }

        emit_message(out, m_output_mode, msg.subject, output_payload, m_format, m_deserializer_stats,
                    m_log, m_ioc, json_prefix_fields, json_suffix_fields, line_prefix);

        m_dump_writer.on_message_written();

        co_return;
    }

private:
    output_mode m_output_mode;
    std::string m_translate_cmd;
    bool m_show_timestamp;
    std::optional<binary_format> m_format;
    deserializer_stats m_deserializer_stats;
    dump_file_writer m_dump_writer;
};

} // namespace nats_tool
