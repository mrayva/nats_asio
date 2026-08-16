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

// Shared per-message output path for grub/js_grub/js_fetch: apply --translate
// if configured, then format as raw/json/normal/none. The three modes only
// differ in what extra context they wrap the subject/payload with (grub's
// optional timestamp and reply-to; js_grub/js_fetch's always-present
// stream/seq), so that's threaded through as pre-formatted string fragments
// rather than duplicating the whole switch statement three times.

#include "common.hpp"
#include "../include/worker.hpp"
#include "../include/string_utils.hpp"
#include "../include/zerialize_json.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/io_context.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <span>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <vector>

namespace nats_tool {

// Shared --dump_file handling for grub/js_grub/js_fetch: opens `path` for
// binary output if non-empty (logging and falling back to stdout on
// failure), and flushes periodically rather than after every message.
// Flushing per-message forces a write syscall per line, throttling
// throughput to per-message syscall latency instead of the buffered writes
// std::ofstream already gives for free - the core connection's own write
// path uses the equivalent threshold/interval-based flush strategy rather
// than flushing per-write, for the same reason.
class dump_file_writer {
public:
    dump_file_writer(const std::string& path, const std::shared_ptr<spdlog::logger>& log,
                     std::size_t flush_every = 100)
        : m_flush_every(flush_every) {
        if (path.empty()) {
            return;
        }
        m_file = std::make_unique<std::ofstream>(path, std::ios::binary);
        if (!m_file->is_open()) {
            log->error("Failed to open dump file: {}", path);
            m_file.reset();
        }
    }

    // The stream to write each message to: the dump file if one is open,
    // stdout otherwise.
    std::ostream& stream() { return m_file ? *m_file : std::cout; }

    // Call once per message written via stream(). Only the dump-file path
    // ever needs flushing - stdout's own buffering/flush-on-exit is fine
    // for terminal/pipe output.
    void on_message_written() {
        if (!m_file) {
            return;
        }
        if (++m_pending >= m_flush_every) {
            m_file->flush();
            m_pending = 0;
        }
    }

private:
    std::unique_ptr<std::ofstream> m_file;
    std::size_t m_flush_every;
    std::size_t m_pending{0};
};

// Runs `cmd` on `payload` via the shared translate_payload() (on a background
// thread) if `cmd` is non-empty, writing the result into `storage` and
// returning a view of it. If `cmd` is empty, returns `payload` itself
// unchanged - no copy, matching the fast path every mode already relied on
// when --translate isn't given.
inline asio::awaitable<std::span<const char>> apply_translate_if_configured(
    const std::string& cmd, nats_asio::string_view subject, std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log, std::string& storage) {
    if (cmd.empty()) {
        co_return payload;
    }
    std::string subj_str(subject);
    std::vector<char> payload_copy(payload.begin(), payload.end());
    auto cmd_copy = cmd;
    auto log_copy = log;
    storage = co_await async_run_blocking([cmd_copy, subj_str, payload_copy, log_copy]() {
        return translate_payload(cmd_copy, subj_str, std::span<const char>(payload_copy), log_copy);
    });
    co_return std::span<const char>(storage.data(), storage.size());
}

// Formats NATS message headers as a `,"headers":{...}` json_suffix_fields
// fragment, or an empty string when there are none -- the same
// only-add-the-field-when-present convention emit_message already uses for
// reply_to (grubber) and stream/seq (js_grubber/js_fetcher). Works with any
// range of key/value pairs whose members are convertible to
// std::string_view, so it takes nats_asio::headers_t (owned, js_grubber's
// js_message::headers) and nats_asio::headers_view_t (zero-copy, what
// lazy_headers_view::get() returns) equally well without a second overload.
template <typename HeaderRange>
inline std::string format_headers_json_suffix(const HeaderRange& headers) {
    if (headers.empty()) return {};

    std::string out = ",\"headers\":{";
    bool first = true;
    for (const auto& [key, value] : headers) {
        if (!first) out += ',';
        first = false;
        out += '"';
        out += escape_json_string(std::span<const char>(key.data(), key.size()));
        out += "\":\"";
        out += escape_json_string(std::span<const char>(value.data(), value.size()));
        out += '"';
    }
    out += '}';
    return out;
}

// Formats and writes one message to `out` per output_mode:
//   - raw: line_prefix (if any) followed by the payload
//   - json: deserializes via `format` if set (tracking failures in `stats`,
//     stopping `ioc` if the bad-message threshold is exceeded), otherwise
//     string-escapes the raw payload. json_prefix_fields is inserted right
//     after '{' (before "subject"), json_suffix_fields right after
//     "subject":"..." (before "payload"). When a binary format successfully
//     deserializes and both are empty, the deserialized JSON is written bare
//     (no envelope) rather than needlessly wrapping it.
//   - normal: line_prefix (if any), then "[subject] payload"
//   - none: nothing
inline void emit_message(std::ostream& out, output_mode mode, nats_asio::string_view subject,
                         std::span<const char> payload, std::optional<binary_format> format,
                         deserializer_stats& stats, const std::shared_ptr<spdlog::logger>& log,
                         asio::io_context& ioc, std::string_view json_prefix_fields = {},
                         std::string_view json_suffix_fields = {},
                         std::string_view line_prefix = {}) {
    switch (mode) {
        case output_mode::raw:
            out << line_prefix;
            out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
            out << '\n';
            break;
        case output_mode::json: {
            if (format) {
                auto json_result = deserialize_to_json(payload, *format, log);
                if (json_result) {
                    stats.record_success();
                    if (json_prefix_fields.empty() && json_suffix_fields.empty()) {
                        out << *json_result << '\n';
                    } else {
                        out << "{" << json_prefix_fields << "\"subject\":\"" << subject << "\""
                            << json_suffix_fields << ",\"payload\":" << *json_result << "}\n";
                    }
                } else {
                    bool should_exit = stats.record_failure();
                    log->warn("Failed to deserialize message on subject '{}' (bad: {}/{}, {:.2f}%)",
                             std::string(subject), stats.bad_messages(), stats.total_messages(),
                             stats.bad_percentage());
                    if (should_exit) {
                        log->error("Error threshold exceeded - exiting");
                        ioc.stop();
                    }
                }
                break;
            }

            std::string escaped = escape_json_string(payload);
            out << "{" << json_prefix_fields << "\"subject\":\"" << subject << "\""
                << json_suffix_fields << ",\"payload\":\"" << escaped << "\"}\n";
            break;
        }
        case output_mode::normal:
            out << line_prefix << "[" << subject << "] ";
            out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
            out << '\n';
            break;
        case output_mode::none:
            break;
    }
}

} // namespace nats_tool
