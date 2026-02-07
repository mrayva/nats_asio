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
#include "../include/fast_json_parser.hpp"
#include "../include/zerialize_json.hpp"
#include "../include/js_sliding_window.hpp"
#include <nats_asio/nats_asio.hpp>
#include <nats_asio/compression.hpp>
#include <nats_asio/http_reader.hpp>
#include <nats_asio/input_reader.hpp>
#include <nats_asio/multi_file_reader.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <vector>
#include <memory>
#include <chrono>

namespace nats_tool {

using nats_asio::zstd_compressor;
using nats_asio::input_source_config;
using nats_asio::async_http_reader;
using nats_asio::async_input_reader;
using nats_asio::async_multi_file_reader;

class publisher : public worker {
public:
    publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              std::vector<nats_asio::iconnection_sptr> connections, const std::string& topic,
              int stats_interval, int max_in_flight = 1000, bool jetstream = false,
              int js_timeout_ms = 5000, bool wait_for_ack = true,
              const nats_asio::headers_t& headers = {}, const std::string& reply_to = {},
              int count = 0, int sleep_ms = 0, const std::string& data = {},
              const input_config& in_cfg = {},
              const input_source_config& src_cfg = {},
              std::optional<binary_format> format = std::nullopt,
              std::size_t js_window_size = 1000,
              const std::string& js_stream = "",
              bool js_create_stream = false,
              int js_max_retries = 3)
        : worker(ioc, console, stats_interval), m_connections(std::move(connections)),
          m_topic(topic), m_next_conn(0), m_in_flight(0), m_max_in_flight(max_in_flight),
          m_jetstream(jetstream), m_js_timeout(std::chrono::milliseconds(js_timeout_ms)),
          m_wait_for_ack(wait_for_ack), m_headers(headers), m_reply_to(reply_to),
          m_count(count), m_sleep_ms(sleep_ms), m_data(data), m_input_cfg(in_cfg),
          m_src_cfg(src_cfg), m_format(format), m_input_reader(ioc, src_cfg, console),
          m_js_window_size(js_window_size), m_js_stream(js_stream),
          m_js_create_stream(js_create_stream), m_js_max_retries(js_max_retries),
          m_strand(asio::make_strand(ioc)) {

        // Initialize sliding window for JetStream if enabled
        if (m_jetstream && m_wait_for_ack) {
            m_js_window = std::make_shared<nats_tool::js_sliding_window>(
                js_window_size, m_js_timeout, m_log, m_js_max_retries);

            // Start ACK processor for first connection
            if (!m_connections.empty() && m_js_window) {
                m_ack_processor = std::make_unique<nats_tool::js_ack_processor>(
                    ioc, m_connections[0], m_js_window, m_log);
                m_ack_processor->start();
            }
        }

        m_log->debug("Publisher constructor complete, starting read_and_publish on strand");

        // Bind to strand for thread-safe execution
        asio::co_spawn(m_strand, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        // Wait until at least one connection is ready
        while (!has_connected_connection()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        // Ensure JetStream stream exists if requested
        if (m_jetstream && m_js_create_stream && !m_js_stream.empty()) {
            std::string stream_name = m_js_stream;
            m_log->info("Ensuring JetStream stream '{}' exists for subject '{}'",
                       stream_name, m_topic);

            auto conn = get_next_connection();
            bool stream_ok = co_await nats_tool::ensure_stream_for_subject(
                conn, stream_name, m_topic, m_log, m_js_timeout);

            if (!stream_ok) {
                m_log->error("Failed to ensure stream '{}' - aborting publish", stream_name);
                m_ioc.stop();
                co_return;
            }

            // Brief delay after stream creation to let NATS stabilize
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(200));
            co_await timer.async_wait(asio::use_awaitable);
        }

        if (m_count > 0 && !m_data.empty()) {
            // Publish fixed message m_count times
            for (int i = 0; i < m_count && !m_ioc.stopped(); i++) {
                std::string subject = apply_count_template(m_topic, ++m_msg_number);
                co_await publish_message(subject, m_data);
                if (m_sleep_ms > 0 && i < m_count - 1) {
                    asio::steady_timer timer(co_await asio::this_coro::executor);
                    timer.expires_after(std::chrono::milliseconds(m_sleep_ms));
                    co_await timer.async_wait(asio::use_awaitable);
                }
            }
        } else if (!m_src_cfg.http_url.empty()) {
            // Read from HTTP source
            m_http_reader = std::make_unique<async_http_reader>(m_ioc, m_src_cfg, m_log);
            if (!co_await m_http_reader->init()) {
                m_log->error("Failed to connect to HTTP source: {}", m_src_cfg.http_url);
                m_ioc.stop();
                co_return;
            }

            // Read lines from HTTP streaming response
            for (;;) {
                auto [line, eof, error] = co_await m_http_reader->read_line();

                if (error) {
                    m_log->error("HTTP read error");
                    break;
                }

                if (eof) {
                    m_log->info("HTTP stream ended");
                    break;
                }

                if (line.empty()) continue;

                co_await process_and_publish(line);

                if (m_sleep_ms > 0) {
                    asio::steady_timer timer(co_await asio::this_coro::executor);
                    timer.expires_after(std::chrono::milliseconds(m_sleep_ms));
                    co_await timer.async_wait(asio::use_awaitable);
                }
            }
        } else if (m_src_cfg.is_multi_file()) {
            // Multi-file reader with glob patterns
            auto patterns = m_src_cfg.get_patterns();
            m_multi_file_reader = std::make_unique<async_multi_file_reader>(
                m_ioc, patterns, m_src_cfg.follow, m_src_cfg.poll_interval_ms, m_log);

            if (!m_multi_file_reader->init()) {
                m_log->error("Failed to initialize multi-file reader");
                m_ioc.stop();
                co_return;
            }

            m_log->info("Reading from {} file(s) matching patterns", m_multi_file_reader->file_count());

            // Read from multiple files
            for (;;) {
                auto [line, file_path, eof, error] = co_await m_multi_file_reader->read_line();

                if (error) {
                    m_log->error("Multi-file read error");
                    break;
                }

                if (eof && !m_multi_file_reader->is_follow_mode()) {
                    // EOF reached on all files and not in follow mode
                    break;
                }

                if (line.empty()) continue;

                co_await process_and_publish(line);

                if (m_sleep_ms > 0) {
                    asio::steady_timer timer(co_await asio::this_coro::executor);
                    timer.expires_after(std::chrono::milliseconds(m_sleep_ms));
                    co_await timer.async_wait(asio::use_awaitable);
                }
            }
        } else {
            // Initialize input reader (file/stdin)
            if (!m_input_reader.init()) {
                m_log->error("Failed to initialize input reader");
                m_ioc.stop();
                co_return;
            }

            // Read from stdin or single file
            for (;;) {
                auto [line, eof, error] = co_await m_input_reader.read_line();

                if (error) {
                    m_log->error("Input read error");
                    break;
                }

                if (eof && !m_input_reader.is_follow_mode()) {
                    // EOF reached and not in follow mode - stop reading
                    break;
                }

                if (line.empty()) continue;

                co_await process_and_publish(line);

                if (m_sleep_ms > 0) {
                    asio::steady_timer timer(co_await asio::this_coro::executor);
                    timer.expires_after(std::chrono::milliseconds(m_sleep_ms));
                    co_await timer.async_wait(asio::use_awaitable);
                }
            }
        }

        // Wait for all in-flight publishes to complete
        if (m_jetstream && m_wait_for_ack && m_js_window) {
            // Use sliding window tracking
            m_log->info("Input complete, waiting for {} in-flight publishes (sliding window)",
                       m_js_window->in_flight_count());
            while (m_js_window->in_flight_count() > 0) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(50));
                co_await timer.async_wait(asio::use_awaitable);
            }
            m_log->info("Sliding window stats: acked={}, failed={}, timeouts={}, retries={}",
                       m_js_window->acked_count(), m_js_window->failed_count(),
                       m_js_window->timeout_count(), m_js_window->retry_count());
        } else {
            // Use regular in-flight tracking
            m_log->info("Input complete, waiting for {} in-flight publishes", m_in_flight.load());
            while (m_in_flight > 0) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(50));
                co_await timer.async_wait(asio::use_awaitable);
            }
        }

        m_log->info("All publishes complete, stopping");
        m_ioc.stop();
        co_return;
    }

private:
    // Replace {{Count}} placeholder with message number
    std::string apply_count_template(const std::string& tpl, std::size_t count) {
        std::string result = tpl;
        std::string placeholder = "{{Count}}";
        size_t pos = result.find(placeholder);
        while (pos != std::string::npos) {
            result.replace(pos, placeholder.length(), std::to_string(count));
            pos = result.find(placeholder, pos + 1);
        }
        return result;
    }

    asio::awaitable<void> process_and_publish(const std::string& line) {
        std::string subject = m_topic;
        std::string payload = line;

        if (m_input_cfg.format == input_format::json) {
            try {
                auto obj = nlohmann::json::parse(line);

                // Apply subject template if provided
                if (!m_input_cfg.subject_template.empty()) {
                    subject = apply_template(m_input_cfg.subject_template, obj);
                }

                // Build payload from selected fields
                payload = build_payload(obj, m_input_cfg.payload_fields);
            } catch (const nlohmann::json::exception& e) {
                m_log->warn("JSON parse error: {} - line: {}", e.what(), line.substr(0, 50));
                co_return;
            }
        } else if (m_input_cfg.format == input_format::csv) {
            if (m_input_cfg.csv_headers.empty()) {
                m_log->error("CSV format requires --csv_headers");
                co_return;
            }

            auto obj = parse_csv_line(line, m_input_cfg.csv_headers);

            // Apply subject template if provided
            if (!m_input_cfg.subject_template.empty()) {
                subject = apply_template(m_input_cfg.subject_template, obj);
            }

            // Build payload from selected fields
            payload = build_payload(obj, m_input_cfg.payload_fields);
        }
        // else: line format - use line as-is with m_topic

        // Apply {{Count}} placeholder to subject
        subject = apply_count_template(subject, ++m_msg_number);

        // Serialize to binary format if configured
        if (m_format) {
            auto binary_result = serialize_from_json(payload, *m_format, m_log);
            if (binary_result) {
                std::string binary_payload(reinterpret_cast<const char*>(binary_result->data()),
                                          binary_result->size());
                co_await publish_message(subject, binary_payload);
                co_return;
            } else {
                m_log->warn("Failed to serialize payload to binary format, publishing as-is");
            }
        }

        co_await publish_message(subject, payload);
    }

    asio::awaitable<void> publish_message(const std::string& payload) {
        co_await publish_message(apply_count_template(m_topic, ++m_msg_number), payload);
    }

    asio::awaitable<void> publish_message(const std::string& subject, const std::string& payload) {
        // Use sliding window for JetStream with ACK tracking
        if (m_jetstream && m_wait_for_ack && m_js_window) {
            // Backpressure: wait for space in sliding window
            co_await m_js_window->wait_for_space(m_ioc);

            auto conn = get_next_connection();
            auto msg = std::make_shared<std::string>(payload);
            auto subj = std::make_shared<std::string>(subject);

            // Get nonce and track message - use shared_ptr like other variables
            auto nonce = std::make_shared<std::string>(m_js_window->get_next_nonce());
            m_js_window->track_message(*nonce, subject, payload, m_js_stream);

            // Spawn coroutine to publish with nonce tracking (like old code)
            asio::co_spawn(
                m_ioc,
                [this, conn, subj, msg, nonce]() -> asio::awaitable<void> {
                    auto window = m_js_window;  // Get window from member variable
                    std::span<const char> payload_span(msg->data(), msg->size());

                    nats_asio::status s;
                    nats_asio::js_pub_ack ack_response;
                    try {
                        // Don't send Nats-Msg-Id header - it causes disconnects
                        // Use nonce only for internal tracking
                        if (m_headers.empty()) {
                            auto [ack, status] = co_await conn->js_publish(
                                *subj, payload_span, m_js_timeout, true);
                            s = status;
                            ack_response = ack;
                        } else {
                            auto [ack, status] = co_await conn->js_publish(
                                *subj, payload_span, m_headers, m_js_timeout, true);
                            s = status;
                            ack_response = ack;
                        }

                        if (s.ok()) {
                            if (!ack_response.stream.empty()) {
                                window->mark_acked(*nonce);
                                m_counter++;
                            } else {
                                // Still mark as acked to avoid timeout
                                window->mark_acked(*nonce);
                                m_counter++;
                            }
                        } else {
                            // Remove from window on failure (not counted as acked)
                            window->mark_acked(*nonce, false);
                        }
                    } catch (const std::exception& e) {
                        m_log->error("Exception during publish for subject '{}': {} (nonce: {})",
                                   *subj, e.what(), *nonce);
                        window->mark_acked(*nonce, false);
                    }
                    co_return;
                },
                asio::detached);

            co_return;
        }

        // Original implementation for non-JetStream or fire-and-forget mode
        // Backpressure: wait if too many publishes in flight
        while (m_in_flight >= m_max_in_flight) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(5));
            co_await timer.async_wait(asio::use_awaitable);
        }

        auto conn = get_next_connection();
        auto msg = std::make_shared<std::string>(payload);
        auto subj = std::make_shared<std::string>(subject);

        m_in_flight++;

        asio::co_spawn(
            m_ioc,
            [this, conn, subj, msg]() -> asio::awaitable<void> {
                std::span<const char> payload_span(msg->data(), msg->size());
                nats_asio::status s;

                if (m_jetstream) {
                    if (m_headers.empty()) {
                        auto [ack, status] = co_await conn->js_publish(*subj, payload_span, m_js_timeout, m_wait_for_ack);
                        s = status;
                    } else {
                        auto [ack, status] = co_await conn->js_publish(*subj, payload_span, m_headers, m_js_timeout, m_wait_for_ack);
                        s = status;
                    }
                } else {
                    std::optional<nats_asio::string_view> reply_opt = m_reply_to.empty()
                        ? std::nullopt
                        : std::optional<nats_asio::string_view>(m_reply_to);
                    if (m_headers.empty()) {
                        s = co_await conn->publish(*subj, payload_span, reply_opt);
                    } else {
                        s = co_await conn->publish(*subj, payload_span, m_headers, reply_opt);
                    }
                }

                if (s.failed()) {
                    m_log->error("publish failed: {}", s.error());
                } else {
                    m_counter++;
                }
                m_in_flight--;
                co_return;
            },
            asio::detached);

        co_return;
    }

private:
    bool has_connected_connection() const {
        for (const auto& conn : m_connections) {
            if (conn->is_connected()) {
                return true;
            }
        }
        return false;
    }

    nats_asio::iconnection_sptr get_next_connection() {
        std::size_t attempts = 0;
        while (attempts < m_connections.size()) {
            auto idx = m_next_conn.fetch_add(1, std::memory_order_relaxed) % m_connections.size();
            auto conn = m_connections[idx];
            if (conn->is_connected()) {
                return conn;
            }
            attempts++;
        }
        // Fallback to first connection (shouldn't happen if has_connected_connection passed)
        return m_connections[0];
    }

    std::vector<nats_asio::iconnection_sptr> m_connections;
    std::string m_topic;
    std::atomic<std::size_t> m_next_conn;
    std::atomic<int> m_in_flight;
    int m_max_in_flight;
    bool m_jetstream;
    std::chrono::milliseconds m_js_timeout;
    bool m_wait_for_ack;
    nats_asio::headers_t m_headers;
    std::string m_reply_to;
    int m_count;
    int m_sleep_ms;
    std::string m_data;
    input_config m_input_cfg;
    input_source_config m_src_cfg;
    std::optional<binary_format> m_format;
    std::size_t m_msg_number = 0;
    async_input_reader m_input_reader;
    std::unique_ptr<async_http_reader> m_http_reader;
    std::unique_ptr<async_multi_file_reader> m_multi_file_reader;

    // JetStream sliding window ACK tracking
    std::size_t m_js_window_size;
    std::shared_ptr<nats_tool::js_sliding_window> m_js_window;
    std::unique_ptr<nats_tool::js_ack_processor> m_ack_processor;
    std::string m_js_stream;
    bool m_js_create_stream;
    int m_js_max_retries;

    // Strand for thread-safe multi-threaded execution
    asio::strand<asio::io_context::executor_type> m_strand;
};

} // namespace nats_tool
