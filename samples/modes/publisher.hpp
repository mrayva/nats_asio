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
#include <deque>
#include <memory>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <random>

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

        if (m_jetstream && m_wait_for_ack) {
            auto s = co_await init_js_ack_pipeline();
            if (s.failed()) {
                m_log->error("Failed to initialize JetStream ACK pipeline: {}", s.error());
                m_ioc.stop();
                co_return;
            }
            asio::co_spawn(m_strand, ack_timeout_loop(), asio::detached);
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
        // Use regular in-flight tracking
        m_log->info("Input complete, waiting for {} in-flight publishes", m_in_flight.load());
        while (m_in_flight > 0) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(asio::use_awaitable);
        }
        if (m_jetstream && m_wait_for_ack) {
            m_log->info("Input complete, waiting for {} in-flight ACKs", m_ack_pending.load());
            while (m_ack_pending > 0) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(10));
                co_await timer.async_wait(asio::use_awaitable);
            }
            m_ack_pipeline_stop.store(true, std::memory_order_release);
        }
        if (m_jetstream && m_wait_for_ack) {
            m_log->info("ACK mode stats: acked={}, failed={}, retries={}",
                        m_ack_success_total.load(std::memory_order_relaxed),
                        m_ack_failures.load(std::memory_order_relaxed),
                        m_ack_retries.load(std::memory_order_relaxed));
        }

        // Ensure queued writes are flushed before shutdown in no-ack JS mode.
        if (m_jetstream && !m_wait_for_ack) {
            for (auto& conn : m_connections) {
                auto s = co_await conn->flush();
                if (s.failed()) {
                    m_log->warn("flush failed during shutdown: {}", s.error());
                }
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
        // JetStream ACK pipeline path: publish with reply token and process ACKs asynchronously.
        if (m_jetstream && m_wait_for_ack) {
            while (m_ack_pending >= static_cast<int>(m_js_window_size)) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            }

            auto [conn, conn_idx] = get_next_connection_with_index();
            if (!conn || conn_idx >= m_ack_conns.size()) {
                m_ack_failures.fetch_add(1, std::memory_order_relaxed);
                co_return;
            }

            auto subj = std::make_shared<std::string>(subject);
            auto msg = std::make_shared<std::string>(payload);
            auto token = m_ack_conns[conn_idx].next_token++;
            std::string reply = m_ack_conns[conn_idx].inbox_base;
            reply.push_back('.');
            reply += std::to_string(token);

            ack_publish_task task;
            task.conn = std::move(conn);
            task.conn_idx = conn_idx;
            task.token = token;
            task.reply_subject = std::move(reply);
            task.subject = std::move(subj);
            task.payload = std::move(msg);
            task.retry_count = 0;
            schedule_ack_publish(std::move(task));
            co_return;
        }

        // Hot path for JetStream fire-and-forget: avoid per-message co_spawn overhead.
        if (m_jetstream && !m_wait_for_ack) {
            auto conn = get_next_connection();
            std::span<const char> payload_span(payload.data(), payload.size());

            nats_asio::status s;
            if (m_headers.empty()) {
                auto [ack, status] =
                    co_await conn->js_publish(subject, payload_span, m_js_timeout, false);
                s = status;
            } else {
                auto [ack, status] = co_await conn->js_publish(
                    subject, payload_span, m_headers, m_js_timeout, false);
                s = status;
            }

            if (s.failed()) {
                m_log->error("publish failed: {}", s.error());
            } else {
                m_counter++;
            }
            co_return;
        }

        // Original implementation for non-JetStream or fire-and-forget mode
        // Backpressure: wait if too many publishes in flight
        auto in_flight_limit = m_max_in_flight;
        if (m_jetstream && m_wait_for_ack) {
            in_flight_limit = std::max(m_max_in_flight, static_cast<int>(m_js_window_size));
        }
        while (m_in_flight >= in_flight_limit) {
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
                    int attempts = 1;
                    if (m_wait_for_ack && m_js_max_retries > 0) {
                        attempts += m_js_max_retries;
                    }
                    for (int attempt = 0; attempt < attempts; ++attempt) {
                        if (m_headers.empty()) {
                            auto [ack, status] = co_await conn->js_publish(*subj, payload_span, m_js_timeout, m_wait_for_ack);
                            s = status;
                        } else {
                            auto [ack, status] = co_await conn->js_publish(*subj, payload_span, m_headers, m_js_timeout, m_wait_for_ack);
                            s = status;
                        }
                        if (s.ok()) {
                            break;
                        }
                        if (m_wait_for_ack && attempt + 1 < attempts) {
                            m_ack_retries.fetch_add(1, std::memory_order_relaxed);
                        }
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
                    if (m_jetstream && m_wait_for_ack) {
                        m_ack_failures.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        m_log->error("publish failed: {}", s.error());
                    }
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

    std::pair<nats_asio::iconnection_sptr, std::size_t> get_next_connection_with_index() {
        std::size_t attempts = 0;
        while (attempts < m_connections.size()) {
            auto idx = m_next_conn.fetch_add(1, std::memory_order_relaxed) % m_connections.size();
            auto conn = m_connections[idx];
            if (conn->is_connected()) {
                return {conn, idx};
            }
            attempts++;
        }
        return {m_connections[0], 0};
    }

    struct ack_conn_state {
        nats_asio::iconnection_sptr conn;
        std::string inbox_base;
        uint64_t next_token{1};
        nats_asio::isubscription_sptr sub;
    };

    struct ack_publish_task {
        nats_asio::iconnection_sptr conn;
        std::size_t conn_idx = 0;
        uint64_t token = 0;
        std::string reply_subject;
        std::shared_ptr<std::string> subject;
        std::shared_ptr<std::string> payload;
        int retry_count = 0;
    };

    struct ack_pending_entry {
        ack_publish_task task;
        std::chrono::steady_clock::time_point sent_at;
    };

    std::string build_ack_inbox_base(std::size_t conn_idx) {
        std::uniform_int_distribution<uint64_t> dist;
        std::string base("_INBOX.NATS_TOOL_ACK.");
        base += std::to_string(dist(m_ack_rng));
        base.push_back('.');
        base += std::to_string(conn_idx);
        return base;
    }

    void on_ack_message(std::size_t conn_idx, nats_asio::string_view subject,
                        std::span<const char> data) {
        if (conn_idx >= m_ack_conns.size()) {
            return;
        }
        const auto& base = m_ack_conns[conn_idx].inbox_base;
        if (subject.size() <= base.size() + 1 ||
            subject.substr(0, base.size()) != base ||
            subject[base.size()] != '.') {
            return;
        }

        auto token_sv = subject.substr(base.size() + 1);
        uint64_t token = 0;
        auto [end, ec] = std::from_chars(token_sv.data(), token_sv.data() + token_sv.size(), token);
        if (ec != std::errc{} || end != token_sv.data() + token_sv.size()) {
            return;
        }

        bool success = true;
        nats_asio::string_view payload_sv(data.data(), data.size());
        if (payload_sv.find("\"error\"") != nats_asio::string_view::npos) {
            success = false;
        }

        {
            std::lock_guard<std::mutex> lock(m_ack_pending_mutexes[conn_idx]);
            auto& pending = m_ack_pending_maps[conn_idx];
            auto it = pending.find(token);
            if (it == pending.end()) {
                return;  // Timed out or already handled.
            }
            pending.erase(it);
        }
        m_ack_pending.fetch_sub(1, std::memory_order_relaxed);
        if (success) {
            m_counter.fetch_add(1, std::memory_order_relaxed);
            m_ack_success_total.fetch_add(1, std::memory_order_relaxed);
        } else {
            m_ack_failures.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void schedule_ack_publish(ack_publish_task task) {
        if (m_ack_pipeline_stop.load(std::memory_order_acquire)) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(m_ack_pending_mutexes[task.conn_idx]);
            m_ack_pending_maps[task.conn_idx][task.token] =
                ack_pending_entry{task, std::chrono::steady_clock::now()};
        }
        m_ack_pending.fetch_add(1, std::memory_order_relaxed);
        m_in_flight.fetch_add(1, std::memory_order_relaxed);

        if (m_headers.empty()) {
            std::span<const char> payload_span(task.payload->data(), task.payload->size());
            std::optional<nats_asio::string_view> reply_to(task.reply_subject);
            auto s = task.conn->publish_queued(*task.subject, payload_span, reply_to);
            if (s.failed()) {
                handle_ack_publish_failure(std::move(task));
            }
            m_in_flight.fetch_sub(1, std::memory_order_relaxed);
            return;
        }

        m_ack_send_queues[task.conn_idx].push_back(std::move(task));
        if (!m_ack_sender_running[task.conn_idx]) {
            m_ack_sender_running[task.conn_idx] = true;
            asio::co_spawn(m_strand, ack_send_loop(task.conn_idx), asio::detached);
        }
    }

    void handle_ack_publish_failure(ack_publish_task task) {
        bool removed = false;
        {
            std::lock_guard<std::mutex> lock(m_ack_pending_mutexes[task.conn_idx]);
            auto& pending = m_ack_pending_maps[task.conn_idx];
            auto it = pending.find(task.token);
            if (it != pending.end()) {
                pending.erase(it);
                removed = true;
            }
        }

        if (!removed) {
            return;
        }

        m_ack_pending.fetch_sub(1, std::memory_order_relaxed);
        if (task.retry_count < m_js_max_retries) {
            task.retry_count++;
            m_ack_retries.fetch_add(1, std::memory_order_relaxed);
            schedule_ack_publish(std::move(task));
        } else {
            m_ack_failures.fetch_add(1, std::memory_order_relaxed);
        }
    }

    asio::awaitable<void> ack_send_loop(std::size_t conn_idx) {
        while (!m_ack_pipeline_stop.load(std::memory_order_acquire)) {
            auto& queue = m_ack_send_queues[conn_idx];
            if (queue.empty()) {
                break;
            }

            auto task = std::move(queue.front());
            queue.pop_front();

            std::span<const char> payload_span(task.payload->data(), task.payload->size());
            std::optional<nats_asio::string_view> reply_to(task.reply_subject);

            nats_asio::status s;
            if (m_headers.empty()) {
                s = co_await task.conn->publish(*task.subject, payload_span, reply_to);
            } else {
                s = co_await task.conn->publish(*task.subject, payload_span, m_headers, reply_to);
            }

            if (s.failed()) {
                handle_ack_publish_failure(std::move(task));
            }

            m_in_flight.fetch_sub(1, std::memory_order_relaxed);
        }

        m_ack_sender_running[conn_idx] = false;
        co_return;
    }

    asio::awaitable<nats_asio::status> init_js_ack_pipeline() {
        m_ack_conns.clear();
        m_ack_conns.resize(m_connections.size());
        m_ack_pending_maps.clear();
        m_ack_pending_maps.resize(m_connections.size());
        m_ack_pending_mutexes.clear();
        m_ack_pending_mutexes.resize(m_connections.size());
        m_ack_send_queues.clear();
        m_ack_send_queues.resize(m_connections.size());
        m_ack_sender_running.clear();
        m_ack_sender_running.resize(m_connections.size(), false);

        for (std::size_t i = 0; i < m_connections.size(); ++i) {
            auto& state = m_ack_conns[i];
            state.conn = m_connections[i];
            state.inbox_base = build_ack_inbox_base(i);
            std::string filter = state.inbox_base + ".*";

            auto [sub, s] = co_await state.conn->subscribe(
                filter,
                [this, i](nats_asio::string_view subject,
                          std::optional<nats_asio::string_view> /*reply*/,
                          std::span<const char> data) -> asio::awaitable<void> {
                    on_ack_message(i, subject, data);
                    co_return;
                });
            if (s.failed()) {
                co_return s;
            }
            state.sub = sub;
        }

        co_return nats_asio::status();
    }

    asio::awaitable<void> ack_timeout_loop() {
        while (!m_ack_pipeline_stop.load(std::memory_order_acquire)) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(10));
            co_await timer.async_wait(asio::use_awaitable);

            auto now = std::chrono::steady_clock::now();
            std::vector<ack_publish_task> retry_tasks;
            for (std::size_t i = 0; i < m_ack_pending_maps.size(); ++i) {
                std::lock_guard<std::mutex> lock(m_ack_pending_mutexes[i]);
                auto& pending = m_ack_pending_maps[i];
                for (auto it = pending.begin(); it != pending.end();) {
                    if (now - it->second.sent_at > m_js_timeout) {
                        auto task = it->second.task;
                        it = pending.erase(it);
                        m_ack_pending.fetch_sub(1, std::memory_order_relaxed);
                        if (task.retry_count < m_js_max_retries) {
                            task.retry_count++;
                            m_ack_retries.fetch_add(1, std::memory_order_relaxed);
                            retry_tasks.push_back(std::move(task));
                        } else {
                            m_ack_failures.fetch_add(1, std::memory_order_relaxed);
                        }
                    } else {
                        ++it;
                    }
                }
            }

            for (auto& task : retry_tasks) {
                schedule_ack_publish(std::move(task));
            }
        }
        co_return;
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

    std::size_t m_js_window_size;
    std::string m_js_stream;
    bool m_js_create_stream;
    int m_js_max_retries;
    std::atomic<uint64_t> m_ack_failures{0};
    std::atomic<uint64_t> m_ack_retries{0};
    std::atomic<uint64_t> m_ack_success_total{0};
    std::atomic<int> m_ack_pending{0};
    std::atomic<bool> m_ack_pipeline_stop{false};
    std::vector<ack_conn_state> m_ack_conns;
    std::vector<std::unordered_map<uint64_t, ack_pending_entry>> m_ack_pending_maps;
    std::deque<std::mutex> m_ack_pending_mutexes;
    std::vector<std::deque<ack_publish_task>> m_ack_send_queues;
    std::vector<bool> m_ack_sender_running;
    std::mt19937_64 m_ack_rng{std::random_device{}()};

    // Strand for thread-safe multi-threaded execution
    asio::strand<asio::io_context::executor_type> m_strand;
};

} // namespace nats_tool
