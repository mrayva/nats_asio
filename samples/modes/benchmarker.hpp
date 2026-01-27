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

class benchmarker : public worker {
public:
    benchmarker(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                std::vector<nats_asio::iconnection_sptr> connections, const std::string& topic,
                int stats_interval, int msg_count, int msg_size, bool jetstream,
                bool request_reply = false, int timeout_ms = 5000, int batch_size = 1000)
        : worker(ioc, console, stats_interval), m_connections(std::move(connections)),
          m_topic(topic), m_msg_count(msg_count), m_msg_size(msg_size),
          m_jetstream(jetstream), m_request_reply(request_reply),
          m_timeout(std::chrono::milliseconds(timeout_ms)), m_batch_size(batch_size),
          m_start_time(std::chrono::steady_clock::now()) {
        // Generate payload of specified size
        m_payload.resize(msg_size, 'x');
        // Reserve space for latencies if measuring round-trip
        if (m_request_reply) {
            m_latencies.reserve(msg_count);
        }
        // Pre-format single PUB command for pipelined mode
        m_pub_cmd = fmt::format("PUB {} {}\r\n", m_topic, m_msg_size);
    }

    asio::awaitable<void> run() {
        // Wait for at least one connection
        while (!has_connected_connection()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        std::string mode_str = m_request_reply ? "request-reply" :
                               (m_jetstream ? "jetstream" :
                               (m_batch_size > 1 ? "pipelined" : "publish"));
        m_log->info("Starting benchmark: {} messages, {} bytes, {} connections, mode={}{}",
                   m_msg_count, m_msg_size, m_connections.size(), mode_str,
                   (m_batch_size > 1 && !m_request_reply && !m_jetstream) ?
                   fmt::format(", batch={}", m_batch_size) : "");
        m_start_time = std::chrono::steady_clock::now();

        if (m_request_reply) {
            co_await run_request_reply();
        } else if (m_jetstream) {
            co_await run_jetstream();
        } else if (m_batch_size > 1) {
            co_await run_pipelined();
        } else {
            co_await run_sequential();
        }

        auto end_time = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - m_start_time).count();
        double duration_sec = duration_ms / 1000.0;

        std::size_t total_msgs = m_counter;
        std::size_t total_bytes = total_msgs * m_msg_size;
        double msgs_per_sec = duration_sec > 0 ? total_msgs / duration_sec : 0;
        double mb_per_sec = duration_sec > 0 ? (total_bytes / 1024.0 / 1024.0) / duration_sec : 0;

        m_log->info("Benchmark complete:");
        m_log->info("  Messages: {} in {:.2f}s", total_msgs, duration_sec);
        m_log->info("  Throughput: {:.0f} msgs/sec, {:.2f} MB/sec", msgs_per_sec, mb_per_sec);

        if (m_request_reply && !m_latencies.empty()) {
            // Calculate percentiles
            std::sort(m_latencies.begin(), m_latencies.end());
            auto p50 = percentile(50);
            auto p95 = percentile(95);
            auto p99 = percentile(99);
            auto avg = std::accumulate(m_latencies.begin(), m_latencies.end(), 0LL) / static_cast<double>(m_latencies.size());
            auto min_lat = m_latencies.front();
            auto max_lat = m_latencies.back();

            m_log->info("  Latency (round-trip):");
            m_log->info("    min: {:.3f} ms, max: {:.3f} ms, avg: {:.3f} ms",
                       min_lat / 1000.0, max_lat / 1000.0, avg / 1000.0);
            m_log->info("    p50: {:.3f} ms, p95: {:.3f} ms, p99: {:.3f} ms",
                       p50 / 1000.0, p95 / 1000.0, p99 / 1000.0);
        } else {
            m_log->info("  Avg latency: {:.3f} ms/msg", duration_sec > 0 ? (duration_ms / static_cast<double>(total_msgs)) : 0);
        }

        m_ioc.stop();
        co_return;
    }

private:
    asio::awaitable<void> run_pipelined() {
        // Build batches of PUB commands and write them using write_raw
        std::string batch_buffer;
        // Pre-calculate single message size: "PUB topic len\r\npayload\r\n"
        size_t single_msg_size = m_pub_cmd.size() + m_payload.size() + 2; // +2 for \r\n after payload
        batch_buffer.reserve(single_msg_size * m_batch_size);

        int remaining = m_msg_count;
        while (remaining > 0 && !m_ioc.stopped()) {
            int batch_count = std::min(remaining, m_batch_size);
            batch_buffer.clear();

            // Build batch of PUB commands
            for (int i = 0; i < batch_count; i++) {
                batch_buffer += m_pub_cmd;
                batch_buffer += m_payload;
                batch_buffer += "\r\n";
            }

            // Write batch using write_raw
            auto conn = get_next_connection();
            std::span<const char> batch_span(batch_buffer.data(), batch_buffer.size());
            auto s = co_await conn->write_raw(batch_span);

            if (s.failed()) {
                m_log->error("write_raw failed: {}", s.error());
            } else {
                m_counter += batch_count;
            }

            remaining -= batch_count;
        }
        co_return;
    }

    asio::awaitable<void> run_sequential() {
        std::span<const char> payload_span(m_payload.data(), m_payload.size());
        for (int i = 0; i < m_msg_count && !m_ioc.stopped(); i++) {
            auto conn = get_next_connection();
            auto s = co_await conn->publish(m_topic, payload_span, std::nullopt);
            if (s.failed()) {
                m_log->error("publish failed: {}", s.error());
            } else {
                m_counter++;
            }
        }
        co_return;
    }

    asio::awaitable<void> run_jetstream() {
        std::span<const char> payload_span(m_payload.data(), m_payload.size());
        for (int i = 0; i < m_msg_count && !m_ioc.stopped(); i++) {
            auto conn = get_next_connection();
            auto s = co_await conn->js_publish_async(m_topic, payload_span);
            if (s.failed()) {
                m_log->error("js_publish failed: {}", s.error());
            } else {
                m_counter++;
            }
        }
        co_return;
    }

    asio::awaitable<void> run_request_reply() {
        std::span<const char> payload_span(m_payload.data(), m_payload.size());
        for (int i = 0; i < m_msg_count && !m_ioc.stopped(); i++) {
            auto conn = get_next_connection();
            auto start = std::chrono::steady_clock::now();
            auto [reply, status] = co_await conn->request(m_topic, payload_span, m_timeout);
            auto end = std::chrono::steady_clock::now();

            if (status.failed()) {
                m_log->error("request failed: {}", status.error());
            } else {
                auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                m_latencies.push_back(latency_us);
                m_counter++;
            }
        }
        co_return;
    }

    bool has_connected_connection() const {
        for (const auto& conn : m_connections) {
            if (conn->is_connected()) return true;
        }
        return false;
    }

    nats_asio::iconnection_sptr get_next_connection() {
        auto conn = m_connections[m_next_conn % m_connections.size()];
        m_next_conn++;
        return conn;
    }

    long long percentile(int p) const {
        if (m_latencies.empty()) return 0;
        size_t idx = (p * m_latencies.size()) / 100;
        if (idx >= m_latencies.size()) idx = m_latencies.size() - 1;
        return m_latencies[idx];
    }

    std::vector<nats_asio::iconnection_sptr> m_connections;
    std::string m_topic;
    int m_msg_count;
    int m_msg_size;
    bool m_jetstream;
    bool m_request_reply;
    std::chrono::milliseconds m_timeout;
    int m_batch_size;
    std::string m_payload;
    std::string m_pub_cmd;
    std::chrono::steady_clock::time_point m_start_time;
    std::vector<long long> m_latencies;
    std::size_t m_next_conn = 0;
};

} // namespace nats_tool
