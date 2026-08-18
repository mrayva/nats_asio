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

#include "../include/js_ack_pipeline.hpp"
#include "../include/worker.hpp"
#include <nats_asio/nats_asio.hpp>
#include <nats_asio/input_reader.hpp>
#include <asio/awaitable.hpp>
#include <asio/detached.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <cassert>
#include <string>
#include <chrono>
#include <memory>
#include <vector>

namespace nats_tool {

class kv_publisher : public worker {
public:
    // Single-connection constructor - kept so every mode this can be used
    // in (currently just pubkv) doesn't have to build a vector for the
    // common (and default) --connections 1 case.
    kv_publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                 nats_asio::iconnection_sptr conn, const std::string& bucket,
                 int stats_interval, int max_in_flight, const std::string& separator,
                 int kv_timeout_ms)
        : kv_publisher(ioc, console, std::vector<nats_asio::iconnection_sptr>{std::move(conn)}, {},
                       bucket, stats_interval, max_in_flight, separator, kv_timeout_ms) {}

    // Round-robins each KV operation across `connections`, mirroring
    // publisher::get_next_connection_with_index() - same skip-if-
    // disconnected retry, same relaxed-atomic index.
    //
    // `shards`, if non-empty, must be the same size as `connections` and
    // is indexed in lockstep with it: shards[i] is the dedicated
    // io_context connections[i] was constructed on (mirroring pub mode's
    // io_shards - one connection, one io_context, one OS thread running
    // it). kv_delete (the only operation still dispatched as an
    // individual coroutine - see handle_line()) co_spawns onto *its*
    // connection's own shard, so the connection is only ever touched
    // from the single thread that owns it for that path - no
    // cross-thread strand hand-off.
    //
    // kv_put itself no longer needs shard dispatch at all: it goes
    // through m_ack_pipeline, which fires via publish_queued() - a
    // lock-based, thread-safe fast path connections expose specifically
    // so callers don't need strand affinity to use it. See
    // js_ack_pipeline.hpp's doc comment for the full rationale (this
    // pipeline is shared with publisher.hpp's --js --wait_for_ack path;
    // ~2x the throughput of the older per-message-coroutine design this
    // class used to have).
    kv_publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                 std::vector<nats_asio::iconnection_sptr> connections,
                 std::vector<std::shared_ptr<asio::io_context>> shards, const std::string& bucket,
                 int stats_interval, int max_in_flight, const std::string& separator,
                 int kv_timeout_ms)
        : worker(ioc, console, stats_interval), m_connections(connections),
          m_ack_pipeline(m_log, std::move(connections), ioc.get_executor(), kDefaultMaxRetries,
                         std::chrono::milliseconds(kv_timeout_ms)),
          m_shards(std::move(shards)), m_bucket(bucket), m_delete_in_flight(0),
          m_max_in_flight(max_in_flight), m_separator(separator),
          m_kv_timeout(std::chrono::milliseconds(kv_timeout_ms)),
          m_input_reader(ioc, nats_asio::input_source_config{}, console), m_next_conn(0) {
        assert(m_shards.empty() || m_shards.size() == m_connections.size());
        m_ack_pipeline.on_success = [this](const js_ack_task&) { m_counter++; };
        m_ack_pipeline.on_failure = [this](const js_ack_task& task, const std::string& reason) {
            m_log->error("kv_put failed for key '{}': {}", task.tag, reason);
        };
        asio::co_spawn(ioc, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        // m_ack_pipeline.init() subscribes on every connection in
        // m_connections, not just one - unlike handle_line()'s per-op
        // wait (any one connection is enough to make progress), this
        // needs all of them up first, or subscribe() fails outright on
        // whichever hasn't finished connecting yet.
        while (!has_all_connections_connected()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        auto init_status = co_await m_ack_pipeline.init();
        if (init_status.failed()) {
            m_log->error("Failed to initialize KV put ACK pipeline: {}", init_status.error());
            m_ioc.stop();
            co_return;
        }
        asio::co_spawn(m_ioc, m_ack_pipeline.timeout_loop(), asio::detached);

        if (!m_input_reader.init()) {
            m_log->error("Failed to initialize stdin reader");
            m_ioc.stop();
            co_return;
        }

        // Buffered reader (same one publisher.hpp uses for its plain
        // "line" format): reads in 8KB chunks and extracts lines from an
        // in-memory buffer via find('\n')/substr, unlike the old
        // asio::async_read_until-per-line approach this replaced, which
        // reconstructed a whole composed async operation (plus a
        // std::istream/getline layer on top) for every single line -
        // profiled at ~6% of total CPU by itself under load.
        for (;;) {
            auto [line, eof, error] = co_await m_input_reader.read_line();
            if (error) {
                m_log->error("stdin read error");
                break;
            }
            if (!line.empty()) {
                co_await handle_line(line);
            }
            if (eof) {
                break;
            }
        }

        // Wait for all in-flight operations to complete
        m_log->info("EOF reached, waiting for {} in-flight KV operations", total_in_flight());
        while (total_in_flight() > 0) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(asio::use_awaitable);
        }
        m_ack_pipeline.stop();

        m_log->info("All KV operations complete, stopping");
        m_ioc.stop();
        co_return;
    }

private:
    // Same starting point as publisher mode's --js_max_retries default -
    // pubkv has no CLI flag of its own for this yet, so it just inherits
    // that default rather than adding one before it's needed.
    static constexpr int kDefaultMaxRetries = 3;

    asio::awaitable<void> handle_line(const std::string& line) {
        // Parse key|value - find first separator
        auto sep_pos = line.find(m_separator);
        if (sep_pos == std::string::npos) {
            m_log->error("invalid line format, missing separator '{}': {}", m_separator, line);
            co_return;
        }

        std::string key = line.substr(0, sep_pos);
        std::string value_part = line.substr(sep_pos + m_separator.size());

        if (key.empty()) {
            m_log->error("empty key in line: {}", line);
            co_return;
        }

        // Check if this is a delete operation (value starts with separator)
        bool is_delete = false;
        if (value_part.size() >= m_separator.size() &&
            value_part.substr(0, m_separator.size()) == m_separator) {
            is_delete = true;
        }

        // No per-line connection-readiness wait here: read_and_publish()
        // already waited for every connection before it started reading,
        // and get_next_connection() below has its own defensive skip-if-
        // disconnected retry - re-scanning all connections on every
        // single line on top of that was pure redundant overhead on the
        // hot path.
        auto [conn, idx] = get_next_connection();

        if (is_delete) {
            // kv_delete has no ack-pipeline equivalent (not the
            // throughput-critical path this was built for) - same
            // individual fire-and-await-inline shape as before, still on
            // this connection's own dedicated shard, with its own small
            // backpressure budget separate from puts.
            while (m_delete_in_flight >= m_max_in_flight) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(5));
                co_await timer.async_wait(asio::use_awaitable);
            }
            m_delete_in_flight++;

            auto key_copy = std::make_shared<std::string>(std::move(key));
            asio::co_spawn(
                shard_for(idx),
                [this, conn, key_copy]() -> asio::awaitable<void> {
                    auto [rev, s] = co_await conn->kv_delete(m_bucket, *key_copy, m_kv_timeout);
                    if (s.failed()) {
                        m_log->error("kv_delete failed for key '{}': {}", *key_copy, s.error());
                    } else {
                        m_counter++;
                        m_log->debug("deleted key '{}' rev={}", *key_copy, rev);
                    }
                    m_delete_in_flight--;
                    co_return;
                },
                asio::detached);
            co_return;
        }

        // kv_put: backpressure against the pipeline's own global pending
        // count (mirrors publisher mode's --js_window check), then fire
        // synchronously - schedule_put() is not a coroutine and doesn't
        // need shard dispatch, so this returns immediately with no
        // per-message co_spawn at all.
        while (m_ack_pipeline.pending() >= m_max_in_flight) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(1));
            co_await timer.async_wait(asio::use_awaitable);
        }

        std::string subject = nats_asio::subjects::kv_subject(m_bucket, key);
        m_ack_pipeline.schedule_put(idx, std::move(subject), std::move(value_part), std::move(key));
        co_return;
    }

    bool has_all_connections_connected() const {
        for (const auto& conn : m_connections) {
            if (!conn->is_connected()) {
                return false;
            }
        }
        return true;
    }

    std::pair<nats_asio::iconnection_sptr, std::size_t> get_next_connection() {
        std::size_t attempts = 0;
        while (attempts < m_connections.size()) {
            auto idx = m_next_conn.fetch_add(1, std::memory_order_relaxed) % m_connections.size();
            auto conn = m_connections[idx];
            if (conn->is_connected()) {
                return {conn, idx};
            }
            attempts++;
        }
        // Fallback to first connection (shouldn't happen if
        // has_connected_connection() passed) - same fallback
        // publisher::get_next_connection_with_index() uses.
        return {m_connections[0], 0};
    }

    // Dispatch onto connection idx's own dedicated shard when one
    // exists (used only by kv_delete now - see this class's constructor
    // doc for why kv_put no longer needs this). Falls back to the shared
    // m_ioc when there are no shards (single connection, or the
    // unsharded multi-connection path).
    asio::io_context& shard_for(std::size_t idx) {
        return m_shards.empty() ? m_ioc : *m_shards[idx];
    }

    // Only used for the EOF drain-wait log/poll, which happens once per
    // run (not per message).
    int total_in_flight() const {
        return m_ack_pipeline.pending() + m_delete_in_flight.load(std::memory_order_relaxed);
    }

    std::vector<nats_asio::iconnection_sptr> m_connections;
    js_ack_pipeline m_ack_pipeline;
    std::vector<std::shared_ptr<asio::io_context>> m_shards;
    std::string m_bucket;
    std::atomic<int> m_delete_in_flight;
    int m_max_in_flight;
    std::string m_separator;
    std::chrono::milliseconds m_kv_timeout;
    nats_asio::async_input_reader m_input_reader;
    std::atomic<std::size_t> m_next_conn;
};

} // namespace nats_tool
