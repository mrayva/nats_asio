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
#include "common.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <cassert>
#include <span>
#include <string>
#include <chrono>
#include <memory>
#include <unistd.h>
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
    // it). Each dispatched operation co_spawns onto *its* connection's
    // own shard, so the connection is only ever touched from the single
    // thread that owns it - no cross-thread strand hand-off.
    //
    // Left empty, every operation co_spawns onto the shared `ioc`
    // instead (single-connection default, or --threads servicing one
    // shared io_context) - measured to have a real, unresolved
    // oscillating-throughput pathology under multiple threads sharing
    // one io_context with multiple strand-bound connections (burst then
    // near-stall, repeating on a ~5s cycle - not a deadlock, confirmed
    // via gdb thread dumps mid-stall showing genuine in-progress work on
    // every thread, just badly paced). Use `shards` for real
    // multi-connection throughput; the no-shards path exists for the
    // single-connection case where there's only one strand and no
    // hand-off to have a problem with.
    kv_publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                 std::vector<nats_asio::iconnection_sptr> connections,
                 std::vector<std::shared_ptr<asio::io_context>> shards, const std::string& bucket,
                 int stats_interval, int max_in_flight, const std::string& separator,
                 int kv_timeout_ms)
        : worker(ioc, console, stats_interval), m_connections(std::move(connections)),
          m_shards(std::move(shards)),
          m_bucket(bucket), m_in_flight(m_connections.size()), m_max_in_flight(max_in_flight),
          m_separator(separator), m_kv_timeout(std::chrono::milliseconds(kv_timeout_ms)),
          m_stdin(ioc, ::dup(STDIN_FILENO)), m_next_conn(0) {
        assert(m_shards.empty() || m_shards.size() == m_connections.size());
        asio::co_spawn(ioc, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        co_await read_stdin_lines(m_stdin, m_log, [this](const std::string& line) {
            return handle_line(line);
        });

        // Wait for all in-flight operations to complete
        m_log->info("EOF reached, waiting for {} in-flight KV operations", total_in_flight());
        while (total_in_flight() > 0) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(asio::use_awaitable);
        }

        m_log->info("All KV operations complete, stopping");
        m_ioc.stop();
        co_return;
    }

private:
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

        // Wait until at least one connection is ready
        while (!has_connected_connection()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        // Pick this operation's connection once, up front - it keeps
        // using the same one for its whole lifetime, same as
        // publisher::get_next_connection_with_index()'s callers. Picked
        // before the backpressure check below so that check (and the
        // in-flight counter it gates) is per-connection, not shared
        // across every connection - a single counter incremented by the
        // main thread and decremented by whichever of N shard threads
        // happens to finish an op is a real, measured source of
        // cross-thread cache-line contention (confirmed via perf: a
        // meaningful chunk of __pv_queued_spin_lock_slowpath/
        // pthread_mutex_lock time). Splitting it per connection still
        // crosses threads (main increments, that connection's one shard
        // thread decrements), but only ever between those same two
        // threads instead of all of them fighting over one cache line.
        auto [conn, idx] = get_next_connection();

        // Backpressure: wait if too many operations in flight *on this
        // connection* - --max_in_flight is a per-connection cap, same as
        // it always effectively was (round-robin already spread load
        // across connections; this just makes the counter match that).
        while (m_in_flight[idx] >= m_max_in_flight) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(5));
            co_await timer.async_wait(asio::use_awaitable);
        }

        m_in_flight[idx]++;

        // Dispatch onto that connection's own dedicated shard when one
        // exists, so the connection is only ever touched from the single
        // thread that owns it - see this class's constructor doc for why
        // that matters. Falls back to the shared m_ioc when there are no
        // shards (single connection, or the unsharded multi-connection
        // path).
        asio::io_context& target_ioc = m_shards.empty() ? m_ioc : *m_shards[idx];

        // Capture data for async operation
        auto key_copy = std::make_shared<std::string>(std::move(key));
        auto value_copy = std::make_shared<std::string>(std::move(value_part));

        // Fire-and-forget: dispatch KV operation without waiting
        asio::co_spawn(
            target_ioc,
            [this, conn, idx, key_copy, value_copy, is_delete]() -> asio::awaitable<void> {
                if (is_delete) {
                    auto [rev, s] = co_await conn->kv_delete(m_bucket, *key_copy, m_kv_timeout);
                    if (s.failed()) {
                        m_log->error("kv_delete failed for key '{}': {}", *key_copy, s.error());
                    } else {
                        m_counter++;
                        m_log->debug("deleted key '{}' rev={}", *key_copy, rev);
                    }
                } else {
                    std::span<const char> value_span(value_copy->data(), value_copy->size());
                    auto [rev, s] = co_await conn->kv_put(m_bucket, *key_copy, value_span, m_kv_timeout);
                    if (s.failed()) {
                        m_log->error("kv_put failed for key '{}': {}", *key_copy, s.error());
                    } else {
                        m_counter++;
                        m_log->debug("put key '{}' rev={}", *key_copy, rev);
                    }
                }
                m_in_flight[idx]--;
                co_return;
            },
            asio::detached);

        co_return;
    }

    bool has_connected_connection() const {
        for (const auto& conn : m_connections) {
            if (conn->is_connected()) {
                return true;
            }
        }
        return false;
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

    // Only used for the EOF drain-wait log/poll, which happens once per
    // run (not per message) - summing every connection's counter here is
    // fine, unlike doing it on the hot per-message path.
    int total_in_flight() const {
        int total = 0;
        for (const auto& c : m_in_flight) {
            total += c.load(std::memory_order_relaxed);
        }
        return total;
    }

    std::vector<nats_asio::iconnection_sptr> m_connections;
    std::vector<std::shared_ptr<asio::io_context>> m_shards;
    std::string m_bucket;
    // One counter per connection, not one shared across all of them -
    // see handle_line()'s comment on get_next_connection() for why.
    std::vector<std::atomic<int>> m_in_flight;
    int m_max_in_flight;
    std::string m_separator;
    std::chrono::milliseconds m_kv_timeout;
    asio::posix::stream_descriptor m_stdin;
    std::atomic<std::size_t> m_next_conn;
};

} // namespace nats_tool
