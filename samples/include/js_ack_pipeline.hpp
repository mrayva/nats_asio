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

#include <nats_asio/nats_asio.hpp>
#include <asio/any_io_executor.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace nats_tool {

// A single "publish this, then track its JetStream ack" unit. Reused
// as-is across retries (same token/reply_subject), so a retry never
// needs a fresh reply subscription or a new slot lookup key.
struct js_ack_task {
    nats_asio::iconnection_sptr conn;
    std::size_t conn_idx = 0;
    uint64_t token = 0;
    std::string reply_subject;
    std::string subject;
    std::string payload;
    // Caller-defined context (e.g. the KV key) surfaced back on
    // on_success/on_failure for logging - the pipeline itself never reads
    // or interprets it.
    std::string tag;
    int retry_count = 0;
};

// Shared JetStream publish-with-ack pipeline, originally built for pub
// mode's --js --wait_for_ack path and extracted here so kv_publisher's
// KV put path can use the exact same mechanics instead of a separate,
// slower implementation.
//
// Deliberately NOT built on nats_asio's generic js_publish_impl/
// js_publish_fire (shared_ptr<js_ack_wait_state> + unordered_map +
// one asio::steady_timer per publish + a strand-redispatch coroutine
// hop for every call not already running on the connection's own
// strand object): profiling that path under load showed ~22% of total
// CPU cycles in pure allocator overhead (heap allocation for coroutine
// frames behind virtual calls can't be elided, plus the per-publish
// shared_ptr/unordered_map/timer). This pipeline instead:
//   - fires via publish_queued() - a lock-based, thread-safe, non-
//     coroutine fast path connections already expose specifically so
//     callers don't need strand affinity or a heap-allocated frame to
//     use it (falling back to an awaited publish() only when headers
//     are needed, since publish_queued() doesn't support them - drained
//     by one persistent per-connection loop, not spawned per message).
//   - matches each ack against a fixed-size, allocated-once slot array
//     keyed by token % slot_count, instead of a per-publish shared_ptr
//     + unordered_map insert/erase.
//   - sweeps for timeouts on a single periodic loop (timeout_loop(),
//     10ms tick) instead of one asio::steady_timer per publish.
// Measured (see nats_tool pub mode, 3M rows, 4 connections): ~278,000
// rows/s, roughly 2x the js_publish_impl-based path's ~139,000 rows/s
// on identical hardware/server.
class js_ack_pipeline {
public:
    js_ack_pipeline(std::shared_ptr<spdlog::logger> log,
                     std::vector<nats_asio::iconnection_sptr> connections,
                     asio::any_io_executor send_loop_executor, int max_retries,
                     std::chrono::milliseconds timeout,
                     const nats_asio::headers_t& headers = {})
        : m_log(std::move(log)), m_connections(std::move(connections)),
          m_send_loop_executor(std::move(send_loop_executor)), m_max_retries(max_retries),
          m_timeout(timeout), m_headers(headers), m_ack_conns(m_connections.size()),
          m_pending_slots(m_connections.size()), m_pending_mutexes(m_connections.size()),
          m_send_queues(m_connections.size()), m_sender_running(m_connections.size(), false) {
        // Sized generously and independent of any single caller's
        // --max_in_flight-style knob: collisions here just force an
        // (already-rare, already-handled) retry, not correctness loss,
        // but a too-small array makes collisions common at high
        // concurrency. Matches pub mode's original sizing.
        static constexpr std::size_t kSlotCount = 4096;
        for (auto& slots : m_pending_slots) {
            slots.resize(kSlotCount);
        }
    }

    // Subscribes each connection to its own ack inbox filter. Call once,
    // before scheduling any tasks.
    asio::awaitable<nats_asio::status> init() {
        for (std::size_t i = 0; i < m_connections.size(); ++i) {
            auto& state = m_ack_conns[i];
            state.inbox_base = build_ack_inbox_base(i);
            std::string filter = state.inbox_base + ".*";

            auto [sub, s] = co_await m_connections[i]->subscribe(
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

    // Convenience for callers that already know which connection they
    // want (round-robin already happened) and just need a subject/
    // payload published and tracked - builds the reply-subject/token
    // pair and calls schedule().
    void schedule_put(std::size_t conn_idx, std::string subject, std::string payload,
                       std::string tag = {}) {
        js_ack_task task;
        task.conn = m_connections[conn_idx];
        task.conn_idx = conn_idx;
        task.token = m_ack_conns[conn_idx].next_token++;
        task.reply_subject = m_ack_conns[conn_idx].inbox_base;
        task.reply_subject.push_back('.');
        task.reply_subject += std::to_string(task.token);
        task.subject = std::move(subject);
        task.payload = std::move(payload);
        task.tag = std::move(tag);
        schedule(std::move(task));
    }

    // Fires task.subject/task.payload now and registers it in the slot
    // array so its ack (or an eventual timeout) resolves asynchronously
    // via on_success/on_failure. Synchronous, no suspension - mirrors
    // publish_queued()'s own fire-and-return shape. Also the re-entry
    // point for retries (same task, incremented retry_count).
    void schedule(js_ack_task task) {
        if (m_stop.load(std::memory_order_acquire)) {
            return;
        }

        auto conn_idx = task.conn_idx;
        {
            std::lock_guard<std::mutex> lock(m_pending_mutexes[conn_idx]);
            auto& slots = m_pending_slots[conn_idx];
            auto idx = slot_index(conn_idx, task.token);
            auto& slot = slots[idx];
            if (slot.active && slot.token != task.token) {
                // Slot collision under extreme concurrency - the older
                // occupant hasn't been reaped yet. Rare enough (4096-wide
                // array) that just failing this task outright is fine.
                m_failure_total.fetch_add(1, std::memory_order_relaxed);
                if (on_failure) {
                    on_failure(task, "ack slot collision");
                }
                return;
            }
            slot.active = true;
            slot.token = task.token;
            slot.task = task;
            slot.sent_at = std::chrono::steady_clock::now();
        }
        m_pending.fetch_add(1, std::memory_order_relaxed);

        if (m_headers.empty()) {
            std::span<const char> payload_span(task.payload.data(), task.payload.size());
            std::optional<nats_asio::string_view> reply_to(task.reply_subject);
            auto s = task.conn->publish_queued(task.subject, payload_span, reply_to);
            if (s.failed()) {
                handle_failure(std::move(task));
            }
            return;
        }

        m_send_queues[conn_idx].push_back(std::move(task));
        if (!m_sender_running[conn_idx]) {
            m_sender_running[conn_idx] = true;
            asio::co_spawn(m_send_loop_executor, send_loop(conn_idx), asio::detached);
        }
    }

    // Periodic timeout sweep - co_spawn this once (anywhere; it only
    // touches mutex-protected state, so it's safe run from any thread)
    // and let it run until stop().
    asio::awaitable<void> timeout_loop() {
        while (!m_stop.load(std::memory_order_acquire)) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(10));
            co_await timer.async_wait(asio::use_awaitable);

            auto now = std::chrono::steady_clock::now();
            std::vector<js_ack_task> retry_tasks;
            for (std::size_t i = 0; i < m_pending_slots.size(); ++i) {
                std::lock_guard<std::mutex> lock(m_pending_mutexes[i]);
                for (auto& slot : m_pending_slots[i]) {
                    if (!slot.active) {
                        continue;
                    }
                    if (now - slot.sent_at > m_timeout) {
                        auto task = std::move(slot.task);
                        slot.active = false;
                        m_pending.fetch_sub(1, std::memory_order_relaxed);
                        if (task.retry_count < m_max_retries) {
                            task.retry_count++;
                            m_retry_total.fetch_add(1, std::memory_order_relaxed);
                            retry_tasks.push_back(std::move(task));
                        } else {
                            m_failure_total.fetch_add(1, std::memory_order_relaxed);
                            if (on_failure) {
                                on_failure(task, "ack timeout");
                            }
                        }
                    }
                }
            }
            for (auto& task : retry_tasks) {
                schedule(std::move(task));
            }
        }
        co_return;
    }

    void stop() { m_stop.store(true, std::memory_order_release); }

    int pending() const { return m_pending.load(std::memory_order_relaxed); }
    uint64_t success_total() const { return m_success_total.load(std::memory_order_relaxed); }
    uint64_t failure_total() const { return m_failure_total.load(std::memory_order_relaxed); }
    uint64_t retry_total() const { return m_retry_total.load(std::memory_order_relaxed); }

    // Invoked (from whichever connection's own thread resolved the ack -
    // not necessarily the caller's thread) on terminal outcomes only,
    // never on a retry.
    std::function<void(const js_ack_task&)> on_success;
    std::function<void(const js_ack_task&, const std::string& reason)> on_failure;

private:
    struct ack_conn_state {
        std::string inbox_base;
        uint64_t next_token{1};
        nats_asio::isubscription_sptr sub;
    };

    struct pending_slot {
        bool active = false;
        uint64_t token = 0;
        js_ack_task task;
        std::chrono::steady_clock::time_point sent_at;
    };

    std::size_t slot_index(std::size_t conn_idx, uint64_t token) const {
        return static_cast<std::size_t>(token % m_pending_slots[conn_idx].size());
    }

    std::string build_ack_inbox_base(std::size_t conn_idx) {
        std::uniform_int_distribution<uint64_t> dist;
        std::string base("_INBOX.NATS_TOOL_ACK.");
        base += std::to_string(dist(m_rng));
        base.push_back('.');
        base += std::to_string(conn_idx);
        return base;
    }

    void on_ack_message(std::size_t conn_idx, nats_asio::string_view subject,
                        std::span<const char> data) {
        const auto& base = m_ack_conns[conn_idx].inbox_base;
        if (subject.size() <= base.size() + 1 || subject.substr(0, base.size()) != base ||
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

        js_ack_task task;
        {
            std::lock_guard<std::mutex> lock(m_pending_mutexes[conn_idx]);
            auto& slots = m_pending_slots[conn_idx];
            auto idx = slot_index(conn_idx, token);
            auto& slot = slots[idx];
            if (!slot.active || slot.token != token) {
                return;  // Timed out or already handled.
            }
            slot.active = false;
            task = std::move(slot.task);
        }

        m_pending.fetch_sub(1, std::memory_order_relaxed);
        if (success) {
            m_success_total.fetch_add(1, std::memory_order_relaxed);
            if (on_success) {
                on_success(task);
            }
        } else {
            m_failure_total.fetch_add(1, std::memory_order_relaxed);
            if (on_failure) {
                on_failure(task, "server returned an error ack");
            }
        }
    }

    // Called when the publish call itself failed synchronously (not an
    // ack-level failure) - unlike on_ack_message's explicit-error case,
    // this DOES retry, same as a timeout.
    void handle_failure(js_ack_task task) {
        bool removed = false;
        {
            std::lock_guard<std::mutex> lock(m_pending_mutexes[task.conn_idx]);
            auto& slots = m_pending_slots[task.conn_idx];
            auto idx = slot_index(task.conn_idx, task.token);
            auto& slot = slots[idx];
            if (slot.active && slot.token == task.token) {
                slot.active = false;
                removed = true;
            }
        }
        if (!removed) {
            return;
        }

        m_pending.fetch_sub(1, std::memory_order_relaxed);
        if (task.retry_count < m_max_retries) {
            task.retry_count++;
            m_retry_total.fetch_add(1, std::memory_order_relaxed);
            schedule(std::move(task));
        } else {
            m_failure_total.fetch_add(1, std::memory_order_relaxed);
            if (on_failure) {
                on_failure(task, "publish failed");
            }
        }
    }

    // Only reached when m_headers is non-empty (publish_queued() doesn't
    // support headers) - drains conn_idx's queue with real awaited
    // publish() calls. One persistent loop per connection, (re)spawned
    // whenever it isn't already running, instead of a coroutine per
    // message.
    asio::awaitable<void> send_loop(std::size_t conn_idx) {
        while (!m_stop.load(std::memory_order_acquire)) {
            auto& queue = m_send_queues[conn_idx];
            if (queue.empty()) {
                break;
            }

            auto task = std::move(queue.front());
            queue.pop_front();

            std::span<const char> payload_span(task.payload.data(), task.payload.size());
            std::optional<nats_asio::string_view> reply_to(task.reply_subject);

            auto s = co_await task.conn->publish(task.subject, payload_span, m_headers, reply_to);
            if (s.failed()) {
                handle_failure(std::move(task));
            }
        }
        m_sender_running[conn_idx] = false;
        co_return;
    }

    std::shared_ptr<spdlog::logger> m_log;
    std::vector<nats_asio::iconnection_sptr> m_connections;
    asio::any_io_executor m_send_loop_executor;
    int m_max_retries;
    std::chrono::milliseconds m_timeout;
    nats_asio::headers_t m_headers;

    std::vector<ack_conn_state> m_ack_conns;
    std::vector<std::vector<pending_slot>> m_pending_slots;
    std::deque<std::mutex> m_pending_mutexes;  // deque: std::mutex is non-movable
    std::vector<std::deque<js_ack_task>> m_send_queues;
    std::vector<bool> m_sender_running;
    std::mt19937_64 m_rng{std::random_device{}()};

    std::atomic<int> m_pending{0};
    std::atomic<uint64_t> m_success_total{0};
    std::atomic<uint64_t> m_failure_total{0};
    std::atomic<uint64_t> m_retry_total{0};
    std::atomic<bool> m_stop{false};
};

} // namespace nats_tool
