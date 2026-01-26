/*
MIT License

Copyright (c) 2019 Vladislav Troinich

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

#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <nats_asio/nats_asio.hpp>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <unordered_map>

namespace nats_tool {

// ============================================================================
// JetStream Sliding Window ACK Tracking
// ============================================================================

// Represents an in-flight message awaiting ACK
struct in_flight_msg {
    std::string nonce;                                       // Message nonce for ACK tracking
    std::chrono::steady_clock::time_point sent_time;        // When message was sent
    std::string subject;                                     // Subject for retry
    std::string payload;                                     // Payload for retry (optional)
    int retry_count = 0;                                     // Number of retries
    bool acked = false;                                      // Whether ACK received
};

// Sliding window tracker for JetStream publish ACKs
// Allows batching publishes and tracking ACKs asynchronously
class js_sliding_window {
public:
    js_sliding_window(std::size_t window_size, std::chrono::milliseconds ack_timeout,
                      std::shared_ptr<spdlog::logger> log)
        : m_window_size(window_size), m_ack_timeout(ack_timeout), m_log(std::move(log)),
          m_next_nonce(1), m_in_flight_count(0), m_acked_count(0), m_timeout_count(0),
          m_failed_count(0) {}

    // Check if window is full (blocks if need backpressure)
    bool is_full() const {
        return m_in_flight_count.load(std::memory_order_relaxed) >= m_window_size;
    }

    // Get next nonce for message
    std::string get_next_nonce() {
        auto nonce = m_next_nonce.fetch_add(1, std::memory_order_relaxed);
        return std::to_string(nonce);
    }

    // Track a sent message
    void track_message(const std::string& nonce, const std::string& subject,
                      const std::string& payload = "") {
        in_flight_msg msg;
        msg.nonce = nonce;
        msg.sent_time = std::chrono::steady_clock::now();
        msg.subject = subject;
        msg.payload = payload;
        msg.acked = false;
        msg.retry_count = 0;

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_in_flight[nonce] = msg;
            m_in_flight_count.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Mark message as ACKed or failed
    bool mark_acked(const std::string& nonce, bool success = true) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_in_flight.find(nonce);
        if (it != m_in_flight.end()) {
            it->second.acked = success;
            m_in_flight.erase(it);
            m_in_flight_count.fetch_sub(1, std::memory_order_relaxed);
            if (success) {
                m_acked_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                m_failed_count.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }
        return false;
    }

    // Check for timed out messages and return them for retry
    std::vector<in_flight_msg> check_timeouts() {
        std::vector<in_flight_msg> timed_out;
        auto now = std::chrono::steady_clock::now();

        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto it = m_in_flight.begin(); it != m_in_flight.end();) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - it->second.sent_time);

            if (elapsed > m_ack_timeout) {
                timed_out.push_back(it->second);
                it = m_in_flight.erase(it);
                m_in_flight_count.fetch_sub(1, std::memory_order_relaxed);
                m_timeout_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                ++it;
            }
        }

        return timed_out;
    }

    // Get statistics
    std::size_t in_flight_count() const {
        return m_in_flight_count.load(std::memory_order_relaxed);
    }

    std::size_t acked_count() const {
        return m_acked_count.load(std::memory_order_relaxed);
    }

    std::size_t timeout_count() const {
        return m_timeout_count.load(std::memory_order_relaxed);
    }

    std::size_t failed_count() const {
        return m_failed_count.load(std::memory_order_relaxed);
    }

    std::size_t window_size() const {
        return m_window_size;
    }

    // Wait until window has space
    asio::awaitable<void> wait_for_space(asio::io_context& ioc) {
        while (is_full()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(1));
            co_await timer.async_wait(asio::use_awaitable);
        }
    }

private:
    std::size_t m_window_size;
    std::chrono::milliseconds m_ack_timeout;
    std::shared_ptr<spdlog::logger> m_log;

    std::atomic<std::size_t> m_next_nonce;
    std::atomic<std::size_t> m_in_flight_count;
    std::atomic<std::size_t> m_acked_count;
    std::atomic<std::size_t> m_timeout_count;
    std::atomic<std::size_t> m_failed_count;

    std::mutex m_mutex;
    std::unordered_map<std::string, in_flight_msg> m_in_flight;
};

// ============================================================================
// ACK Processor - runs in background to process JetStream ACKs
// ============================================================================

class js_ack_processor {
public:
    js_ack_processor(asio::io_context& ioc, nats_asio::iconnection_sptr conn,
                     std::shared_ptr<js_sliding_window> window,
                     std::shared_ptr<spdlog::logger> log)
        : m_ioc(ioc), m_conn(std::move(conn)), m_window(std::move(window)),
          m_log(std::move(log)), m_running(false) {}

    // Start ACK processing coroutine
    void start() {
        if (m_running.load()) return;
        m_running.store(true);
        asio::co_spawn(m_ioc, process_acks(), asio::detached);
    }

    void stop() {
        m_running.store(false);
    }

private:
    asio::awaitable<void> process_acks() {
        m_log->info("JetStream ACK processor started (sliding window size: {})",
                    m_window->window_size());

        // Subscribe to JetStream ACKs ($JS.ACK.>)
        // For now, we'll process ACKs via the js_publish return values
        // In a more advanced implementation, we could subscribe to ACK subjects

        while (m_running.load()) {
            // Check for timed out messages
            auto timed_out = m_window->check_timeouts();
            if (!timed_out.empty()) {
                m_log->warn("Detected {} timed out messages (timeout: {}ms)",
                           timed_out.size(), m_window->window_size());
                // Could implement retry logic here
            }

            // Sleep briefly before next check
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);

            // Log stats periodically
            static auto last_log = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 5) {
                m_log->debug("Window stats: in_flight={}, acked={}, timeouts={}",
                            m_window->in_flight_count(),
                            m_window->acked_count(),
                            m_window->timeout_count());
                last_log = now;
            }
        }

        m_log->info("JetStream ACK processor stopped");
        co_return;
    }

    asio::io_context& m_ioc;
    nats_asio::iconnection_sptr m_conn;
    std::shared_ptr<js_sliding_window> m_window;
    std::shared_ptr<spdlog::logger> m_log;
    std::atomic<bool> m_running;
};

} // namespace nats_tool
