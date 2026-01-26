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
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <chrono>
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <future>
#include <memory>
#include <nats_asio/nats_asio.hpp>
#include <optional>
#include <poll.h>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace nats_tool {

// Batch item for queue
struct batch_item {
    std::string data;
    std::size_t msg_count;
};

// High-performance lock-free batch queue using moodycamel::BlockingConcurrentQueue
class batch_queue {
public:
    batch_queue() : m_queue(1024) {}  // Pre-allocate for 1024 items

    void set_max_size(std::size_t max_size) {
        m_max_size = max_size;
    }

    // Returns true if pushed, false if queue is full (when max_size set)
    bool push(batch_item item, std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
        // Check size limit (approximate, lock-free check)
        if (m_max_size > 0 && m_size.load(std::memory_order_relaxed) >= m_max_size) {
            // Wait for space with timeout
            auto deadline = std::chrono::steady_clock::now() + timeout;
            while (m_size.load(std::memory_order_relaxed) >= m_max_size) {
                if (std::chrono::steady_clock::now() >= deadline || m_done.load(std::memory_order_relaxed)) {
                    return false;
                }
                std::this_thread::yield();
            }
        }

        if (m_queue.enqueue(std::move(item))) {
            m_size.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    bool pop(batch_item& item, std::chrono::milliseconds timeout) {
        if (m_queue.wait_dequeue_timed(item, timeout)) {
            // Only decrement size for non-sentinel items
            if (item.msg_count > 0) {
                m_size.fetch_sub(1, std::memory_order_relaxed);
            }
            return true;
        }
        return false;
    }

    void set_done() {
        m_done.store(true, std::memory_order_release);
        // Enqueue a sentinel to wake up waiting consumers (msg_count=0 identifies it)
        m_queue.enqueue(batch_item{"", 0});
    }

    bool is_done() const {
        return m_done.load(std::memory_order_acquire) &&
               m_size.load(std::memory_order_relaxed) == 0;
    }

    std::size_t size() const {
        return m_size.load(std::memory_order_relaxed);
    }

private:
    moodycamel::BlockingConcurrentQueue<batch_item> m_queue;
    std::atomic<std::size_t> m_size{0};
    std::atomic<bool> m_done{false};
    std::size_t m_max_size = 0;  // 0 = unlimited
};

// Multi-threaded batch publisher
// - Reader thread: reads stdin, formats batches, pushes to queue
// - Writer threads: each with own io_context + connection, pulls from queue
class batch_publisher {
public:
    batch_publisher(std::shared_ptr<spdlog::logger> console,
                    const nats_asio::connect_config& conf,
                    std::optional<nats_asio::ssl_config> ssl_conf,
                    const std::string& topic,
                    int num_writers, int stats_interval,
                    std::size_t batch_size = 65536,
                    std::size_t max_queue_size = 100,
                    int flush_timeout_ms = 0,
                    const std::string& file_path = "")
        : m_console(std::move(console)), m_conf(conf), m_ssl_conf(std::move(ssl_conf)),
          m_topic(topic), m_num_writers(num_writers), m_stats_interval(stats_interval),
          m_batch_size(batch_size), m_flush_timeout_ms(flush_timeout_ms),
          m_file_path(file_path), m_counter(0), m_pending_writes(0), m_running(true) {
        m_queue.set_max_size(max_queue_size);
        // Pre-build the static part of PUB command: "PUB <topic> "
        m_pub_prefix = "PUB ";
        m_pub_prefix += m_topic;
        m_pub_prefix += ' ';
    }

    // Fast integer to string using 2-digit lookup table
    // Returns pointer past the last written character
    static char* fast_itoa(char* buf, std::size_t val) {
        // Lookup table for 2-digit pairs "00" to "99"
        static constexpr char digits[201] =
            "00010203040506070809"
            "10111213141516171819"
            "20212223242526272829"
            "30313233343536373839"
            "40414243444546474849"
            "50515253545556575859"
            "60616263646566676869"
            "70717273747576777879"
            "80818283848586878889"
            "90919293949596979899";

        // Count digits to write backwards
        char temp[24];
        char* p = temp + sizeof(temp);

        while (val >= 100) {
            auto idx = (val % 100) * 2;
            val /= 100;
            *--p = digits[idx + 1];
            *--p = digits[idx];
        }

        if (val >= 10) {
            auto idx = val * 2;
            *--p = digits[idx + 1];
            *--p = digits[idx];
        } else {
            *--p = '0' + static_cast<char>(val);
        }

        auto len = static_cast<std::size_t>((temp + sizeof(temp)) - p);
        std::memcpy(buf, p, len);
        return buf + len;
    }

    // Fast PUB header formatting without fmt overhead
    // Appends "PUB <topic> <size>\r\n" to buffer
    void append_pub_header(std::string& buf, std::size_t payload_size) {
        buf.append(m_pub_prefix);
        char size_buf[24];
        char* end = fast_itoa(size_buf, payload_size);
        buf.append(size_buf, static_cast<std::size_t>(end - size_buf));
        buf.append("\r\n", 2);
    }

    void run() {
        // Start writer threads
        for (int i = 0; i < m_num_writers; i++) {
            m_writer_threads.emplace_back([this, i] { writer_thread(i); });
        }

        // Start stats thread
        if (m_stats_interval > 0) {
            m_stats_thread = std::thread([this] { stats_thread(); });
        }

        // Reader runs in main thread
        reader_thread();

        // Signal done and wait for writers
        m_queue.set_done();
        for (auto& t : m_writer_threads) {
            if (t.joinable()) t.join();
        }

        m_running = false;
        if (m_stats_thread.joinable()) {
            m_stats_thread.join();
        }

        m_console->info("Batch publisher finished, total messages: {}", m_counter.load());
    }

private:
    void reader_thread() {
        // Open file or use stdin
        int input_fd = STDIN_FILENO;
        bool close_fd = false;

        if (!m_file_path.empty()) {
            input_fd = ::open(m_file_path.c_str(), O_RDONLY);
            if (input_fd < 0) {
                m_console->error("Failed to open file '{}': {}", m_file_path, strerror(errno));
                return;
            }
            close_fd = true;
            m_console->info("Reading from file: {}", m_file_path);
        }

        if (m_flush_timeout_ms > 0) {
            m_console->info("Reader thread started, batch_size={}, flush_timeout={}ms",
                           m_batch_size, m_flush_timeout_ms);
        } else {
            m_console->info("Reader thread started, batch_size={}", m_batch_size);
        }

        // Accumulation buffer - data is appended here, processed lines are removed
        std::string accum_buf;
        accum_buf.reserve(m_batch_size * 2);

        // Output batch buffer
        std::string batch_buf;
        batch_buf.reserve(m_batch_size * 2);

        struct pollfd pfd;
        pfd.fd = input_fd;
        pfd.events = POLLIN;

        while (m_running) {
            // Use poll() if flush_timeout is enabled to avoid blocking indefinitely
            if (m_flush_timeout_ms > 0) {
                int poll_result = poll(&pfd, 1, m_flush_timeout_ms);
                if (poll_result == 0) {
                    // Timeout - no data available within flush_timeout
                    // Flush accum_buf as a complete message if present
                    if (!accum_buf.empty()) {
                        auto accum_size = accum_buf.size();
                        batch_buf.clear();
                        append_pub_header(batch_buf, accum_size);
                        batch_buf.append(accum_buf);
                        batch_buf.append("\r\n", 2);
                        m_queue.push({std::move(batch_buf), 1});
                        batch_buf.clear();
                        batch_buf.reserve(m_batch_size * 2);
                        accum_buf.clear();
                        m_console->debug("Flush timeout: flushed partial line ({} bytes)", accum_size);
                    }
                    continue;
                } else if (poll_result < 0) {
                    if (errno == EINTR) continue;
                    m_console->error("poll error: {}", strerror(errno));
                    break;
                }
                // poll_result > 0: data available, proceed to read
            }

            // Read directly into accum_buf to avoid extra copy
            std::size_t old_size = accum_buf.size();
            accum_buf.resize(old_size + m_batch_size);

            auto read_start = std::chrono::steady_clock::now();
            ssize_t bytes_read = ::read(input_fd, accum_buf.data() + old_size, m_batch_size);
            auto read_end = std::chrono::steady_clock::now();

            if (bytes_read <= 0) {
                accum_buf.resize(old_size);  // Restore size
                break;  // EOF or error
            }

            accum_buf.resize(old_size + static_cast<std::size_t>(bytes_read));
            m_bytes_read += static_cast<std::size_t>(bytes_read);

            auto read_us = std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start).count();
            if (read_us > 10000) {  // Log if read took > 10ms
                m_console->warn("Slow read: {}ms for {} bytes", read_us / 1000, bytes_read);
            }

            batch_buf.clear();
            std::size_t msg_count = 0;
            std::size_t start = 0;
            std::size_t pos = 0;

            // Process complete lines from accum_buf
            const char* data = accum_buf.data();
            std::size_t data_size = accum_buf.size();

            while (start < data_size) {
                // Find newline using memchr (faster than string::find for raw memory)
                const char* nl = static_cast<const char*>(
                    std::memchr(data + start, '\n', data_size - start));
                if (!nl) break;

                pos = static_cast<std::size_t>(nl - data);
                std::size_t line_len = pos - start;

                // Strip trailing \r if present
                if (line_len > 0 && data[start + line_len - 1] == '\r') {
                    line_len--;
                }

                if (line_len > 0) {
                    // Format PUB command - append directly from accum_buf
                    append_pub_header(batch_buf, line_len);
                    batch_buf.append(data + start, line_len);
                    batch_buf.append("\r\n", 2);
                    msg_count++;
                }

                start = pos + 1;
            }

            // Remove processed data from accum_buf
            if (start > 0) {
                if (start < accum_buf.size()) {
                    // Move remaining data to beginning
                    std::memmove(accum_buf.data(), accum_buf.data() + start, accum_buf.size() - start);
                    accum_buf.resize(accum_buf.size() - start);
                } else {
                    accum_buf.clear();
                }
            }

            if (msg_count > 0) {
                m_queue.push({std::move(batch_buf), msg_count});
                batch_buf.clear();
                batch_buf.reserve(m_batch_size * 2);
            }
        }

        // Handle final leftover in accum_buf
        if (!accum_buf.empty()) {
            batch_buf.clear();
            append_pub_header(batch_buf, accum_buf.size());
            batch_buf.append(accum_buf);
            batch_buf.append("\r\n", 2);
            m_queue.push({std::move(batch_buf), 1});
        }

        if (close_fd) {
            ::close(input_fd);
        }

        m_console->info("Reader thread finished");
    }

    void writer_thread(int id) {
        // Each writer has its own io_context and connection
        asio::io_context ioc;

        auto conn = m_ssl_conf
            ? nats_asio::create_connection(ioc,
                [](nats_asio::iconnection&) -> asio::awaitable<void> { co_return; },
                [](nats_asio::iconnection&) -> asio::awaitable<void> { co_return; },
                [this](nats_asio::iconnection&, std::string_view err) -> asio::awaitable<void> {
                    m_console->error("connection error: {}", err);
                    co_return;
                }, m_ssl_conf)
            : nats_asio::create_connection(ioc,
                [](nats_asio::iconnection&) -> asio::awaitable<void> { co_return; },
                [](nats_asio::iconnection&) -> asio::awaitable<void> { co_return; },
                [this](nats_asio::iconnection&, std::string_view err) -> asio::awaitable<void> {
                    m_console->error("connection error: {}", err);
                    co_return;
                }, std::nullopt);

        conn->start(m_conf);

        // Run io_context in background to establish connection
        std::thread io_thread([&ioc] { ioc.run(); });

        // Wait for connection
        while (!conn->is_connected() && m_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        m_console->info("Writer {} connected", id);

        // Process batches
        while (m_running || !m_queue.is_done()) {
            batch_item item;
            if (!m_queue.pop(item, std::chrono::milliseconds(100))) {
                if (m_queue.is_done()) break;
                continue;
            }

            // Skip sentinel items (used to wake up consumers on shutdown)
            if (item.msg_count == 0 && item.data.empty()) {
                if (m_queue.is_done()) break;
                continue;
            }

            m_pending_writes++;

            // Post write to io_context
            std::promise<bool> write_done;
            auto write_future = write_done.get_future();

            asio::co_spawn(ioc,
                [this, &conn, data = std::move(item.data), count = item.msg_count,
                 &write_done]() mutable -> asio::awaitable<void> {
                    std::span<const char> batch_span(data.data(), data.size());
                    auto s = co_await conn->write_raw(batch_span);
                    if (s.failed()) {
                        m_console->error("batch write failed: {}", s.error());
                    } else {
                        m_counter += count;
                    }
                    write_done.set_value(true);
                }, asio::detached);

            // Wait for write to complete
            write_future.wait();
            m_pending_writes--;
        }

        conn->stop();
        ioc.stop();
        if (io_thread.joinable()) io_thread.join();

        m_console->info("Writer {} finished", id);
    }

    void stats_thread() {
        while (m_running) {
            std::this_thread::sleep_for(std::chrono::seconds(m_stats_interval));
            auto count = m_counter.exchange(0);
            auto bytes = m_bytes_read.exchange(0);
            auto queue_size = m_queue.size();
            auto pending = m_pending_writes.load();
            m_console->info("Stats: {} events/sec, {} MB/sec read, queue={}, pending={}",
                           count / m_stats_interval,
                           bytes / m_stats_interval / 1024 / 1024,
                           queue_size, pending);
        }
    }

    std::shared_ptr<spdlog::logger> m_console;
    nats_asio::connect_config m_conf;
    std::optional<nats_asio::ssl_config> m_ssl_conf;
    std::string m_topic;
    std::string m_pub_prefix;  // Pre-built "PUB <topic> " for fast formatting
    int m_num_writers;
    int m_stats_interval;
    std::size_t m_batch_size;
    int m_flush_timeout_ms;
    std::string m_file_path;
    std::atomic<std::size_t> m_counter;
    std::atomic<std::size_t> m_bytes_read{0};
    std::atomic<std::size_t> m_pending_writes;
    std::atomic<bool> m_running;
    batch_queue m_queue;
    std::vector<std::thread> m_writer_threads;
    std::thread m_stats_thread;
};

} // namespace nats_tool
