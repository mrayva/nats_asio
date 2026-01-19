#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <condition_variable>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <nats_asio/nats_asio.hpp>
#include <queue>
#include <thread>
#include <tuple>
#include <unistd.h>

#include "cxxopts.hpp"

const std::string grub_mode("grub");
const std::string gen_mode("gen");
const std::string pub_mode("pub");
const std::string js_grub_mode("js_grub");
const std::string js_fetch_mode("js_fetch");
const std::string pubkv_mode("pubkv");
const std::string kvwatch_mode("kvwatch");
const std::string kvcreate_mode("kvcreate");
const std::string kvupdate_mode("kvupdate");
const std::string kvkeys_mode("kvkeys");
const std::string kvhistory_mode("kvhistory");
const std::string kvpurge_mode("kvpurge");
const std::string kvrevert_mode("kvrevert");

enum class mode { grubber, generator, publisher, js_grubber, js_fetcher, kv_publisher, kv_watcher, kv_creator, kv_updater, kv_keys_lister, kv_history_viewer, kv_purger, kv_reverter };

class worker {
public:
    worker(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval)
        : m_stats_interval(stats_interval), m_counter(0), m_ioc(ioc), m_log(console) {
        if (m_stats_interval > 0) {
            asio::co_spawn(
                ioc, [this]() -> asio::awaitable<void> { return stats_timer(); }, asio::detached);
        }
    }

    asio::awaitable<void> stats_timer() {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        while (true) {
            timer.expires_after(std::chrono::seconds(m_stats_interval));

            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));

            if (ec) {
                co_return;
            }

            m_log->info("Stats: {} events/sec", m_counter / m_stats_interval);
            m_counter = 0;
        }
    }

protected:
    int m_stats_interval;
    std::size_t m_counter;
    asio::io_context& m_ioc;
    std::shared_ptr<spdlog::logger> m_log;
};

class generator : public worker {
public:
    generator(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              const nats_asio::iconnection_sptr& conn, const std::string& topic, int stats_interval,
              int publish_interval_ms)
        : worker(ioc, console, stats_interval), m_publish_interval_ms(publish_interval_ms),
          m_topic(topic), m_conn(conn) {
        if (m_publish_interval_ms >= 0) {
            asio::co_spawn(ioc, publish(), asio::detached);
        }
    }

    asio::awaitable<void> publish() {
        asio::steady_timer timer(co_await asio::this_coro::executor);
        const std::string msg("{\"value\": 123}");
        std::span<const char> payload_span(msg.data(), msg.size());

        for (;;) {
            auto s = co_await m_conn->publish(m_topic, payload_span, std::nullopt);

            if (s.failed()) {
                m_log->error("publish failed with error {}", s.error());
            } else {
                m_counter++;
            }

            timer.expires_after(std::chrono::milliseconds(m_publish_interval_ms));

            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));

            if (ec) {
                co_return;
            }
        }
    }

private:
    int m_publish_interval_ms;
    std::string m_topic;
    nats_asio::iconnection_sptr m_conn;
};

class grubber : public worker {
public:
    grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
            bool print_to_stdout)
        : worker(ioc, console, stats_interval), m_print_to_stdout(print_to_stdout) {}

    asio::awaitable<void> on_message(nats_asio::string_view,
                                     nats_asio::optional<nats_asio::string_view>,
                                     std::span<const char> payload) {
        m_counter++;

        if (m_print_to_stdout) {
            std::cout.write(payload.data(), payload.size()) << std::endl;
        }

        co_return;
    }

private:
    bool m_print_to_stdout;
};

// JetStream subscriber using push consumer
class js_grubber : public worker {
public:
    js_grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
               bool print_to_stdout, bool auto_ack)
        : worker(ioc, console, stats_interval), m_print_to_stdout(print_to_stdout),
          m_auto_ack(auto_ack) {}

    asio::awaitable<void> on_js_message(nats_asio::ijs_subscription& sub,
                                         const nats_asio::js_message& msg) {
        m_counter++;

        if (m_print_to_stdout) {
            std::cout.write(msg.msg.payload.data(), msg.msg.payload.size()) << std::endl;
            if (!msg.stream.empty()) {
                m_log->debug("stream={} consumer={} seq={}/{} delivered={}",
                            msg.stream, msg.consumer, msg.stream_sequence,
                            msg.consumer_sequence, msg.num_delivered);
            }
        }

        // Auto-acknowledge if enabled
        if (m_auto_ack) {
            auto s = co_await sub.ack(msg);
            if (s.failed()) {
                m_log->error("ack failed: {}", s.error());
            }
        }

        co_return;
    }

private:
    bool m_print_to_stdout;
    bool m_auto_ack;
};

// JetStream fetcher using pull consumer
class js_fetcher : public worker {
public:
    js_fetcher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
               const nats_asio::iconnection_sptr& conn, const std::string& stream,
               const std::string& consumer, int stats_interval, bool print_to_stdout,
               int batch_size, int fetch_interval_ms)
        : worker(ioc, console, stats_interval), m_conn(conn), m_stream(stream),
          m_consumer(consumer), m_print_to_stdout(print_to_stdout),
          m_batch_size(batch_size), m_fetch_interval_ms(fetch_interval_ms) {
        asio::co_spawn(ioc, fetch_loop(), asio::detached);
    }

    asio::awaitable<void> fetch_loop() {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        while (true) {
            if (!m_conn->is_connected()) {
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(asio::use_awaitable);
                continue;
            }

            auto [messages, s] = co_await m_conn->js_fetch(
                m_stream, m_consumer, m_batch_size, std::chrono::milliseconds(5000));

            if (s.failed()) {
                m_log->error("js_fetch failed: {}", s.error());
            } else {
                for (const auto& msg : messages) {
                    m_counter++;
                    if (m_print_to_stdout) {
                        std::cout.write(msg.msg.payload.data(), msg.msg.payload.size()) << std::endl;
                    }

                    // Acknowledge the message
                    if (msg.msg.reply_to) {
                        auto ack_status = co_await m_conn->publish(
                            *msg.msg.reply_to, std::span<const char>("+ACK", 4), std::nullopt);
                        if (ack_status.failed()) {
                            m_log->error("ack failed: {}", ack_status.error());
                        }
                    }
                }

                if (messages.empty()) {
                    m_log->debug("No messages available");
                }
            }

            if (m_fetch_interval_ms > 0) {
                timer.expires_after(std::chrono::milliseconds(m_fetch_interval_ms));
                co_await timer.async_wait(asio::use_awaitable);
            }
        }
    }

private:
    nats_asio::iconnection_sptr m_conn;
    std::string m_stream;
    std::string m_consumer;
    bool m_print_to_stdout;
    int m_batch_size;
    int m_fetch_interval_ms;
};

// KV publisher - reads key|value pairs from stdin and publishes to KV bucket
class kv_publisher : public worker {
public:
    kv_publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                 nats_asio::iconnection_sptr conn, const std::string& bucket,
                 int stats_interval, int max_in_flight, const std::string& separator,
                 int kv_timeout_ms)
        : worker(ioc, console, stats_interval), m_conn(std::move(conn)),
          m_bucket(bucket), m_in_flight(0), m_max_in_flight(max_in_flight),
          m_separator(separator), m_kv_timeout(std::chrono::milliseconds(kv_timeout_ms)),
          m_stdin(ioc, ::dup(STDIN_FILENO)) {
        asio::co_spawn(ioc, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        asio::streambuf buf;

        for (;;) {
            // Async read line from stdin
            auto [ec, bytes_read] = co_await asio::async_read_until(
                m_stdin, buf, '\n', asio::as_tuple(asio::use_awaitable));

            if (ec) {
                if (ec == asio::error::eof || ec == asio::error::not_found) {
                    break;  // EOF or no more data
                }
                m_log->error("stdin read error: {}", ec.message());
                break;
            }

            // Extract line from buffer (without newline)
            std::string line;
            std::istream is(&buf);
            std::getline(is, line);

            // Skip empty lines
            if (line.empty()) {
                continue;
            }

            // Parse key|value - find first separator
            auto sep_pos = line.find(m_separator);
            if (sep_pos == std::string::npos) {
                m_log->error("invalid line format, missing separator '{}': {}", m_separator, line);
                continue;
            }

            std::string key = line.substr(0, sep_pos);
            std::string value_part = line.substr(sep_pos + m_separator.size());

            if (key.empty()) {
                m_log->error("empty key in line: {}", line);
                continue;
            }

            // Check if this is a delete operation (value starts with separator)
            bool is_delete = false;
            if (value_part.size() >= m_separator.size() &&
                value_part.substr(0, m_separator.size()) == m_separator) {
                is_delete = true;
            }

            // Wait until connection is ready
            while (!m_conn->is_connected()) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(asio::use_awaitable);
            }

            // Backpressure: wait if too many operations in flight
            while (m_in_flight >= m_max_in_flight) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(5));
                co_await timer.async_wait(asio::use_awaitable);
            }

            m_in_flight++;

            // Capture data for async operation
            auto key_copy = std::make_shared<std::string>(std::move(key));
            auto value_copy = std::make_shared<std::string>(std::move(value_part));

            // Fire-and-forget: dispatch KV operation without waiting
            asio::co_spawn(
                m_ioc,
                [this, key_copy, value_copy, is_delete]() -> asio::awaitable<void> {
                    if (is_delete) {
                        auto [rev, s] = co_await m_conn->kv_delete(m_bucket, *key_copy, m_kv_timeout);
                        if (s.failed()) {
                            m_log->error("kv_delete failed for key '{}': {}", *key_copy, s.error());
                        } else {
                            m_counter++;
                            m_log->debug("deleted key '{}' rev={}", *key_copy, rev);
                        }
                    } else {
                        std::span<const char> value_span(value_copy->data(), value_copy->size());
                        auto [rev, s] = co_await m_conn->kv_put(m_bucket, *key_copy, value_span, m_kv_timeout);
                        if (s.failed()) {
                            m_log->error("kv_put failed for key '{}': {}", *key_copy, s.error());
                        } else {
                            m_counter++;
                            m_log->debug("put key '{}' rev={}", *key_copy, rev);
                        }
                    }
                    m_in_flight--;
                    co_return;
                },
                asio::detached);
        }

        // Wait for all in-flight operations to complete
        m_log->info("EOF reached, waiting for {} in-flight KV operations", m_in_flight.load());
        while (m_in_flight > 0) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(asio::use_awaitable);
        }

        m_log->info("All KV operations complete, stopping");
        m_ioc.stop();
        co_return;
    }

private:
    nats_asio::iconnection_sptr m_conn;
    std::string m_bucket;
    std::atomic<int> m_in_flight;
    int m_max_in_flight;
    std::string m_separator;
    std::chrono::milliseconds m_kv_timeout;
    asio::posix::stream_descriptor m_stdin;
};

// KV watcher for watching bucket changes
class kv_watcher_handler : public worker {
public:
    kv_watcher_handler(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
                       int stats_interval, bool print_to_stdout)
        : worker(ioc, console, stats_interval), m_print_to_stdout(print_to_stdout) {}

    asio::awaitable<void> on_kv_entry(const nats_asio::kv_entry& entry) {
        m_counter++;

        if (m_print_to_stdout) {
            std::string op_str;
            switch (entry.op) {
                case nats_asio::kv_entry::operation::put: op_str = "PUT"; break;
                case nats_asio::kv_entry::operation::del: op_str = "DEL"; break;
                case nats_asio::kv_entry::operation::purge: op_str = "PURGE"; break;
            }
            std::cout << "[" << op_str << "] " << entry.bucket << "/" << entry.key
                      << " rev=" << entry.revision;
            if (entry.op == nats_asio::kv_entry::operation::put && !entry.value.empty()) {
                std::cout << " value=";
                std::cout.write(entry.value.data(), entry.value.size());
            }
            std::cout << std::endl;
        }

        co_return;
    }

private:
    bool m_print_to_stdout;
};

class publisher : public worker {
public:
    publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              std::vector<nats_asio::iconnection_sptr> connections, const std::string& topic,
              int stats_interval, int max_in_flight = 1000, bool jetstream = false,
              int js_timeout_ms = 5000, bool wait_for_ack = true)
        : worker(ioc, console, stats_interval), m_connections(std::move(connections)),
          m_topic(topic), m_next_conn(0), m_in_flight(0), m_max_in_flight(max_in_flight),
          m_jetstream(jetstream), m_js_timeout(std::chrono::milliseconds(js_timeout_ms)),
          m_wait_for_ack(wait_for_ack), m_stdin(ioc, ::dup(STDIN_FILENO)) {
        asio::co_spawn(ioc, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        asio::streambuf buf;

        for (;;) {
            // Async read line from stdin
            auto [ec, bytes_read] = co_await asio::async_read_until(
                m_stdin, buf, '\n', asio::as_tuple(asio::use_awaitable));

            if (ec) {
                if (ec == asio::error::eof || ec == asio::error::not_found) {
                    break;  // EOF or no more data
                }
                m_log->error("stdin read error: {}", ec.message());
                break;
            }

            // Extract line from buffer (without newline)
            std::string line;
            std::istream is(&buf);
            std::getline(is, line);

            // Wait until at least one connection is ready
            while (!has_connected_connection()) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(asio::use_awaitable);
            }

            // Backpressure: wait if too many publishes in flight
            while (m_in_flight >= m_max_in_flight) {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(5));
                co_await timer.async_wait(asio::use_awaitable);
            }

            // Round-robin connection selection
            auto conn = get_next_connection();

            // Capture message in shared_ptr for async lifetime
            auto msg = std::make_shared<std::string>(std::move(line));

            m_in_flight++;

            // Fire-and-forget: dispatch publish without waiting
            asio::co_spawn(
                m_ioc,
                [this, conn, msg]() -> asio::awaitable<void> {
                    std::span<const char> payload_span(msg->data(), msg->size());

                    if (m_jetstream) {
                        auto [ack, s] = co_await conn->js_publish(m_topic, payload_span, m_js_timeout, m_wait_for_ack);
                        if (s.failed()) {
                            m_log->error("js_publish failed: {}", s.error());
                        } else {
                            m_counter++;
                        }
                    } else {
                        auto s = co_await conn->publish(m_topic, payload_span, std::nullopt);
                        if (s.failed()) {
                            m_log->error("publish failed: {}", s.error());
                        } else {
                            m_counter++;
                        }
                    }
                    m_in_flight--;
                    co_return;
                },
                asio::detached);
        }

        // Wait for all in-flight publishes to complete
        m_log->info("EOF reached, waiting for {} in-flight publishes", m_in_flight.load());
        while (m_in_flight > 0) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(asio::use_awaitable);
        }

        m_log->info("All publishes complete, stopping");
        m_ioc.stop();
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
            auto conn = m_connections[m_next_conn % m_connections.size()];
            m_next_conn++;
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
    std::size_t m_next_conn;
    std::atomic<int> m_in_flight;
    int m_max_in_flight;
    bool m_jetstream;
    std::chrono::milliseconds m_js_timeout;
    bool m_wait_for_ack;
    asio::posix::stream_descriptor m_stdin;
};

// Thread-safe batch queue
struct batch_item {
    std::string data;
    std::size_t msg_count;
};

class batch_queue {
public:
    void set_max_size(std::size_t max_size) {
        m_max_size = max_size;
    }

    // Returns true if pushed, false if queue is full (when max_size set)
    bool push(batch_item item, std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
        std::unique_lock<std::mutex> lock(m_mutex);

        // Wait if queue is full
        if (m_max_size > 0) {
            if (!m_cv_full.wait_for(lock, timeout, [this] { return m_queue.size() < m_max_size || m_done; })) {
                return false;  // timeout, queue full
            }
        }

        m_queue.push(std::move(item));
        lock.unlock();
        m_cv.notify_one();
        return true;
    }

    bool pop(batch_item& item, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_cv.wait_for(lock, timeout, [this] { return !m_queue.empty() || m_done; })) {
            return false;  // timeout
        }
        if (m_queue.empty()) {
            return false;  // done signal
        }
        item = std::move(m_queue.front());
        m_queue.pop();

        // Notify producer that space is available
        lock.unlock();
        m_cv_full.notify_one();
        return true;
    }

    void set_done() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_done = true;
        }
        m_cv.notify_all();
        m_cv_full.notify_all();
    }

    bool is_done() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_done && m_queue.empty();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

private:
    std::queue<batch_item> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    std::condition_variable m_cv_full;
    bool m_done = false;
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
                    std::size_t max_queue_size = 100)
        : m_console(std::move(console)), m_conf(conf), m_ssl_conf(std::move(ssl_conf)),
          m_topic(topic), m_num_writers(num_writers), m_stats_interval(stats_interval),
          m_batch_size(batch_size), m_counter(0), m_pending_writes(0), m_running(true) {
        m_queue.set_max_size(max_queue_size);
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
        m_console->info("Reader thread started, batch_size={}", m_batch_size);

        std::vector<char> read_buf(m_batch_size);
        std::string leftover;
        std::string batch_buf;
        batch_buf.reserve(m_batch_size * 2);

        while (m_running) {
            auto read_start = std::chrono::steady_clock::now();
            ssize_t bytes_read = ::read(STDIN_FILENO, read_buf.data(), read_buf.size());
            auto read_end = std::chrono::steady_clock::now();

            if (bytes_read <= 0) {
                break;  // EOF or error
            }

            m_bytes_read += static_cast<std::size_t>(bytes_read);
            auto read_us = std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start).count();
            if (read_us > 10000) {  // Log if read took > 10ms
                m_console->warn("Slow stdin read: {}ms for {} bytes", read_us / 1000, bytes_read);
            }

            std::string_view chunk(read_buf.data(), static_cast<std::size_t>(bytes_read));

            batch_buf.clear();
            std::size_t msg_count = 0;
            std::size_t start = 0;
            std::size_t pos = 0;

            // Combine with leftover
            std::string combined;
            if (!leftover.empty()) {
                combined = leftover + std::string(chunk);
                chunk = combined;
                leftover.clear();
            }

            while ((pos = chunk.find('\n', start)) != std::string_view::npos) {
                std::string_view line = chunk.substr(start, pos - start);
                start = pos + 1;

                if (line.empty()) continue;
                if (!line.empty() && line.back() == '\r') {
                    line = line.substr(0, line.size() - 1);
                }
                if (line.empty()) continue;

                // Format PUB command
                fmt::format_to(std::back_inserter(batch_buf),
                    "PUB {} {}\r\n", m_topic, line.size());
                batch_buf.append(line.data(), line.size());
                batch_buf.append("\r\n");
                msg_count++;
            }

            if (start < chunk.size()) {
                leftover = std::string(chunk.substr(start));
            }

            if (msg_count > 0) {
                m_queue.push({std::move(batch_buf), msg_count});
                batch_buf = std::string();
                batch_buf.reserve(m_batch_size * 2);
            }
        }

        // Handle final leftover
        if (!leftover.empty()) {
            batch_buf.clear();
            fmt::format_to(std::back_inserter(batch_buf),
                "PUB {} {}\r\n", m_topic, leftover.size());
            batch_buf.append(leftover);
            batch_buf.append("\r\n");
            m_queue.push({std::move(batch_buf), 1});
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
    int m_num_writers;
    int m_stats_interval;
    std::size_t m_batch_size;
    std::atomic<std::size_t> m_counter;
    std::atomic<std::size_t> m_bytes_read{0};
    std::atomic<std::size_t> m_pending_writes;
    std::atomic<bool> m_running;
    batch_queue m_queue;
    std::vector<std::thread> m_writer_threads;
    std::thread m_stats_thread;
};

std::string read_file(const std::shared_ptr<spdlog::logger>& console, const std::string& path) {
    try {
        if (path.empty()) {
            return {};
        }

        std::ifstream t(path);
        std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
        return str;
    } catch (const std::exception& e) {
        console->error("failed to read file {}, with error: {}", path, e.what());
    }

    return {};
}

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options options(argv[0], " - filters command line options");
        nats_asio::connect_config conf;
        nats_asio::ssl_config ssl_conf;
        ssl_conf.verify = true;
        conf.address = "127.0.0.1";
        conf.port = 4222;
        std::string username;
        std::string password;
        std::string mode;
        std::string topic;
        int stats_interval = 1;
        int publish_interval = -1;
        int num_connections = 1;
        int max_in_flight = 1000;
        bool print_to_stdout = false;
        std::string ssl_key_file;
        std::string ssl_cert_file;
        std::string ssl_ca_file;
        /* clang-format off */
        options.add_options()
        ("h,help", "Print help")
        ("d,debug", "Enable debugging")
        ("address", "Address of NATS server", cxxopts::value<std::string>(conf.address))
        ("port", "Port of NATS server", cxxopts::value<uint16_t >(conf.port))
        ("user", "Username", cxxopts::value<std::string >(username))
        ("pass", "Password", cxxopts::value<std::string >(password))
        ("mode", "mode", cxxopts::value<std::string >(mode))
        ("topic", "topic", cxxopts::value<std::string >(topic))
        ("stats_interval", "stat interval seconds", cxxopts::value<int>(stats_interval))
        ("publish_interval", "publish interval seconds in ms", cxxopts::value<int>(publish_interval))
        ("n,connections", "Number of connections for pub mode (default: 1)", cxxopts::value<int>(num_connections))
        ("max_in_flight", "Max in-flight publishes for pub mode (default: 1000)", cxxopts::value<int>(max_in_flight))
        ("batch_pub", "Use high-performance batch publishing (pub mode, non-JS only)")
        ("batch_size", "Batch read size in bytes for batch_pub mode (default: 65536)", cxxopts::value<int>())
        ("max_queue", "Max batches in queue for batch_pub mode (default: 100)", cxxopts::value<int>())
        ("js,jetstream", "Use JetStream publish (pub mode only)")
        ("no_ack", "Fire-and-forget JetStream publish, don't wait for ack (with --js)")
        ("js_timeout", "JetStream publish timeout in ms (default: 5000)", cxxopts::value<int>())
        ("stream", "JetStream stream name (js_grub/js_fetch mode)", cxxopts::value<std::string>())
        ("consumer", "JetStream consumer name (js_grub/js_fetch mode)", cxxopts::value<std::string>())
        ("durable", "Durable consumer name (js_grub mode)", cxxopts::value<std::string>())
        ("batch", "Batch size for js_fetch mode (default: 10)", cxxopts::value<int>())
        ("fetch_interval", "Interval between fetches in ms (default: 100)", cxxopts::value<int>())
        ("auto_ack", "Auto-acknowledge messages (js_grub mode)", cxxopts::value<bool>())
        ("bucket", "KV bucket name (pubkv/kvwatch/kvcreate/kvupdate mode)", cxxopts::value<std::string>())
        ("key", "KV key (required for kvcreate/kvupdate, optional filter for kvwatch)", cxxopts::value<std::string>())
        ("value", "KV value for kvcreate/kvupdate mode", cxxopts::value<std::string>())
        ("revision", "Expected revision for kvupdate mode", cxxopts::value<uint64_t>())
        ("separator", "Key-value separator for pubkv mode (default: |)", cxxopts::value<std::string>())
        ("kv_timeout", "KV operation timeout in ms (default: 5000)", cxxopts::value<int>())
        ("print", "print messages to stdout", cxxopts::value<bool>(print_to_stdout))
        ("ssl", "Enable ssl")
        ("ssl_key", "ssl_key", cxxopts::value<std::string>(ssl_key_file))
        ("ssl_cert", "ssl_cert", cxxopts::value<std::string>(ssl_cert_file))
        ("ssl_ca", "ssl_ca", cxxopts::value<std::string>(ssl_ca_file))
        ;
        /* clang-format on */
        options.parse_positional({"mode"});
        auto result = options.parse(argc, argv);

        if (result.count("help")) {
            std::cout << options.help() << std::endl;
            return 0;
        }

        auto console = spdlog::stdout_color_mt("console");

        if (result.count("debug")) {
            console->set_level(spdlog::level::debug);
        }

        if (result.count("mode") == 0) {
            console->error("Please specify mode");
            return 1;
        }

        std::optional<nats_asio::ssl_config> opt_ssl_conf;
        if (result.count("ssl")) {
            ssl_conf.cert = read_file(console, ssl_cert_file);
            ssl_conf.ca = read_file(console, ssl_ca_file);
            ssl_conf.key = read_file(console, ssl_key_file);
            opt_ssl_conf = ssl_conf;
        } else {
            opt_ssl_conf = std::nullopt;
        }

        mode = result["mode"].as<std::string>();

        // Only require topic for modes that need it
        if (topic.empty() && result.count("topic")) {
            topic = result["topic"].as<std::string>();
        }

        if (mode != grub_mode && mode != gen_mode && mode != pub_mode &&
            mode != js_grub_mode && mode != js_fetch_mode && mode != pubkv_mode &&
            mode != kvwatch_mode && mode != kvcreate_mode && mode != kvupdate_mode &&
            mode != kvkeys_mode && mode != kvhistory_mode && mode != kvpurge_mode &&
            mode != kvrevert_mode) {
            console->error("Invalid mode. Use --help to see available modes");
            return 1;
        }

        auto m = mode::generator;

        if (mode == grub_mode) {
            m = mode::grubber;
            publish_interval = -1;
        } else if (mode == pub_mode) {
            m = mode::publisher;
            if (num_connections < 1) {
                num_connections = 1;
            }
        } else if (mode == js_grub_mode) {
            m = mode::js_grubber;
            publish_interval = -1;
        } else if (mode == js_fetch_mode) {
            m = mode::js_fetcher;
            publish_interval = -1;
        } else if (mode == pubkv_mode) {
            m = mode::kv_publisher;
            publish_interval = -1;
        } else if (mode == kvwatch_mode) {
            m = mode::kv_watcher;
            publish_interval = -1;
        } else if (mode == kvcreate_mode) {
            m = mode::kv_creator;
            publish_interval = -1;
        } else if (mode == kvupdate_mode) {
            m = mode::kv_updater;
            publish_interval = -1;
        } else if (mode == kvkeys_mode) {
            m = mode::kv_keys_lister;
            publish_interval = -1;
        } else if (mode == kvhistory_mode) {
            m = mode::kv_history_viewer;
            publish_interval = -1;
        } else if (mode == kvpurge_mode) {
            m = mode::kv_purger;
            publish_interval = -1;
        } else if (mode == kvrevert_mode) {
            m = mode::kv_reverter;
            publish_interval = -1;
        } else {
            if (publish_interval < 0) {
                publish_interval = 1000;
            }
        }

        // Validate JetStream mode requirements
        std::string js_stream, js_consumer, js_durable;
        int batch_size = 10;
        int fetch_interval_ms = 100;
        bool auto_ack = result.count("auto_ack") > 0;

        if (m == mode::js_grubber || m == mode::js_fetcher) {
            if (!result.count("stream")) {
                console->error("JetStream mode requires --stream");
                return 1;
            }
            js_stream = result["stream"].as<std::string>();

            if (m == mode::js_fetcher) {
                if (!result.count("consumer")) {
                    console->error("js_fetch mode requires --consumer");
                    return 1;
                }
                js_consumer = result["consumer"].as<std::string>();
            }

            if (result.count("durable")) {
                js_durable = result["durable"].as<std::string>();
            }
            if (result.count("batch")) {
                batch_size = result["batch"].as<int>();
            }
            if (result.count("fetch_interval")) {
                fetch_interval_ms = result["fetch_interval"].as<int>();
            }
        }

        // Validate KV mode requirements
        std::string kv_bucket;
        std::string kv_key;
        std::string kv_value;
        uint64_t kv_revision = 0;
        std::string kv_separator = "|";
        int kv_timeout_ms = 5000;

        if (m == mode::kv_publisher || m == mode::kv_watcher ||
            m == mode::kv_creator || m == mode::kv_updater || m == mode::kv_keys_lister ||
            m == mode::kv_history_viewer || m == mode::kv_purger || m == mode::kv_reverter) {
            if (!result.count("bucket")) {
                console->error("{} mode requires --bucket", mode);
                return 1;
            }
            kv_bucket = result["bucket"].as<std::string>();

            if (result.count("separator")) {
                kv_separator = result["separator"].as<std::string>();
            }
            if (result.count("kv_timeout")) {
                kv_timeout_ms = result["kv_timeout"].as<int>();
            }
            if (result.count("key")) {
                kv_key = result["key"].as<std::string>();
            }
            if (result.count("value")) {
                kv_value = result["value"].as<std::string>();
            }
            if (result.count("revision")) {
                kv_revision = result["revision"].as<uint64_t>();
            }

            // kvcreate and kvupdate require key and value
            if (m == mode::kv_creator || m == mode::kv_updater) {
                if (kv_key.empty()) {
                    console->error("{} mode requires --key", mode);
                    return 1;
                }
                if (kv_value.empty()) {
                    console->error("{} mode requires --value", mode);
                    return 1;
                }
            }

            // kvhistory, kvpurge, and kvrevert require key
            if (m == mode::kv_history_viewer || m == mode::kv_purger || m == mode::kv_reverter) {
                if (kv_key.empty()) {
                    console->error("{} mode requires --key", mode);
                    return 1;
                }
            }

            // kvrevert requires revision
            if (m == mode::kv_reverter) {
                if (kv_revision == 0) {
                    console->error("kvrevert mode requires --revision > 0");
                    return 1;
                }
            }

            // kvupdate requires revision
            if (m == mode::kv_updater) {
                if (kv_revision == 0) {
                    console->error("kvupdate mode requires --revision > 0");
                    return 1;
                }
            }
        }

        asio::io_context ioc;
        std::shared_ptr<grubber> grub_ptr;
        std::shared_ptr<generator> gen_ptr;
        std::shared_ptr<publisher> pub_ptr;
        std::shared_ptr<batch_publisher> batch_pub_ptr;
        std::shared_ptr<js_grubber> js_grub_ptr;
        std::shared_ptr<js_fetcher> js_fetch_ptr;
        std::shared_ptr<kv_publisher> kv_pub_ptr;
        std::shared_ptr<kv_watcher_handler> kv_watch_ptr;
        nats_asio::iconnection_sptr conn;
        std::vector<nats_asio::iconnection_sptr> pub_connections;

        if (m == mode::grubber) {
            grub_ptr = std::make_shared<grubber>(ioc, console, stats_interval, print_to_stdout);
        } else if (m == mode::js_grubber) {
            js_grub_ptr = std::make_shared<js_grubber>(ioc, console, stats_interval, print_to_stdout, auto_ack);
        } else if (m == mode::kv_watcher) {
            kv_watch_ptr = std::make_shared<kv_watcher_handler>(ioc, console, stats_interval, print_to_stdout);
        }

        // Helper to create a connection
        auto make_connection = [&](int conn_id = -1) {
            return nats_asio::create_connection(
                ioc,
                [&, conn_id](nats_asio::iconnection& /*c*/) -> asio::awaitable<void> {
                    if (conn_id >= 0) {
                        console->info("connection {} connected", conn_id);
                    } else {
                        console->info("on connected");
                    }

                    if (m == mode::grubber) {
                        auto r = co_await conn->subscribe(
                            topic,
                            [grub_ptr](auto v1, auto v2, auto v3) -> asio::awaitable<void> {
                                return grub_ptr->on_message(v1, v2, v3);
                            });

                        if (r.second.failed()) {
                            console->error("failed to subscribe with error: {}", r.second.error());
                        }
                    } else if (m == mode::js_grubber) {
                        // Setup JetStream push consumer
                        nats_asio::js_consumer_config config;
                        config.stream = js_stream;
                        config.filter_subject = topic.empty() ? std::nullopt : std::optional<std::string>(topic);
                        if (!js_durable.empty()) {
                            config.durable_name = js_durable;
                        }
                        config.ack = nats_asio::js_ack_policy::explicit_;

                        auto [sub, s] = co_await conn->js_subscribe(
                            config,
                            [js_grub_ptr](nats_asio::ijs_subscription& sub,
                                         const nats_asio::js_message& msg) -> asio::awaitable<void> {
                                return js_grub_ptr->on_js_message(sub, msg);
                            });

                        if (s.failed()) {
                            console->error("js_subscribe failed: {}", s.error());
                        } else {
                            console->info("JetStream subscription active: stream={} consumer={}",
                                        sub->info().stream, sub->info().name);
                        }
                    } else if (m == mode::kv_watcher) {
                        // Setup KV watcher
                        auto [watcher, s] = co_await conn->kv_watch(
                            kv_bucket,
                            [kv_watch_ptr](const nats_asio::kv_entry& entry) -> asio::awaitable<void> {
                                return kv_watch_ptr->on_kv_entry(entry);
                            },
                            kv_key);

                        if (s.failed()) {
                            console->error("kv_watch failed: {}", s.error());
                        } else {
                            if (kv_key.empty()) {
                                console->info("Watching KV bucket: {}", kv_bucket);
                            } else {
                                console->info("Watching KV bucket: {} key: {}", kv_bucket, kv_key);
                            }
                        }
                    } else if (m == mode::kv_creator) {
                        // Create key (only if doesn't exist)
                        std::span<const char> value_span(kv_value.data(), kv_value.size());
                        auto [rev, s] = co_await conn->kv_create(
                            kv_bucket, kv_key, value_span,
                            std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_create failed: {}", s.error());
                        } else {
                            console->info("Created {}/{} revision={}", kv_bucket, kv_key, rev);
                        }
                        ioc.stop();
                    } else if (m == mode::kv_updater) {
                        // Update key (only if revision matches)
                        std::span<const char> value_span(kv_value.data(), kv_value.size());
                        auto [rev, s] = co_await conn->kv_update(
                            kv_bucket, kv_key, value_span, kv_revision,
                            std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_update failed: {}", s.error());
                        } else {
                            console->info("Updated {}/{} revision={} (was {})", kv_bucket, kv_key, rev, kv_revision);
                        }
                        ioc.stop();
                    } else if (m == mode::kv_keys_lister) {
                        // List all keys in bucket
                        auto [keys, s] = co_await conn->kv_keys(
                            kv_bucket, std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_keys failed: {}", s.error());
                        } else {
                            console->info("Keys in bucket '{}' ({} keys):", kv_bucket, keys.size());
                            for (const auto& key : keys) {
                                std::cout << key << std::endl;
                            }
                        }
                        ioc.stop();
                    } else if (m == mode::kv_history_viewer) {
                        // Show full history for a key
                        auto [history, s] = co_await conn->kv_history(
                            kv_bucket, kv_key, std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_history failed: {}", s.error());
                        } else {
                            console->info("History for {}/{} ({} revisions):", kv_bucket, kv_key, history.size());
                            for (const auto& entry : history) {
                                std::string op_str;
                                switch (entry.op) {
                                    case nats_asio::kv_entry::operation::put: op_str = "PUT"; break;
                                    case nats_asio::kv_entry::operation::del: op_str = "DEL"; break;
                                    case nats_asio::kv_entry::operation::purge: op_str = "PURGE"; break;
                                }
                                std::cout << "rev=" << entry.revision << " [" << op_str << "]";
                                if (entry.op == nats_asio::kv_entry::operation::put && !entry.value.empty()) {
                                    std::cout << " value=";
                                    std::cout.write(entry.value.data(), entry.value.size());
                                }
                                std::cout << std::endl;
                            }
                        }
                        ioc.stop();
                    } else if (m == mode::kv_purger) {
                        // Purge key (delete and clear history)
                        auto [rev, s] = co_await conn->kv_purge(
                            kv_bucket, kv_key, std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_purge failed: {}", s.error());
                        } else {
                            console->info("Purged {}/{} revision={}", kv_bucket, kv_key, rev);
                        }
                        ioc.stop();
                    } else if (m == mode::kv_reverter) {
                        // Revert key to a previous revision
                        auto [rev, s] = co_await conn->kv_revert(
                            kv_bucket, kv_key, kv_revision,
                            std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_revert failed: {}", s.error());
                        } else {
                            console->info("Reverted {}/{} to revision {} -> new revision={}",
                                         kv_bucket, kv_key, kv_revision, rev);
                        }
                        ioc.stop();
                    }
                    co_return;
                },
                [&console, conn_id](nats_asio::iconnection&) -> asio::awaitable<void> {
                    if (conn_id >= 0) {
                        console->info("connection {} disconnected", conn_id);
                    } else {
                        console->info("on disconnected");
                    }
                    co_return;
                },
                [&console, conn_id](nats_asio::iconnection&, nats_asio::string_view err) -> asio::awaitable<void> {
                    if (conn_id >= 0) {
                        console->error("connection {} error: {}", conn_id, err);
                    } else {
                        console->error("on error: {}", err);
                    }
                    co_return;
                },
                opt_ssl_conf);
        };

        if (m == mode::publisher) {
            bool use_batch_pub = result.count("batch_pub") > 0;
            bool use_jetstream = result.count("jetstream") > 0;

            if (use_batch_pub && use_jetstream) {
                console->error("--batch_pub cannot be used with --jetstream");
                return 1;
            }

            if (use_batch_pub) {
                // Multi-threaded batch publishing mode
                int batch_size_val = result.count("batch_size") ? result["batch_size"].as<int>() : 65536;
                int max_queue_val = result.count("max_queue") ? result["max_queue"].as<int>() : 100;
                console->info("Using multi-threaded batch publisher: {} writers, batch_size={}, max_queue={}",
                             num_connections, batch_size_val, max_queue_val);
                batch_pub_ptr = std::make_shared<batch_publisher>(console, conf, opt_ssl_conf, topic,
                                                                   num_connections, stats_interval,
                                                                   static_cast<std::size_t>(batch_size_val),
                                                                   static_cast<std::size_t>(max_queue_val));
                // Run batch publisher (blocks until done)
                batch_pub_ptr->run();
                return 0;
            } else {
                // Standard publisher mode - multiple connections supported
                console->info("Creating {} connections for pub mode", num_connections);
                for (int i = 0; i < num_connections; i++) {
                    auto c = make_connection(i);
                    c->start(conf);
                    pub_connections.push_back(c);
                }
                int js_timeout_ms = result.count("js_timeout") ? result["js_timeout"].as<int>() : 5000;
                bool wait_for_ack = result.count("no_ack") == 0;  // default: wait for ack
                pub_ptr = std::make_shared<publisher>(ioc, console, pub_connections, topic, stats_interval,
                                                       max_in_flight, use_jetstream, js_timeout_ms, wait_for_ack);
            }
        } else {
            conn = make_connection();
            conn->start(conf);

            if (m == mode::generator) {
                gen_ptr = std::make_shared<generator>(ioc, console, conn, topic, stats_interval,
                                                      publish_interval);
            } else if (m == mode::js_fetcher) {
                js_fetch_ptr = std::make_shared<js_fetcher>(ioc, console, conn, js_stream,
                                                            js_consumer, stats_interval, print_to_stdout,
                                                            batch_size, fetch_interval_ms);
            } else if (m == mode::kv_publisher) {
                kv_pub_ptr = std::make_shared<kv_publisher>(ioc, console, conn, kv_bucket,
                                                            stats_interval, max_in_flight,
                                                            kv_separator, kv_timeout_ms);
            }
        }

        ioc.run();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "unknown exception" << std::endl;
        return 1;
    }

    return 0;
}
