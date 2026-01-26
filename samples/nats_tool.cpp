// mimalloc: drop-in malloc replacement - must be included first
#include <mimalloc-new-delete.h>

#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <inja/inja.hpp>
#include <numeric>
#include <sstream>
#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl.hpp>
#include <asio/ip/tcp.hpp>
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#include <fstream>
#include <future>
#include <iostream>
#include <nats_asio/nats_asio.hpp>
#include <nats_asio/compression.hpp>
#include <nats_asio/http_reader.hpp>
#include <nats_asio/input_reader.hpp>
#include <simdjson.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <poll.h>
#include <zstd.h>

#include "cxxopts.hpp"
#include "include/string_utils.hpp"
#include "include/fast_json_parser.hpp"
#include "include/worker.hpp"
#include "include/batch_publisher.hpp"

using nats_asio::zstd_compressor;
using nats_asio::input_source_config;
using nats_asio::async_http_reader;
using nats_asio::async_input_reader;

using nats_tool::output_mode;
using nats_tool::input_format;
using nats_tool::input_config;
using nats_tool::worker;
using nats_tool::fast_json_parser;
using nats_tool::split_string;
using nats_tool::apply_template;
using nats_tool::build_payload;
using nats_tool::parse_csv_line;
using nats_tool::translate_payload;
using nats_tool::batch_item;
using nats_tool::batch_queue;
using nats_tool::batch_publisher;

const std::string grub_mode("grub");
const std::string gen_mode("gen");
const std::string pub_mode("pub");
const std::string req_mode("req");
const std::string reply_mode("reply");
const std::string bench_mode("bench");
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

enum class mode { grubber, generator, publisher, requester, replier, benchmarker, js_grubber, js_fetcher, kv_publisher, kv_watcher, kv_creator, kv_updater, kv_keys_lister, kv_history_viewer, kv_purger, kv_reverter };

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

// Helper to run blocking work on a thread and await completion
// Prevents blocking the ASIO event loop
template<typename Func>
asio::awaitable<std::invoke_result_t<Func>> async_run_blocking(Func&& func) {
    using ResultType = std::invoke_result_t<Func>;

    auto executor = co_await asio::this_coro::executor;
    auto future = std::async(std::launch::async, std::forward<Func>(func));

    // Poll for completion using timer to avoid blocking the event loop
    asio::steady_timer timer(executor);
    while (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        timer.expires_after(std::chrono::milliseconds(1));
        co_await timer.async_wait(asio::use_awaitable);
    }

    co_return future.get();
}

class grubber : public worker {
public:
    grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
            output_mode mode, const std::string& dump_file = {}, const std::string& translate_cmd = {},
            bool show_timestamp = false)
        : worker(ioc, console, stats_interval), m_output_mode(mode), m_translate_cmd(translate_cmd),
          m_show_timestamp(show_timestamp) {
        if (!dump_file.empty()) {
            m_dump_file = std::make_unique<std::ofstream>(dump_file, std::ios::binary);
            if (!m_dump_file->is_open()) {
                console->error("Failed to open dump file: {}", dump_file);
                m_dump_file.reset();
            }
        }
    }

    asio::awaitable<void> on_message(nats_asio::string_view subject,
                                     nats_asio::optional<nats_asio::string_view> reply_to,
                                     std::span<const char> payload) {
        m_counter++;

        std::ostream* out = m_dump_file ? m_dump_file.get() : &std::cout;

        // Apply translation if configured (runs on background thread)
        std::string translated;
        std::span<const char> output_payload = payload;
        if (!m_translate_cmd.empty()) {
            std::string subj_str(subject);
            std::vector<char> payload_copy(payload.begin(), payload.end());
            auto log = m_log;
            std::string cmd = m_translate_cmd;
            translated = co_await async_run_blocking([cmd, subj_str, payload_copy, log]() {
                return translate_payload(cmd, subj_str, std::span<const char>(payload_copy), log);
            });
            output_payload = std::span<const char>(translated.data(), translated.size());
        }

        // Get timestamp if needed
        std::string timestamp_str;
        if (m_show_timestamp && m_output_mode != output_mode::none) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) % 1000;
            std::tm tm_now{};
            localtime_r(&time_t_now, &tm_now);
            timestamp_str = fmt::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
                tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday,
                tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec, static_cast<int>(ms.count()));
        }

        switch (m_output_mode) {
            case output_mode::raw:
                if (m_show_timestamp) {
                    *out << "[" << timestamp_str << "] ";
                }
                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                *out << '\n';
                break;
            case output_mode::json: {
                // Escape payload for JSON
                std::string escaped;
                escaped.reserve(output_payload.size());
                for (char c : output_payload) {
                    switch (c) {
                        case '"': escaped += "\\\""; break;
                        case '\\': escaped += "\\\\"; break;
                        case '\n': escaped += "\\n"; break;
                        case '\r': escaped += "\\r"; break;
                        case '\t': escaped += "\\t"; break;
                        default:
                            if (static_cast<unsigned char>(c) < 32) {
                                escaped += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                            } else {
                                escaped += c;
                            }
                    }
                }
                *out << "{";
                if (m_show_timestamp) {
                    *out << "\"timestamp\":\"" << timestamp_str << "\",";
                }
                *out << "\"subject\":\"" << subject << "\"";
                if (reply_to) {
                    *out << ",\"reply_to\":\"" << *reply_to << "\"";
                }
                *out << ",\"payload\":\"" << escaped << "\"}\n";
                break;
            }
            case output_mode::normal:
                if (m_show_timestamp) {
                    *out << "[" << timestamp_str << "] ";
                }
                *out << "[" << subject << "] ";
                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                *out << '\n';
                break;
            case output_mode::none:
                break;
        }

        if (m_dump_file) {
            m_dump_file->flush();
        }

        co_return;
    }

private:
    output_mode m_output_mode;
    std::string m_translate_cmd;
    bool m_show_timestamp;
    std::unique_ptr<std::ofstream> m_dump_file;
};

// JetStream subscriber using push consumer
class js_grubber : public worker {
public:
    js_grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
               output_mode mode, bool auto_ack, const std::string& dump_file = {},
               const std::string& translate_cmd = {})
        : worker(ioc, console, stats_interval), m_output_mode(mode), m_auto_ack(auto_ack),
          m_translate_cmd(translate_cmd) {
        if (!dump_file.empty()) {
            m_dump_file = std::make_unique<std::ofstream>(dump_file, std::ios::binary);
            if (!m_dump_file->is_open()) {
                console->error("Failed to open dump file: {}", dump_file);
                m_dump_file.reset();
            }
        }
    }

    asio::awaitable<void> on_js_message(nats_asio::ijs_subscription& sub,
                                         const nats_asio::js_message& msg) {
        m_counter++;

        std::ostream* out = m_dump_file ? m_dump_file.get() : &std::cout;
        const auto& payload = msg.msg.payload;
        const auto& subject = msg.msg.subject;

        // Apply translation if configured (runs on background thread)
        std::string translated;
        std::span<const char> output_payload(payload.data(), payload.size());
        if (!m_translate_cmd.empty()) {
            std::string subj_str(subject);
            std::vector<char> payload_copy(payload.begin(), payload.end());
            auto log = m_log;
            std::string cmd = m_translate_cmd;
            translated = co_await async_run_blocking([cmd, subj_str, payload_copy, log]() {
                return translate_payload(cmd, subj_str, std::span<const char>(payload_copy), log);
            });
            output_payload = std::span<const char>(translated.data(), translated.size());
        }

        switch (m_output_mode) {
            case output_mode::raw:
                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                *out << '\n';
                break;
            case output_mode::json: {
                std::string escaped;
                escaped.reserve(output_payload.size());
                for (char c : output_payload) {
                    switch (c) {
                        case '"': escaped += "\\\""; break;
                        case '\\': escaped += "\\\\"; break;
                        case '\n': escaped += "\\n"; break;
                        case '\r': escaped += "\\r"; break;
                        case '\t': escaped += "\\t"; break;
                        default:
                            if (static_cast<unsigned char>(c) < 32) {
                                escaped += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                            } else {
                                escaped += c;
                            }
                    }
                }
                *out << "{\"subject\":\"" << subject << "\""
                     << ",\"stream\":\"" << msg.stream << "\""
                     << ",\"seq\":" << msg.stream_sequence
                     << ",\"payload\":\"" << escaped << "\"}\n";
                break;
            }
            case output_mode::normal:
                *out << "[" << subject << "] ";
                out->write(output_payload.data(), static_cast<std::streamsize>(output_payload.size()));
                *out << '\n';
                m_log->debug("stream={} consumer={} seq={}/{} delivered={}",
                            msg.stream, msg.consumer, msg.stream_sequence,
                            msg.consumer_sequence, msg.num_delivered);
                break;
            case output_mode::none:
                break;
        }

        if (m_dump_file) {
            m_dump_file->flush();
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
    output_mode m_output_mode;
    bool m_auto_ack;
    std::string m_translate_cmd;
    std::unique_ptr<std::ofstream> m_dump_file;
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
              int js_timeout_ms = 5000, bool wait_for_ack = true,
              const nats_asio::headers_t& headers = {}, const std::string& reply_to = {},
              int count = 0, int sleep_ms = 0, const std::string& data = {},
              const input_config& in_cfg = {},
              const input_source_config& src_cfg = {})
        : worker(ioc, console, stats_interval), m_connections(std::move(connections)),
          m_topic(topic), m_next_conn(0), m_in_flight(0), m_max_in_flight(max_in_flight),
          m_jetstream(jetstream), m_js_timeout(std::chrono::milliseconds(js_timeout_ms)),
          m_wait_for_ack(wait_for_ack), m_headers(headers), m_reply_to(reply_to),
          m_count(count), m_sleep_ms(sleep_ms), m_data(data), m_input_cfg(in_cfg),
          m_src_cfg(src_cfg), m_input_reader(ioc, src_cfg, console) {
        asio::co_spawn(ioc, read_and_publish(), asio::detached);
    }

    asio::awaitable<void> read_and_publish() {
        // Wait until at least one connection is ready
        while (!has_connected_connection()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
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
        } else {
            // Initialize input reader (file/stdin)
            if (!m_input_reader.init()) {
                m_log->error("Failed to initialize input reader");
                m_ioc.stop();
                co_return;
            }

            // Read from stdin or file
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
        m_log->info("Input complete, waiting for {} in-flight publishes", m_in_flight.load());
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

        co_await publish_message(subject, payload);
    }

    asio::awaitable<void> publish_message(const std::string& payload) {
        co_await publish_message(apply_count_template(m_topic, ++m_msg_number), payload);
    }

    asio::awaitable<void> publish_message(const std::string& subject, const std::string& payload) {
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
    nats_asio::headers_t m_headers;
    std::string m_reply_to;
    int m_count;
    int m_sleep_ms;
    std::string m_data;
    input_config m_input_cfg;
    input_source_config m_src_cfg;
    std::size_t m_msg_number = 0;
    async_input_reader m_input_reader;
    std::unique_ptr<async_http_reader> m_http_reader;
};

// Requester - sends requests and waits for replies
class requester : public worker {
public:
    requester(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              nats_asio::iconnection_sptr conn, const std::string& topic,
              int stats_interval, int timeout_ms, const std::string& data,
              output_mode mode, const nats_asio::headers_t& headers = {})
        : worker(ioc, console, stats_interval), m_conn(std::move(conn)),
          m_topic(topic), m_timeout(std::chrono::milliseconds(timeout_ms)),
          m_data(data), m_output_mode(mode), m_headers(headers),
          m_stdin(ioc, ::dup(STDIN_FILENO)) {
        asio::co_spawn(ioc, run(), asio::detached);
    }

    asio::awaitable<void> run() {
        // Wait for connection
        while (!m_conn->is_connected()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        if (!m_data.empty()) {
            // Single request with provided data
            co_await send_request(m_data);
        } else {
            // Read requests from stdin
            asio::streambuf buf;
            for (;;) {
                auto [ec, bytes_read] = co_await asio::async_read_until(
                    m_stdin, buf, '\n', asio::as_tuple(asio::use_awaitable));

                if (ec) {
                    if (ec == asio::error::eof || ec == asio::error::not_found) {
                        break;
                    }
                    m_log->error("stdin read error: {}", ec.message());
                    break;
                }

                std::string line;
                std::istream is(&buf);
                std::getline(is, line);

                if (!line.empty()) {
                    co_await send_request(line);
                }
            }
        }

        m_log->info("Requester finished, {} requests sent", m_counter);
        m_ioc.stop();
        co_return;
    }

private:
    asio::awaitable<void> send_request(const std::string& payload) {
        std::span<const char> payload_span(payload.data(), payload.size());
        auto [reply, status] = m_headers.empty()
            ? co_await m_conn->request(m_topic, payload_span, m_timeout)
            : co_await m_conn->request(m_topic, payload_span, m_headers, m_timeout);

        if (status.failed()) {
            m_log->error("request failed: {}", status.error());
            co_return;
        }

        m_counter++;

        switch (m_output_mode) {
            case output_mode::raw:
                std::cout.write(reply.payload.data(), reply.payload.size());
                std::cout << std::endl;
                break;
            case output_mode::json: {
                nlohmann::json j;
                j["subject"] = reply.subject;
                if (reply.reply_to) j["reply_to"] = *reply.reply_to;
                j["payload"] = std::string(reply.payload.begin(), reply.payload.end());
                std::cout << j.dump() << std::endl;
                break;
            }
            case output_mode::normal:
            default:
                std::cout << "[" << reply.subject << "] ";
                std::cout.write(reply.payload.data(), reply.payload.size());
                std::cout << std::endl;
                break;
        }

        co_return;
    }

    nats_asio::iconnection_sptr m_conn;
    std::string m_topic;
    std::chrono::milliseconds m_timeout;
    std::string m_data;
    output_mode m_output_mode;
    nats_asio::headers_t m_headers;
    asio::posix::stream_descriptor m_stdin;
};

// Replier - subscribes and replies to requests
class replier : public worker {
public:
    replier(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
            nats_asio::iconnection_sptr conn, const std::string& topic,
            int stats_interval, const std::string& data, bool echo_mode,
            const std::string& translate_cmd, const std::string& queue_group,
            output_mode mode)
        : worker(ioc, console, stats_interval), m_conn(std::move(conn)),
          m_topic(topic), m_data(data), m_echo_mode(echo_mode),
          m_translate_cmd(translate_cmd), m_queue_group(queue_group),
          m_output_mode(mode) {}

    asio::awaitable<void> start() {
        // Wait for connection
        while (!m_conn->is_connected()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        nats_asio::subscribe_options sub_opts;
        if (!m_queue_group.empty()) {
            sub_opts.queue_group = m_queue_group;
        }

        auto [sub, status] = co_await m_conn->subscribe(
            m_topic,
            [this](nats_asio::string_view subject, nats_asio::optional<nats_asio::string_view> reply_to,
                   std::span<const char> payload) -> asio::awaitable<void> {
                nats_asio::message msg;
                msg.subject = std::string(subject);
                if (reply_to) {
                    msg.reply_to = std::string(*reply_to);
                }
                msg.payload.assign(payload.begin(), payload.end());
                co_await handle_message(msg);
            },
            sub_opts);

        if (status.failed()) {
            m_log->error("subscribe failed: {}", status.error());
            m_ioc.stop();
            co_return;
        }

        m_log->info("Replier listening on '{}'{}", m_topic,
                   m_queue_group.empty() ? "" : " (queue: " + m_queue_group + ")");
        co_return;
    }

private:
    asio::awaitable<void> handle_message(const nats_asio::message& msg) {
        m_counter++;

        // Print received message based on output mode
        switch (m_output_mode) {
            case output_mode::raw:
                std::cout.write(msg.payload.data(), msg.payload.size());
                std::cout << std::endl;
                break;
            case output_mode::json: {
                nlohmann::json j;
                j["subject"] = msg.subject;
                if (msg.reply_to) j["reply_to"] = *msg.reply_to;
                j["payload"] = std::string(msg.payload.begin(), msg.payload.end());
                std::cout << j.dump() << std::endl;
                break;
            }
            case output_mode::normal:
                std::cout << "[" << msg.subject << "] ";
                std::cout.write(msg.payload.data(), msg.payload.size());
                std::cout << std::endl;
                break;
            case output_mode::none:
                break;
        }

        // Only reply if there's a reply_to subject
        if (!msg.reply_to || msg.reply_to->empty()) {
            co_return;
        }

        std::string reply_payload;

        if (m_echo_mode) {
            // Echo mode - reply with received payload
            reply_payload = std::string(msg.payload.begin(), msg.payload.end());
        } else if (!m_translate_cmd.empty()) {
            // Transform through external command (runs on thread pool to avoid blocking)
            std::string cmd = m_translate_cmd;
            // Replace {{Subject}} placeholder if present
            std::string subject_placeholder = "{{Subject}}";
            size_t pos = cmd.find(subject_placeholder);
            if (pos != std::string::npos) {
                cmd.replace(pos, subject_placeholder.length(), msg.subject);
            }

            // Copy payload for thread-safe access
            std::vector<char> payload_copy(msg.payload.begin(), msg.payload.end());

            // Run blocking subprocess on background thread
            reply_payload = co_await async_run_blocking([this, cmd, payload_copy]() {
                return run_translate_command(cmd, std::span<const char>(payload_copy));
            });

            if (reply_payload.empty()) {
                m_log->warn("translate command returned empty output");
            }
        } else if (!m_data.empty()) {
            // Fixed reply data
            reply_payload = m_data;
        } else {
            // Default empty reply
            reply_payload = "";
        }

        std::span<const char> reply_span(reply_payload.data(), reply_payload.size());
        auto status = co_await m_conn->publish(*msg.reply_to, reply_span, std::nullopt);

        if (status.failed()) {
            m_log->error("reply failed: {}", status.error());
        }

        co_return;
    }

    std::string run_translate_command(const std::string& cmd, std::span<const char> input) {
        int pipe_in[2], pipe_out[2];
        if (pipe(pipe_in) < 0 || pipe(pipe_out) < 0) {
            m_log->error("pipe creation failed");
            return "";
        }

        pid_t pid = fork();
        if (pid < 0) {
            m_log->error("fork failed");
            close(pipe_in[0]); close(pipe_in[1]);
            close(pipe_out[0]); close(pipe_out[1]);
            return "";
        }

        if (pid == 0) {
            // Child process
            close(pipe_in[1]);
            close(pipe_out[0]);
            dup2(pipe_in[0], STDIN_FILENO);
            dup2(pipe_out[1], STDOUT_FILENO);
            close(pipe_in[0]);
            close(pipe_out[1]);
            execl("/bin/sh", "sh", "-c", cmd.c_str(), nullptr);
            _exit(1);
        }

        // Parent process
        close(pipe_in[0]);
        close(pipe_out[1]);

        // Write input to child
        if (!input.empty()) {
            ssize_t written = ::write(pipe_in[1], input.data(), input.size());
            if (written < 0 || static_cast<size_t>(written) != input.size()) {
                m_log->warn("translate command: incomplete write to stdin");
            }
        }
        close(pipe_in[1]);

        // Read output from child
        std::string result;
        char buf[4096];
        ssize_t n;
        while ((n = ::read(pipe_out[0], buf, sizeof(buf))) > 0) {
            result.append(buf, n);
        }
        close(pipe_out[0]);

        // Wait for child
        int status;
        waitpid(pid, &status, 0);

        // Trim trailing newline
        while (!result.empty() && (result.back() == '\n' || result.back() == '\r')) {
            result.pop_back();
        }

        return result;
    }

    nats_asio::iconnection_sptr m_conn;
    std::string m_topic;
    std::string m_data;
    bool m_echo_mode;
    std::string m_translate_cmd;
    std::string m_queue_group;
    output_mode m_output_mode;
};

// Benchmarker - measures pub/sub throughput
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
        ("batch_pub", "Use high-performance batch publishing (pub mode)")
        ("batch_size", "Batch read size in bytes for batch_pub mode (default: 65536)", cxxopts::value<int>())
        ("max_queue", "Max batches in queue for batch_pub mode (default: 100)", cxxopts::value<int>())
        ("flush_timeout", "Flush timeout in ms for batch_pub mode - flush partial batch if no data arrives within timeout (default: 0 = disabled)", cxxopts::value<int>())
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
        ("queue", "Queue group for subscribe (grub/js_grub mode)", cxxopts::value<std::string>())
        ("max_msgs", "Auto-unsubscribe after N messages (grub mode)", cxxopts::value<uint32_t>())
        ("raw", "Output raw payload only (grub/js_grub mode)")
        ("dump", "Dump messages to file (grub/js_grub mode)", cxxopts::value<std::string>())
        ("json", "Output messages as JSON (grub/js_grub mode)")
        ("translate", "Transform payload through external command (supports {{Subject}})", cxxopts::value<std::string>())
        ("data", "Payload data for req/reply mode (if not provided, reads from stdin)", cxxopts::value<std::string>())
        ("timeout", "Request timeout in ms for req mode (default: 5000)", cxxopts::value<int>())
        ("echo", "Echo mode for reply - reply with received payload")
        ("H,header", "Add header to message (format: Key:Value, repeatable)", cxxopts::value<std::vector<std::string>>())
        ("reply_to", "Custom reply-to subject for pub mode", cxxopts::value<std::string>())
        ("t,timestamp", "Show timestamps in subscribe output")
        ("count", "Number of messages to publish (pub mode, default: 1)", cxxopts::value<int>())
        ("sleep", "Sleep interval between publishes in ms (pub mode)", cxxopts::value<int>())
        ("pub_size", "Message size in bytes for bench mode (default: 128)", cxxopts::value<int>())
        ("bench_rtt", "Bench mode: measure round-trip latency using request-reply")
        ("bench_batch", "Bench mode: messages per batch for pipelined publishing (default: 1000)", cxxopts::value<int>())
        ("input_format", "Input format: line (default), json, csv", cxxopts::value<std::string>())
        ("subject_template", "Subject template with {{field}} placeholders (requires json/csv input)", cxxopts::value<std::string>())
        ("payload_fields", "Comma-separated fields to include in payload (default: all)", cxxopts::value<std::string>())
        ("csv_headers", "Comma-separated header names for CSV input", cxxopts::value<std::string>())
        ("compress", "Compress payloads with zstd (pub/batch_pub mode)")
        ("decompress", "Decompress zstd payloads (grub mode)")
        ("compress_level", "Compression level 1-22 (default: 3)", cxxopts::value<int>())
        ("file", "Read input from file instead of stdin", cxxopts::value<std::string>())
        ("follow,f", "Follow mode - continuously read new data (like tail -f)")
        ("poll_interval", "Poll interval in ms for follow mode (default: 100)", cxxopts::value<int>())
        ("http", "HTTP URL to read streaming data from (e.g., Feldera output)", cxxopts::value<std::string>())
        ("http_method", "HTTP method: GET or POST (default: POST)", cxxopts::value<std::string>())
        ("http_body", "HTTP request body for POST method", cxxopts::value<std::string>())
        ("http_header", "HTTP header (format: Key:Value, repeatable)", cxxopts::value<std::vector<std::string>>())
        ("http_insecure", "Skip SSL certificate verification for HTTPS")
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
            mode != req_mode && mode != reply_mode && mode != bench_mode &&
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
        } else if (mode == req_mode) {
            m = mode::requester;
            publish_interval = -1;
        } else if (mode == reply_mode) {
            m = mode::replier;
            publish_interval = -1;
        } else if (mode == bench_mode) {
            m = mode::benchmarker;
            publish_interval = -1;
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

        // Subscribe options for grub mode
        std::string queue_group;
        uint32_t max_msgs = 0;
        output_mode out_mode = print_to_stdout ? output_mode::normal : output_mode::none;
        std::string dump_file;

        if (result.count("queue")) {
            queue_group = result["queue"].as<std::string>();
        }
        if (result.count("max_msgs")) {
            max_msgs = result["max_msgs"].as<uint32_t>();
        }
        if (result.count("raw")) {
            out_mode = output_mode::raw;
        }
        if (result.count("json")) {
            out_mode = output_mode::json;
        }
        if (result.count("dump")) {
            dump_file = result["dump"].as<std::string>();
            // If dump is specified but no output mode, default to normal
            if (out_mode == output_mode::none) {
                out_mode = output_mode::normal;
            }
        }

        std::string translate_cmd;
        if (result.count("translate")) {
            translate_cmd = result["translate"].as<std::string>();
            // If translate is specified but no output mode, default to raw
            if (out_mode == output_mode::none) {
                out_mode = output_mode::raw;
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
        std::shared_ptr<requester> req_ptr;
        std::shared_ptr<replier> reply_ptr;
        std::shared_ptr<benchmarker> bench_ptr;
        std::shared_ptr<batch_publisher> batch_pub_ptr;
        std::shared_ptr<js_grubber> js_grub_ptr;
        std::shared_ptr<js_fetcher> js_fetch_ptr;
        std::shared_ptr<kv_publisher> kv_pub_ptr;
        std::shared_ptr<kv_watcher_handler> kv_watch_ptr;
        nats_asio::iconnection_sptr conn;
        std::vector<nats_asio::iconnection_sptr> pub_connections;

        // Parse timestamp option
        bool show_timestamp = result.count("timestamp") > 0;

        // Parse headers (format: Key:Value)
        nats_asio::headers_t headers;
        if (result.count("header")) {
            auto header_strs = result["header"].as<std::vector<std::string>>();
            for (const auto& h : header_strs) {
                auto colon_pos = h.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = h.substr(0, colon_pos);
                    std::string value = h.substr(colon_pos + 1);
                    // Trim leading whitespace from value
                    while (!value.empty() && (value[0] == ' ' || value[0] == '\t')) {
                        value.erase(0, 1);
                    }
                    headers.emplace_back(key, value);
                } else {
                    console->warn("Invalid header format (expected Key:Value): {}", h);
                }
            }
        }

        // Parse reply_to, count, sleep options
        std::string custom_reply_to;
        int pub_count = 0;
        int pub_sleep_ms = 0;
        std::string pub_data;
        if (result.count("reply_to")) {
            custom_reply_to = result["reply_to"].as<std::string>();
        }
        if (result.count("count")) {
            pub_count = result["count"].as<int>();
        }
        if (result.count("sleep")) {
            pub_sleep_ms = result["sleep"].as<int>();
        }
        if (result.count("data")) {
            pub_data = result["data"].as<std::string>();
        }

        // Parse input format options for publisher
        input_config in_cfg;
        if (result.count("input_format")) {
            std::string fmt = result["input_format"].as<std::string>();
            if (fmt == "json") {
                in_cfg.format = input_format::json;
            } else if (fmt == "csv") {
                in_cfg.format = input_format::csv;
            } else if (fmt != "line") {
                console->warn("Unknown input format '{}', using 'line'", fmt);
            }
        }
        if (result.count("subject_template")) {
            in_cfg.subject_template = result["subject_template"].as<std::string>();
        }
        if (result.count("payload_fields")) {
            in_cfg.payload_fields = split_string(result["payload_fields"].as<std::string>(), ',');
        }
        if (result.count("csv_headers")) {
            in_cfg.csv_headers = split_string(result["csv_headers"].as<std::string>(), ',');
        }

        // Parse input source options (file vs stdin, follow mode, HTTP)
        input_source_config src_cfg;
        if (result.count("file")) {
            src_cfg.file_path = result["file"].as<std::string>();
        }
        src_cfg.follow = result.count("follow") > 0;
        if (result.count("poll_interval")) {
            src_cfg.poll_interval_ms = result["poll_interval"].as<int>();
        }

        // HTTP source options
        if (result.count("http")) {
            src_cfg.http_url = result["http"].as<std::string>();
        }
        if (result.count("http_method")) {
            src_cfg.http_method = result["http_method"].as<std::string>();
        }
        if (result.count("http_body")) {
            src_cfg.http_body = result["http_body"].as<std::string>();
        }
        src_cfg.http_insecure = result.count("http_insecure") > 0;
        if (result.count("http_header")) {
            auto http_headers = result["http_header"].as<std::vector<std::string>>();
            for (const auto& h : http_headers) {
                auto colon_pos = h.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = h.substr(0, colon_pos);
                    std::string value = h.substr(colon_pos + 1);
                    // Trim leading whitespace from value
                    while (!value.empty() && (value[0] == ' ' || value[0] == '\t')) {
                        value.erase(0, 1);
                    }
                    src_cfg.http_headers.emplace_back(key, value);
                } else {
                    console->warn("Invalid HTTP header format (expected Key:Value): {}", h);
                }
            }
        }

        if (m == mode::grubber) {
            grub_ptr = std::make_shared<grubber>(ioc, console, stats_interval, out_mode, dump_file, translate_cmd, show_timestamp);
        } else if (m == mode::js_grubber) {
            js_grub_ptr = std::make_shared<js_grubber>(ioc, console, stats_interval, out_mode, auto_ack, dump_file, translate_cmd);
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
                        nats_asio::subscribe_options sub_opts;
                        if (!queue_group.empty()) {
                            sub_opts.queue_group = queue_group;
                        }
                        sub_opts.max_messages = max_msgs;

                        auto r = co_await conn->subscribe(
                            topic,
                            [grub_ptr](auto v1, auto v2, auto v3) -> asio::awaitable<void> {
                                return grub_ptr->on_message(v1, v2, v3);
                            },
                            sub_opts);

                        if (r.second.failed()) {
                            console->error("failed to subscribe with error: {}", r.second.error());
                        } else if (max_msgs > 0) {
                            console->info("subscribed to {} (will auto-unsubscribe after {} messages)", topic, max_msgs);
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

            if (use_batch_pub && use_jetstream && result.count("no_ack") == 0) {
                console->error("--batch_pub with --jetstream requires --no_ack (fire-and-forget mode)");
                return 1;
            }

            if (use_batch_pub) {
                // Multi-threaded batch publishing mode
                int batch_size_val = result.count("batch_size") ? result["batch_size"].as<int>() : 65536;
                int max_queue_val = result.count("max_queue") ? result["max_queue"].as<int>() : 100;
                int flush_timeout_val = result.count("flush_timeout") ? result["flush_timeout"].as<int>() : 0;
                console->info("Using multi-threaded batch publisher: {} writers, batch_size={}, max_queue={}",
                             num_connections, batch_size_val, max_queue_val);
                batch_pub_ptr = std::make_shared<batch_publisher>(console, conf, opt_ssl_conf, topic,
                                                                   num_connections, stats_interval,
                                                                   static_cast<std::size_t>(batch_size_val),
                                                                   static_cast<std::size_t>(max_queue_val),
                                                                   flush_timeout_val,
                                                                   src_cfg.file_path);
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
                                                       max_in_flight, use_jetstream, js_timeout_ms, wait_for_ack,
                                                       headers, custom_reply_to, pub_count, pub_sleep_ms, pub_data,
                                                       in_cfg, src_cfg);
            }
        } else if (m == mode::benchmarker) {
            int bench_count = result.count("count") ? result["count"].as<int>() : 100000;
            int bench_size = result.count("pub_size") ? result["pub_size"].as<int>() : 128;
            bool use_js = result.count("jetstream") > 0;
            bool bench_rtt = result.count("bench_rtt") > 0;
            int bench_timeout = result.count("timeout") ? result["timeout"].as<int>() : 5000;
            int bench_batch = result.count("bench_batch") ? result["bench_batch"].as<int>() : 1000;

            // Create connections for benchmark (use --connections option)
            std::vector<nats_asio::iconnection_sptr> bench_connections;
            for (int i = 0; i < num_connections; i++) {
                auto c = make_connection(i);
                c->start(conf);
                bench_connections.push_back(c);
            }
            console->info("Benchmark using {} connection(s)", num_connections);

            bench_ptr = std::make_shared<benchmarker>(ioc, console, bench_connections, topic, stats_interval,
                                                       bench_count, bench_size, use_js, bench_rtt, bench_timeout, bench_batch);
            asio::co_spawn(ioc, bench_ptr->run(), asio::detached);
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
            } else if (m == mode::requester) {
                int req_timeout = result.count("timeout") ? result["timeout"].as<int>() : 5000;
                std::string req_data = result.count("data") ? result["data"].as<std::string>() : "";
                req_ptr = std::make_shared<requester>(ioc, console, conn, topic, stats_interval,
                                                      req_timeout, req_data, out_mode, headers);
            } else if (m == mode::replier) {
                std::string reply_data = result.count("data") ? result["data"].as<std::string>() : "";
                bool echo_mode = result.count("echo") > 0;
                reply_ptr = std::make_shared<replier>(ioc, console, conn, topic, stats_interval,
                                                      reply_data, echo_mode, translate_cmd, queue_group, out_mode);
                asio::co_spawn(ioc, reply_ptr->start(), asio::detached);
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
