#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <fstream>
#include <iostream>
#include <nats_asio/nats_asio.hpp>
#include <tuple>
#include <unistd.h>

#include "cxxopts.hpp"

const std::string grub_mode("grub");
const std::string gen_mode("gen");
const std::string pub_mode("pub");
const std::string js_grub_mode("js_grub");
const std::string js_fetch_mode("js_fetch");
const std::string pubkv_mode("pubkv");

enum class mode { grubber, generator, publisher, js_grubber, js_fetcher, kv_publisher };

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

class publisher : public worker {
public:
    publisher(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
              std::vector<nats_asio::iconnection_sptr> connections, const std::string& topic,
              int stats_interval, int max_in_flight = 1000, bool jetstream = false,
              int js_timeout_ms = 5000)
        : worker(ioc, console, stats_interval), m_connections(std::move(connections)),
          m_topic(topic), m_next_conn(0), m_in_flight(0), m_max_in_flight(max_in_flight),
          m_jetstream(jetstream), m_js_timeout(std::chrono::milliseconds(js_timeout_ms)),
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
                        auto [ack, s] = co_await conn->js_publish(m_topic, payload_span, m_js_timeout);
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
    asio::posix::stream_descriptor m_stdin;
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
        ("js,jetstream", "Use JetStream publish with acknowledgment (pub mode only)")
        ("js_timeout", "JetStream publish timeout in ms (default: 5000)", cxxopts::value<int>())
        ("stream", "JetStream stream name (js_grub/js_fetch mode)", cxxopts::value<std::string>())
        ("consumer", "JetStream consumer name (js_grub/js_fetch mode)", cxxopts::value<std::string>())
        ("durable", "Durable consumer name (js_grub mode)", cxxopts::value<std::string>())
        ("batch", "Batch size for js_fetch mode (default: 10)", cxxopts::value<int>())
        ("fetch_interval", "Interval between fetches in ms (default: 100)", cxxopts::value<int>())
        ("auto_ack", "Auto-acknowledge messages (js_grub mode)", cxxopts::value<bool>())
        ("bucket", "KV bucket name (pubkv mode)", cxxopts::value<std::string>())
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
            mode != js_grub_mode && mode != js_fetch_mode && mode != pubkv_mode) {
            console->error("Invalid mode. Could be `{}`, `{}`, `{}`, `{}`, `{}`, or `{}`",
                          grub_mode, gen_mode, pub_mode, js_grub_mode, js_fetch_mode, pubkv_mode);
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
        std::string kv_separator = "|";
        int kv_timeout_ms = 5000;

        if (m == mode::kv_publisher) {
            if (!result.count("bucket")) {
                console->error("pubkv mode requires --bucket");
                return 1;
            }
            kv_bucket = result["bucket"].as<std::string>();

            if (result.count("separator")) {
                kv_separator = result["separator"].as<std::string>();
            }
            if (result.count("kv_timeout")) {
                kv_timeout_ms = result["kv_timeout"].as<int>();
            }
        }

        asio::io_context ioc;
        std::shared_ptr<grubber> grub_ptr;
        std::shared_ptr<generator> gen_ptr;
        std::shared_ptr<publisher> pub_ptr;
        std::shared_ptr<js_grubber> js_grub_ptr;
        std::shared_ptr<js_fetcher> js_fetch_ptr;
        std::shared_ptr<kv_publisher> kv_pub_ptr;
        nats_asio::iconnection_sptr conn;
        std::vector<nats_asio::iconnection_sptr> pub_connections;

        if (m == mode::grubber) {
            grub_ptr = std::make_shared<grubber>(ioc, console, stats_interval, print_to_stdout);
        } else if (m == mode::js_grubber) {
            js_grub_ptr = std::make_shared<js_grubber>(ioc, console, stats_interval, print_to_stdout, auto_ack);
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
            // Create multiple connections for publisher mode
            console->info("Creating {} connections for pub mode", num_connections);
            for (int i = 0; i < num_connections; i++) {
                auto c = make_connection(i);
                c->start(conf);
                pub_connections.push_back(c);
            }
            bool use_jetstream = result.count("jetstream") > 0;
            int js_timeout_ms = result.count("js_timeout") ? result["js_timeout"].as<int>() : 5000;
            pub_ptr = std::make_shared<publisher>(ioc, console, pub_connections, topic, stats_interval,
                                                   max_in_flight, use_jetstream, js_timeout_ms);
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
