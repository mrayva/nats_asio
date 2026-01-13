#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <fstream>
#include <iostream>
#include <nats_asio/nats_asio.hpp>
#include <tuple>

#include "cxxopts.hpp"

const std::string grub_mode("grub");
const std::string gen_mode("gen");

enum class mode { grubber, generator };

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
            auto s = co_await m_conn->publish(m_topic, payload_span, {});

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

        if (topic.empty()) {
            topic = result["topic"].as<std::string>();
        }

        mode = result["mode"].as<std::string>();

        if (mode != grub_mode && mode != gen_mode) {
            console->error("Invalid mode. Could be `{}` or `{}`", grub_mode, gen_mode);
            return 1;
        }

        auto m = mode::generator;

        if (mode == grub_mode) {
            m = mode::grubber;
            publish_interval = -1;
        } else {
            if (publish_interval < 0) {
                publish_interval = 1000;
            }
        }

        asio::io_context ioc;
        std::shared_ptr<grubber> grub_ptr;
        std::shared_ptr<generator> gen_ptr;
        nats_asio::iconnection_sptr conn;

        if (m == mode::grubber) {
            grub_ptr = std::make_shared<grubber>(ioc, console, stats_interval, print_to_stdout);
        }

        conn = nats_asio::create_connection(
            ioc,
            [&](nats_asio::iconnection& /*c*/) -> asio::awaitable<void> /*nats_asio::ctx ctx)*/ {
                console->info("on connected");

                if (m == mode::grubber) {
                    auto r = co_await conn->subscribe(
                        topic,
                        [grub_ptr](auto v1, auto v2, auto v3) -> asio::awaitable<void> {
                            return grub_ptr->on_message(v1, v2, v3);
                        });

                    if (r.second.failed()) {
                        console->error("failed to subscribe with error: {}", r.second.error());
                    }
                }
                co_return;
            },
            [&console](nats_asio::iconnection&) -> asio::awaitable<void> {
                console->info("on disconnected");
                co_return;
            },
            [&console](nats_asio::iconnection&, nats_asio::string_view err) -> asio::awaitable<void> {
                console->error("on error: {}", err);
                co_return;
            },
            opt_ssl_conf);

        conn->start(conf);

        if (m == mode::generator) {
            gen_ptr = std::make_shared<generator>(ioc, console, conn, topic, stats_interval,
                                                  publish_interval);
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
