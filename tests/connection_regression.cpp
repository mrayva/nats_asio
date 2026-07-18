#include <gtest/gtest.h>

#include <algorithm>
#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <nats_asio/nats_asio.hpp>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <thread>

using namespace nats_asio;

namespace {

std::string read_line(asio::ip::tcp::socket& socket, asio::streambuf& buffer,
                      asio::error_code& ec) {
    asio::read_until(socket, buffer, "\r\n", ec);
    if (ec)
        return {};
    std::istream input(&buffer);
    std::string line;
    std::getline(input, line);
    if (!line.empty() && line.back() == '\r')
        line.pop_back();
    return line;
}

void send_info(asio::ip::tcp::socket& socket, asio::error_code& ec) {
    static const std::string info = "INFO {\"max_payload\":1048576,\"headers\":true}\r\n";
    asio::write(socket, asio::buffer(info), ec);
}

std::pair<std::string, std::string> parse_sub(const std::string& line) {
    std::istringstream input(line);
    std::string op;
    std::string subject;
    std::string sid;
    input >> op >> subject >> sid;
    return {subject, sid};
}

} // namespace

TEST(connection_protocol, advertises_headers_and_formats_no_reply_publish) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(server_ioc,
                                     asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto port = acceptor.local_endpoint().port();
    std::atomic<bool> server_ok{false};

    std::thread server([&] {
        asio::error_code ec;
        asio::ip::tcp::socket socket(server_ioc);
        acceptor.accept(socket, ec);
        if (ec)
            return;
        send_info(socket, ec);
        asio::streambuf buffer;
        const auto connect = read_line(socket, buffer, ec);
        const auto publish = read_line(socket, buffer, ec);
        const auto payload = read_line(socket, buffer, ec);
        if (ec || !connect.starts_with("CONNECT "))
            return;

        const auto options = nlohmann::json::parse(connect.substr(8));
        server_ok.store(options.value("headers", false) && options.value("protocol", 0) == 1 &&
                            publish == "PUB events 3" && payload == "abc",
                        std::memory_order_release);
    });

    asio::io_context client_ioc;
    asio::steady_timer watchdog(client_ioc);
    std::atomic<bool> publish_ok{false};
    auto conn = create_connection(
        client_ioc,
        [&](iconnection& connection) -> asio::awaitable<void> {
            const std::string payload = "abc";
            auto status = co_await connection.publish(
                "events", std::span<const char>(payload.data(), payload.size()), std::nullopt);
            publish_ok.store(status.ok(), std::memory_order_release);
            watchdog.cancel();
            connection.stop();
        },
        [](iconnection&) -> asio::awaitable<void> { co_return; },
        [](iconnection&, string_view) -> asio::awaitable<void> { co_return; }, std::nullopt);

    connect_config config;
    config.address = "127.0.0.1";
    config.port = port;
    conn->start(config);
    watchdog.expires_after(std::chrono::seconds(2));
    watchdog.async_wait([&](const asio::error_code& ec) {
        if (!ec)
            conn->stop();
    });
    client_ioc.run();
    server.join();

    EXPECT_TRUE(publish_ok.load(std::memory_order_acquire));
    EXPECT_TRUE(server_ok.load(std::memory_order_acquire));
}

TEST(connection_callbacks, request_from_message_callback_is_reentrant) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(server_ioc,
                                     asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto port = acceptor.local_endpoint().port();
    std::atomic<bool> server_ok{false};

    std::thread server([&] {
        asio::error_code ec;
        asio::ip::tcp::socket socket(server_ioc);
        acceptor.accept(socket, ec);
        if (ec)
            return;
        send_info(socket, ec);
        asio::streambuf buffer;
        (void)read_line(socket, buffer, ec); // CONNECT
        const auto event_sub = parse_sub(read_line(socket, buffer, ec));
        if (ec || event_sub.first != "events")
            return;

        const std::string event = "MSG events " + event_sub.second + " 0\r\n\r\n";
        asio::write(socket, asio::buffer(event), ec);

        const auto reply_sub = parse_sub(read_line(socket, buffer, ec));
        const auto request_line = read_line(socket, buffer, ec);
        (void)read_line(socket, buffer, ec); // empty request payload
        std::istringstream request(request_line);
        std::string op;
        std::string subject;
        std::string reply;
        std::size_t size = 1;
        request >> op >> subject >> reply >> size;
        if (ec || reply_sub.first != reply || subject != "service" || size != 0)
            return;

        const std::string headers = "NATS/1.0\r\nX-Test: yes\r\n\r\n";
        const std::string response =
            "HMSG " + reply + " " + reply_sub.second + " " + std::to_string(headers.size()) + " " +
            std::to_string(headers.size() + 4) + "\r\n" + headers + "done\r\n";
        asio::write(socket, asio::buffer(response), ec);
        server_ok.store(!ec, std::memory_order_release);
    });

    asio::io_context client_ioc;
    asio::steady_timer watchdog(client_ioc);
    std::atomic<bool> request_ok{false};
    auto conn = create_connection(
        client_ioc,
        [&](iconnection& connection) -> asio::awaitable<void> {
            auto [subscription, status] = co_await connection.subscribe(
                "events",
                [&](string_view, optional<string_view>,
                    std::span<const char>) -> asio::awaitable<void> {
                    auto [response, request_status] =
                        co_await connection.request("service", {}, std::chrono::seconds(1));
                    const bool header_ok = std::any_of(
                        response.headers.begin(), response.headers.end(), [](const auto& header) {
                            return header.first == "X-Test" && header.second == "yes";
                        });
                    request_ok.store(
                        request_status.ok() && header_ok &&
                            std::string(response.payload.begin(), response.payload.end()) == "done",
                        std::memory_order_release);
                    watchdog.cancel();
                    connection.stop();
                });
            if (status.failed())
                connection.stop();
            (void)subscription;
        },
        [](iconnection&) -> asio::awaitable<void> { co_return; },
        [](iconnection&, string_view) -> asio::awaitable<void> { co_return; }, std::nullopt);

    connect_config config;
    config.address = "127.0.0.1";
    config.port = port;
    conn->start(config);
    watchdog.expires_after(std::chrono::seconds(2));
    watchdog.async_wait([&](const asio::error_code& ec) {
        if (!ec)
            conn->stop();
    });
    client_ioc.run();
    server.join();

    EXPECT_TRUE(server_ok.load(std::memory_order_acquire));
    EXPECT_TRUE(request_ok.load(std::memory_order_acquire));
}

TEST(connection_callbacks, request_from_connected_callback_is_reentrant) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(
        server_ioc, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto port = acceptor.local_endpoint().port();
    std::atomic<bool> server_ok{false};

    std::thread server([&] {
        asio::error_code ec;
        asio::ip::tcp::socket socket(server_ioc);
        acceptor.accept(socket, ec);
        if (ec) return;
        send_info(socket, ec);
        asio::streambuf buffer;
        (void)read_line(socket, buffer, ec); // CONNECT
        const auto reply_sub = parse_sub(read_line(socket, buffer, ec));
        const auto request_line = read_line(socket, buffer, ec);
        (void)read_line(socket, buffer, ec); // empty request payload

        std::istringstream request(request_line);
        std::string op;
        std::string subject;
        std::string reply;
        std::size_t size = 1;
        request >> op >> subject >> reply >> size;
        if (ec || reply_sub.first != reply || subject != "startup" || size != 0) return;

        const std::string response =
            "MSG " + reply + " " + reply_sub.second + " 2\r\nok\r\n";
        asio::write(socket, asio::buffer(response), ec);
        server_ok.store(!ec, std::memory_order_release);
    });

    asio::io_context client_ioc;
    asio::steady_timer watchdog(client_ioc);
    std::atomic<bool> request_ok{false};
    auto conn = create_connection(
        client_ioc,
        [&](iconnection& connection) -> asio::awaitable<void> {
            auto [response, status] =
                co_await connection.request("startup", {}, std::chrono::seconds(1));
            request_ok.store(
                status.ok() &&
                    std::string(response.payload.begin(), response.payload.end()) == "ok",
                std::memory_order_release);
            watchdog.cancel();
            connection.stop();
        },
        [](iconnection&) -> asio::awaitable<void> { co_return; },
        [](iconnection&, string_view) -> asio::awaitable<void> { co_return; },
        std::nullopt);

    connect_config config;
    config.address = "127.0.0.1";
    config.port = port;
    conn->start(config);
    watchdog.expires_after(std::chrono::seconds(2));
    watchdog.async_wait([&](const asio::error_code& ec) {
        if (!ec) conn->stop();
    });
    client_ioc.run();
    server.join();

    EXPECT_TRUE(server_ok.load(std::memory_order_acquire));
    EXPECT_TRUE(request_ok.load(std::memory_order_acquire));
}

TEST(connection_lifecycle, restores_subscription_after_reconnect) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(server_ioc,
                                     asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto port = acceptor.local_endpoint().port();
    std::atomic<bool> restored{false};

    std::thread server([&] {
        for (int attempt = 0; attempt < 2; ++attempt) {
            asio::error_code ec;
            asio::ip::tcp::socket socket(server_ioc);
            acceptor.accept(socket, ec);
            if (ec)
                return;
            send_info(socket, ec);
            asio::streambuf buffer;
            (void)read_line(socket, buffer, ec); // CONNECT
            const auto subscription = parse_sub(read_line(socket, buffer, ec));
            if (ec || subscription.first != "events")
                return;
            if (attempt == 1) {
                restored.store(true, std::memory_order_release);
            }
            socket.close(ec);
        }
    });

    asio::io_context client_ioc;
    asio::steady_timer watchdog(client_ioc);
    std::atomic<int> connections{0};
    auto conn = create_connection(
        client_ioc,
        [&](iconnection& connection) -> asio::awaitable<void> {
            const int count = connections.fetch_add(1, std::memory_order_acq_rel) + 1;
            if (count == 1) {
                auto [subscription, status] = co_await connection.subscribe(
                    "events",
                    [](string_view, optional<string_view>,
                       std::span<const char>) -> asio::awaitable<void> { co_return; });
                if (status.failed())
                    connection.stop();
                (void)subscription;
            } else {
                watchdog.cancel();
                connection.stop();
            }
        },
        [](iconnection&) -> asio::awaitable<void> { co_return; },
        [](iconnection&, string_view) -> asio::awaitable<void> { co_return; }, std::nullopt);

    connect_config config;
    config.address = "127.0.0.1";
    config.port = port;
    config.retry_initial_delay_ms = 1;
    config.retry_max_delay_ms = 1;
    config.retry_jitter_factor = 0;
    conn->start(config);
    watchdog.expires_after(std::chrono::seconds(2));
    watchdog.async_wait([&](const asio::error_code& ec) {
        if (!ec)
            conn->stop();
    });
    client_ioc.run();
    server.join();

    EXPECT_EQ(connections.load(std::memory_order_acquire), 2);
    EXPECT_TRUE(restored.load(std::memory_order_acquire));
}
