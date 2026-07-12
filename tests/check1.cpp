#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <nats_asio/nats_asio.hpp>
#include <sstream>
#include <thread>

#include "../samples/include/batch_publisher.hpp"

using namespace nats_asio;

struct parser_mock : public parser_observer {
    MOCK_METHOD(asio::awaitable<void>, consumed, (std::size_t n), (override));
    MOCK_METHOD(asio::awaitable<void>, on_ok, (), (override));
    MOCK_METHOD(asio::awaitable<void>, on_pong, (), (override));
    MOCK_METHOD(asio::awaitable<void>, on_ping, (), (override));
    MOCK_METHOD(asio::awaitable<void>, on_error, (string_view err), (override));
    MOCK_METHOD(asio::awaitable<void>, on_info, (string_view info), (override));
    MOCK_METHOD(asio::awaitable<void>, on_message,
                (string_view subject, string_view sid, optional<string_view> reply_to,
                 std::size_t n),
                (override));
    MOCK_METHOD(asio::awaitable<void>, on_hmessage,
                (string_view subject, string_view sid, optional<string_view> reply_to,
                 std::size_t header_len, std::size_t total_len),
                (override));
};

// Change the signature to accept a function returning awaitable<void>
void async_process(std::function<asio::awaitable<void>()> f) {
    asio::io_context ioc;
    asio::co_spawn(ioc, f(), asio::detached);
    ioc.run();
}

asio::awaitable<void> make_ready_awaitable() {
    co_return;
}

TEST(small_messages, ping) {
    parser_mock m;
    std::string payload("PING\r\n");
    std::string header;
    EXPECT_CALL(m, on_ping()).WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s.failed());
        co_return;
    });
}

TEST(small_messages, pong) {
    parser_mock m;
    std::string payload("PONG\r\n");
    std::string header;
    EXPECT_CALL(m, on_pong()).WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s.failed());
        co_return;
    });
}

TEST(small_messages, ok) {
    parser_mock m;
    std::string payload("+OK\r\n");
    std::string header;
    EXPECT_CALL(m, on_ok()).WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s.failed());
        co_return;
    });
}

TEST(payload_messages, err) {
    parser_mock m;
    string_view msg("some big error");
    auto payload = fmt::format("-ERR {}\r\n", msg);
    std::string header;
    EXPECT_CALL(m, on_error(msg)).WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s.failed());
        co_return;
    });
}

TEST(payload_messages, info) {
    parser_mock m;
    string_view info_msg(R"({"verbose":false,"pedantic":false,"tls_required":false})");
    auto payload = fmt::format("INFO {}\r\n", info_msg);
    std::string header;
    EXPECT_CALL(m, on_info(info_msg)).WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s.failed());
        co_return;
    });
}

TEST(payload_messages, info_with_overflow) {
    parser_mock m;
    string_view info_msg(R"({"verbose":false,"pedantic":false,"tls_required":false})");
    std::string header;
    auto payload = fmt::format("INFO {}\r\n", info_msg);
    auto payload_over = payload + "-ERR abrakadabra\r\n";
    EXPECT_CALL(m, on_info(info_msg)).WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload_over);
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s.failed());
        co_return;
    });
}

TEST(payload_messages, on_message) {
    parser_mock m;
    const char* msg = R"(subscription payload)";
    auto msg_size = strlen(msg);
    string_view sid("6789654");
    string_view subject("sub1.1");
    string_view reply_to("some_reply_to");
    std::string header;
    std::string payload = fmt::format("MSG {} {} {}\r\n{}\r\n", subject, sid, msg_size, msg);
    std::string payload2 =
        fmt::format("MSG {} {} {} {}\r\n{}\r\n", subject, sid, reply_to, msg_size, msg);

    EXPECT_CALL(m, on_message(subject, sid, optional<string_view>(), msg_size))
        .WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    EXPECT_CALL(m, on_message(subject, sid, optional<string_view>(reply_to), msg_size))
        .WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    EXPECT_CALL(m, consumed(msg_size + 2))
        .Times(2)
        .WillRepeatedly(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s1 = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s1.failed());
        std::stringstream ss2(payload2);
        auto s2 = co_await protocol_parser::parse_header(header, ss2, m);
        EXPECT_EQ(false, s2.failed());
    });
}

TEST(payload_messages, on_message_binary) {
    parser_mock m;
    string_view sid("6789654");
    string_view subject("sub1.1");
    std::size_t msg_size = 10;
    std::vector<char> binary_payload(msg_size);

    for (std::size_t i = 0; i < msg_size; ++i) {
        binary_payload[i] = static_cast<char>(i);
    }

    std::vector<char> buffer;
    auto payload_header = fmt::format("MSG {} {} {}\r\n", subject, sid, msg_size);
    std::copy(payload_header.begin(), payload_header.end(), std::back_inserter(buffer));
    std::copy(binary_payload.begin(), binary_payload.end(), std::back_inserter(buffer));
    buffer.push_back('\r');
    buffer.push_back('\n');
    std::string header;

    EXPECT_CALL(m, on_message(subject, sid, optional<string_view>(), msg_size))
        .WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    EXPECT_CALL(m, consumed(msg_size + 2))
        .WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::string payload_data(buffer.begin(), buffer.end());
        std::stringstream ss2(payload_data);
        auto s1 = co_await protocol_parser::parse_header(header, ss2, m);
        EXPECT_EQ(false, s1.failed());
    });
}

TEST(payload_messages, on_message_not_full_no_sep) {
    parser_mock m;
    std::string payload("MSG abra abra");
    std::string header;
    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s1 = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(true, s1.failed());
    });
}

TEST(payload_messages, on_message_not_full) {
    parser_mock m;
    const char* msg = R"(subscription payload)";
    auto msg_size = strlen(msg);
    string_view sid("6789654");
    string_view subject("sub1.1");
    std::string header;
    auto payload = fmt::format("MSG {} {} {}\r\n{}", subject, sid, msg_size, msg);

    EXPECT_CALL(m, on_message(subject, sid, optional<string_view>(), msg_size))
        .WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    EXPECT_CALL(m, consumed(msg_size + 2))
        .WillOnce(testing::InvokeWithoutArgs(make_ready_awaitable));

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss(payload);
        auto s1 = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_EQ(false, s1.failed());
    });
}

TEST(protocol_parser, rejects_message_larger_than_configured_limit) {
    parser_mock m;
    std::string header;

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss("MSG subject 1 11\r\n");
        auto s = co_await protocol_parser::parse_header(header, ss, m, 10);
        EXPECT_TRUE(s.failed());
        co_return;
    });
}

TEST(protocol_parser, rejects_hmessage_with_header_larger_than_total) {
    parser_mock m;
    std::string header;

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream ss("HMSG subject 1 11 10\r\n");
        auto s = co_await protocol_parser::parse_header(header, ss, m);
        EXPECT_TRUE(s.failed());
        co_return;
    });
}

TEST(protocol_parser, rejects_invalid_command_prefixes) {
    parser_mock m;
    std::string header;

    async_process([&]() -> asio::awaitable<void> {
        std::stringstream info("INFOX {}\r\n");
        auto info_status = co_await protocol_parser::parse_header(header, info, m);
        EXPECT_TRUE(info_status.failed());

        std::stringstream err("-ERRX bad\r\n");
        auto err_status = co_await protocol_parser::parse_header(header, err, m);
        EXPECT_TRUE(err_status.failed());
        co_return;
    });
}

TEST(write_queue, tracks_pending_bytes_with_concurrent_producers) {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t messages_per_producer = 10'000;
    const std::string message = "payload";
    write_queue queue;
    std::atomic<bool> start{false};
    std::atomic<std::size_t> producers_remaining{producer_count};
    std::atomic<bool> enqueue_failed{false};
    std::vector<std::thread> producers;
    producers.reserve(producer_count);

    for (std::size_t i = 0; i < producer_count; ++i) {
        producers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (std::size_t n = 0; n < messages_per_producer; ++n) {
                if (!queue.enqueue(std::string(message))) {
                    enqueue_failed.store(true, std::memory_order_relaxed);
                }
            }
            producers_remaining.fetch_sub(1, std::memory_order_release);
        });
    }

    start.store(true, std::memory_order_release);
    std::size_t consumed_bytes = 0;
    while (producers_remaining.load(std::memory_order_acquire) != 0 || !queue.empty()) {
        std::string batch;
        queue.dequeue_all(batch);
        consumed_bytes += batch.size();
        if (batch.empty()) {
            std::this_thread::yield();
        }
    }

    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_FALSE(enqueue_failed.load(std::memory_order_relaxed));
    EXPECT_EQ(consumed_bytes, producer_count * messages_per_producer * message.size());
    EXPECT_EQ(queue.pending_bytes(), 0);
}

TEST(batch_queue, full_queue_blocks_without_dropping_batch) {
    nats_tool::batch_queue queue;
    queue.set_max_size(1);
    ASSERT_TRUE(queue.push({"first", 1}));

    std::atomic<bool> second_pushed{false};
    std::thread producer([&] {
        second_pushed.store(queue.push({"second", 1}), std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(second_pushed.load(std::memory_order_acquire));

    nats_tool::batch_item item;
    ASSERT_TRUE(queue.pop(item, std::chrono::milliseconds(100)));
    EXPECT_EQ(item.data, "first");

    producer.join();
    EXPECT_TRUE(second_pushed.load(std::memory_order_acquire));
    ASSERT_TRUE(queue.pop(item, std::chrono::milliseconds(100)));
    EXPECT_EQ(item.data, "second");
    queue.set_done();
}

TEST(connection_lifetime, queued_start_and_stop_own_connection) {
    asio::io_context ioc;
    auto noop_connected = [](iconnection&) -> asio::awaitable<void> { co_return; };
    auto noop_disconnected = [](iconnection&) -> asio::awaitable<void> { co_return; };
    auto noop_error = [](iconnection&, string_view) -> asio::awaitable<void> { co_return; };

    auto conn = create_connection(
        ioc, noop_connected, noop_disconnected, noop_error, std::nullopt);
    std::weak_ptr<iconnection> weak_conn = conn;

    connect_config config;
    conn->start(config);
    conn->stop();
    conn.reset();

    // Queued lifecycle handlers retain ownership until the io_context runs them.
    EXPECT_FALSE(weak_conn.expired());
    ioc.run();
    EXPECT_TRUE(weak_conn.expired());
}

void expect_malformed_frame_disconnects(const std::string& malformed,
                                        string_view expected_error) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(
        server_ioc, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto port = acceptor.local_endpoint().port();
    std::atomic<bool> server_ok{true};

    std::thread server([&] {
        asio::error_code ec;
        asio::ip::tcp::socket socket(server_ioc);
        acceptor.accept(socket, ec);
        if (ec) {
            server_ok.store(false, std::memory_order_relaxed);
            return;
        }
        acceptor.close(ec);

        const std::string info = "INFO {\"max_payload\":1048576}\r\n";
        asio::write(socket, asio::buffer(info), ec);
        asio::streambuf connect_buf;
        asio::read_until(socket, connect_buf, "\r\n", ec);
        if (ec) {
            server_ok.store(false, std::memory_order_relaxed);
            return;
        }

        asio::write(socket, asio::buffer(malformed), ec);

        std::array<char, 1> byte{};
        socket.read_some(asio::buffer(byte), ec);
        if (!ec) {
            server_ok.store(false, std::memory_order_relaxed);
        }
    });

    asio::io_context client_ioc;
    asio::steady_timer watchdog(client_ioc);
    std::atomic<bool> protocol_error_seen{false};
    std::atomic<bool> disconnected{false};
    auto conn = create_connection(
        client_ioc,
        [](iconnection&) -> asio::awaitable<void> { co_return; },
        [&](iconnection& c) -> asio::awaitable<void> {
            disconnected.store(true, std::memory_order_relaxed);
            watchdog.cancel();
            c.stop();
            co_return;
        },
        [&](iconnection&, string_view error) -> asio::awaitable<void> {
            if (error.find(expected_error) != string_view::npos) {
                protocol_error_seen.store(true, std::memory_order_relaxed);
            }
            co_return;
        },
        std::nullopt);

    connect_config config;
    config.address = "127.0.0.1";
    config.port = port;
    config.retry_initial_delay_ms = 1;
    config.retry_max_delay_ms = 1;
    conn->start(config);

    watchdog.expires_after(std::chrono::seconds(2));
    watchdog.async_wait([&](const asio::error_code& ec) {
        if (!ec) {
            conn->stop();
        }
    });

    client_ioc.run();
    server.join();

    EXPECT_TRUE(server_ok.load(std::memory_order_relaxed));
    EXPECT_TRUE(protocol_error_seen.load(std::memory_order_relaxed));
    EXPECT_TRUE(disconnected.load(std::memory_order_relaxed));
}

TEST(connection_protocol, malformed_header_disconnects) {
    expect_malformed_frame_disconnects("BOGUS\r\n", "protocol parse failed");
}

TEST(connection_protocol, malformed_payload_terminator_disconnects) {
    expect_malformed_frame_disconnects(
        "MSG subject 1 3\r\nabcXX", "invalid message payload terminator");
}

TEST(connection_lifecycle, drain_closes_connection_from_outside_strand) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(
        server_ioc, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto port = acceptor.local_endpoint().port();
    std::atomic<bool> server_observed_close{false};

    std::thread server([&] {
        asio::error_code ec;
        asio::ip::tcp::socket socket(server_ioc);
        acceptor.accept(socket, ec);
        if (ec) {
            return;
        }
        acceptor.close(ec);

        const std::string info = "INFO {\"max_payload\":1048576}\r\n";
        asio::write(socket, asio::buffer(info), ec);
        asio::streambuf connect_buf;
        asio::read_until(socket, connect_buf, "\r\n", ec);
        if (ec) {
            return;
        }

        std::array<char, 1> byte{};
        socket.read_some(asio::buffer(byte), ec);
        server_observed_close.store(
            ec == asio::error::eof || ec == asio::error::connection_reset,
            std::memory_order_relaxed);
    });

    asio::io_context client_ioc;
    std::atomic<bool> connected{false};
    std::atomic<bool> drain_succeeded{false};
    auto conn = create_connection(
        client_ioc,
        [&](iconnection&) -> asio::awaitable<void> {
            connected.store(true, std::memory_order_release);
            co_return;
        },
        [](iconnection&) -> asio::awaitable<void> { co_return; },
        [](iconnection&, string_view) -> asio::awaitable<void> { co_return; },
        std::nullopt);

    connect_config config;
    config.address = "127.0.0.1";
    config.port = port;
    conn->start(config);

    asio::co_spawn(
        client_ioc,
        [&]() -> asio::awaitable<void> {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            while (!connected.load(std::memory_order_acquire)) {
                timer.expires_after(std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            }

            auto result = co_await conn->drain(std::chrono::milliseconds(500));
            drain_succeeded.store(result.ok(), std::memory_order_release);
            co_return;
        },
        asio::detached);

    client_ioc.run();
    server.join();

    EXPECT_TRUE(drain_succeeded.load(std::memory_order_acquire));
    EXPECT_TRUE(server_observed_close.load(std::memory_order_relaxed));
    EXPECT_FALSE(conn->is_connected());
}
