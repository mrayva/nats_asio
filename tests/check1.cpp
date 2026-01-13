#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <nats_asio/nats_asio.hpp>
#include <sstream>

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
        std::stringstream ss(payload);
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
        std::stringstream ss2(payload_header);
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
