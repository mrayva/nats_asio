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

TEST(circuit_breaker, disabled_when_threshold_is_zero) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/0, /*timeout_ms=*/30000, /*half_open_max=*/3);

    for (int i = 0; i < 10; ++i) {
        cb.record_failure();
    }

    EXPECT_TRUE(cb.allow_request());
    EXPECT_EQ(cb.stats().state.load(), circuit_state::closed);
    EXPECT_EQ(cb.stats().failure_count.load(), 0u);
}

TEST(circuit_breaker, stays_closed_below_threshold) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/3, /*timeout_ms=*/30000, /*half_open_max=*/2);

    cb.record_failure();
    cb.record_failure();

    EXPECT_TRUE(cb.allow_request());
    EXPECT_EQ(cb.stats().state.load(), circuit_state::closed);
    EXPECT_EQ(cb.stats().failure_count.load(), 2u);
}

TEST(circuit_breaker, opens_at_threshold_and_rejects_requests) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/3, /*timeout_ms=*/30000, /*half_open_max=*/2);

    cb.record_failure();
    cb.record_failure();
    cb.record_failure();

    EXPECT_EQ(cb.stats().state.load(), circuit_state::open);
    EXPECT_FALSE(cb.allow_request());
    EXPECT_EQ(cb.stats().rejected_count.load(), 1u);
    EXPECT_FALSE(cb.allow_request());
    EXPECT_EQ(cb.stats().rejected_count.load(), 2u);
}

TEST(circuit_breaker, success_decays_failure_count_while_closed) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/3, /*timeout_ms=*/30000, /*half_open_max=*/2);

    cb.record_failure();
    cb.record_failure();
    cb.record_success();
    // Decayed back to 1 failure - one more shouldn't be enough to open
    // a threshold-3 breaker.
    cb.record_failure();

    EXPECT_EQ(cb.stats().state.load(), circuit_state::closed);
    EXPECT_EQ(cb.stats().failure_count.load(), 2u);
    EXPECT_TRUE(cb.allow_request());
}

TEST(circuit_breaker, transitions_to_half_open_after_timeout_elapses) {
    circuit_breaker_impl cb;
    // timeout_ms=0 makes "timeout elapsed" true as soon as any time has
    // passed at all, so the open->half_open transition is deterministic
    // without needing to sleep in the test.
    cb.configure(/*threshold=*/1, /*timeout_ms=*/0, /*half_open_max=*/2);

    cb.record_failure();
    ASSERT_EQ(cb.stats().state.load(), circuit_state::open);

    EXPECT_TRUE(cb.allow_request());
    EXPECT_EQ(cb.stats().state.load(), circuit_state::half_open);
}

// Regression test for a real bug: allow_request()'s open->half_open
// transition only resets success_count, never failure_count. The
// half-open budget check is `success_count + failure_count < half_open_max`,
// so the stale failure_count left over from *before* the circuit opened
// counts against the half-open probation budget. With this library's own
// documented defaults (circuit_breaker_threshold=5, half_open_max=3), a
// breaker that trips after 5 failures enters half-open with
// failure_count still at 5, so 5 < 3 is false immediately - every
// request except the one transitioning call gets rejected. Since any
// half-open failure immediately reopens the circuit and reaching
// half_open_max successes is what closes it, there's no path back to a
// working state: the breaker is permanently stuck rejecting almost
// everything for any configuration where threshold >= half_open_max.
TEST(circuit_breaker, half_open_budget_is_not_corrupted_by_stale_pre_open_failure_count) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/5, /*timeout_ms=*/0, /*half_open_max=*/3);

    for (int i = 0; i < 5; ++i) {
        cb.record_failure();
    }
    ASSERT_EQ(cb.stats().state.load(), circuit_state::open);
    ASSERT_EQ(cb.stats().failure_count.load(), 5u);

    ASSERT_TRUE(cb.allow_request());  // open -> half_open (unconditional)
    ASSERT_EQ(cb.stats().state.load(), circuit_state::half_open);

    // Recovery should be judged on outcomes recorded during half-open, not
    // whatever failure count caused it to open in the first place.
    EXPECT_TRUE(cb.allow_request());
}

TEST(circuit_breaker, half_open_closes_after_enough_successes) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/1, /*timeout_ms=*/0, /*half_open_max=*/2);

    cb.record_failure();
    ASSERT_TRUE(cb.allow_request());  // open -> half_open

    cb.record_success();
    EXPECT_EQ(cb.stats().state.load(), circuit_state::half_open);
    cb.record_success();

    EXPECT_EQ(cb.stats().state.load(), circuit_state::closed);
    EXPECT_EQ(cb.stats().failure_count.load(), 0u);
    EXPECT_TRUE(cb.allow_request());
}

TEST(circuit_breaker, half_open_reopens_on_any_failure) {
    circuit_breaker_impl cb;
    // Non-zero timeout: with timeout_ms=0, checking allow_request() again
    // after reopening would immediately transition back to half-open (any
    // elapsed time satisfies "timeout expired"), masking the reopen this
    // test is checking for. A real timeout keeps the post-reopen state
    // observable as genuinely open.
    cb.configure(/*threshold=*/1, /*timeout_ms=*/30000, /*half_open_max=*/2);

    cb.record_failure();
    ASSERT_EQ(cb.stats().state.load(), circuit_state::open);

    // Force past the timeout without sleeping: reconfigure to 0 just long
    // enough to make the one transition call succeed, then restore it so
    // the reopen below isn't immediately re-eligible to transition again.
    cb.configure(/*threshold=*/1, /*timeout_ms=*/0, /*half_open_max=*/2);
    ASSERT_TRUE(cb.allow_request());  // open -> half_open
    ASSERT_EQ(cb.stats().state.load(), circuit_state::half_open);
    cb.configure(/*threshold=*/1, /*timeout_ms=*/30000, /*half_open_max=*/2);

    cb.record_failure();

    EXPECT_EQ(cb.stats().state.load(), circuit_state::open);
    EXPECT_FALSE(cb.allow_request());
}

TEST(circuit_breaker, reset_restores_closed_state_and_zeroes_counters) {
    circuit_breaker_impl cb;
    cb.configure(/*threshold=*/1, /*timeout_ms=*/30000, /*half_open_max=*/2);

    cb.record_failure();
    ASSERT_EQ(cb.stats().state.load(), circuit_state::open);
    cb.allow_request();  // rejected - timeout hasn't elapsed

    cb.reset();

    EXPECT_EQ(cb.stats().state.load(), circuit_state::closed);
    EXPECT_EQ(cb.stats().failure_count.load(), 0u);
    EXPECT_EQ(cb.stats().success_count.load(), 0u);
    EXPECT_EQ(cb.stats().rejected_count.load(), 0u);
    EXPECT_TRUE(cb.allow_request());
}

namespace {
std::span<const char> as_span(const std::string& s) {
    return std::span<const char>(s.data(), s.size());
}
}  // namespace

TEST(offline_queue, disabled_when_max_size_is_zero) {
    offline_queue q(/*max_size=*/0, /*max_bytes=*/1000);
    std::string payload = "hello";

    EXPECT_FALSE(q.enqueue("subj", as_span(payload), {}, std::nullopt));
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
}

TEST(offline_queue, enqueue_succeeds_and_tracks_size_and_bytes) {
    offline_queue q(/*max_size=*/10, /*max_bytes=*/1000);
    std::string payload = "hello";

    ASSERT_TRUE(q.enqueue("subj", as_span(payload), {}, std::nullopt));

    EXPECT_EQ(q.size(), 1u);
    EXPECT_FALSE(q.empty());
    // "subj" (4) + "hello" (5) = 9 bytes.
    EXPECT_EQ(q.bytes(), 9u);
}

TEST(offline_queue, byte_accounting_includes_headers_and_reply_to) {
    offline_queue q(/*max_size=*/10, /*max_bytes=*/1000);
    std::string payload = "hi";
    headers_t headers = {{"k1", "v1"}, {"k2", "v22"}};

    ASSERT_TRUE(q.enqueue("s", as_span(payload), headers, string_view("reply")));

    // "s"(1) + "hi"(2) + "k1"+"v1"(4) + "k2"+"v22"(5) + "reply"(5) = 17.
    EXPECT_EQ(q.bytes(), 17u);
}

TEST(offline_queue, rejects_once_max_size_reached) {
    offline_queue q(/*max_size=*/2, /*max_bytes=*/1000);
    std::string payload = "x";

    ASSERT_TRUE(q.enqueue("a", as_span(payload), {}, std::nullopt));
    ASSERT_TRUE(q.enqueue("b", as_span(payload), {}, std::nullopt));
    EXPECT_FALSE(q.enqueue("c", as_span(payload), {}, std::nullopt));
    EXPECT_EQ(q.size(), 2u);
}

TEST(offline_queue, rejects_once_max_bytes_would_be_exceeded) {
    std::string payload = "1234567890";  // 10 bytes
    // subject "s" (1) + payload (10) = 11 bytes for one message exactly.
    offline_queue q(/*max_size=*/100, /*max_bytes=*/11);

    ASSERT_TRUE(q.enqueue("s", as_span(payload), {}, std::nullopt));
    // Exactly at budget - a second message of any size must be rejected.
    EXPECT_FALSE(q.enqueue("s", as_span(payload), {}, std::nullopt));
    EXPECT_EQ(q.size(), 1u);
    EXPECT_EQ(q.bytes(), 11u);
}

TEST(offline_queue, drain_returns_messages_in_order_and_empties_the_queue) {
    offline_queue q(/*max_size=*/10, /*max_bytes=*/1000);
    std::string p1 = "one";
    std::string p2 = "two";
    std::string p3 = "three";

    ASSERT_TRUE(q.enqueue("subj1", as_span(p1), {}, std::nullopt));
    ASSERT_TRUE(q.enqueue("subj2", as_span(p2), {}, std::nullopt));
    ASSERT_TRUE(q.enqueue("subj3", as_span(p3), {}, std::nullopt));

    auto drained = q.drain();

    ASSERT_EQ(drained.size(), 3u);
    EXPECT_EQ(drained[0].subject, "subj1");
    EXPECT_EQ(drained[1].subject, "subj2");
    EXPECT_EQ(drained[2].subject, "subj3");

    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_EQ(q.bytes(), 0u);

    // Draining an already-empty queue is safe and returns nothing.
    EXPECT_TRUE(q.drain().empty());
}

TEST(offline_queue, drain_preserves_payload_headers_and_reply_to) {
    offline_queue q(/*max_size=*/10, /*max_bytes=*/1000);
    std::string payload = "the-payload";
    headers_t headers = {{"Content-Type", "text/plain"}};

    ASSERT_TRUE(q.enqueue("orders.new", as_span(payload), headers, string_view("reply.inbox")));

    auto drained = q.drain();
    ASSERT_EQ(drained.size(), 1u);
    const auto& msg = drained[0];

    EXPECT_EQ(msg.subject, "orders.new");
    EXPECT_EQ(std::string(msg.payload.begin(), msg.payload.end()), payload);
    ASSERT_EQ(msg.headers.size(), 1u);
    EXPECT_EQ(msg.headers[0].first, "Content-Type");
    EXPECT_EQ(msg.headers[0].second, "text/plain");
    ASSERT_TRUE(msg.reply_to.has_value());
    EXPECT_EQ(*msg.reply_to, "reply.inbox");
}

TEST(offline_queue, enqueue_without_reply_to_leaves_it_unset) {
    offline_queue q(/*max_size=*/10, /*max_bytes=*/1000);
    std::string payload = "x";

    ASSERT_TRUE(q.enqueue("subj", as_span(payload), {}, std::nullopt));

    auto drained = q.drain();
    ASSERT_EQ(drained.size(), 1u);
    EXPECT_FALSE(drained[0].reply_to.has_value());
}

TEST(offline_queue, clear_discards_messages_without_returning_them) {
    offline_queue q(/*max_size=*/10, /*max_bytes=*/1000);
    std::string payload = "x";
    ASSERT_TRUE(q.enqueue("subj", as_span(payload), {}, std::nullopt));

    q.clear();

    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_EQ(q.bytes(), 0u);
    EXPECT_TRUE(q.drain().empty());
}

TEST(offline_queue, remains_consistent_under_concurrent_producers) {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t messages_per_producer = 5'000;
    offline_queue q(/*max_size=*/producer_count * messages_per_producer,
                    /*max_bytes=*/1u << 30);
    std::string payload = "x";
    std::atomic<bool> start{false};
    std::atomic<std::size_t> accepted{0};
    std::vector<std::thread> producers;
    producers.reserve(producer_count);

    for (std::size_t i = 0; i < producer_count; ++i) {
        producers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (std::size_t n = 0; n < messages_per_producer; ++n) {
                if (q.enqueue("subj", as_span(payload), {}, std::nullopt)) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(accepted.load(), producer_count * messages_per_producer);
    EXPECT_EQ(q.size(), accepted.load());
    auto drained = q.drain();
    EXPECT_EQ(drained.size(), accepted.load());
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
