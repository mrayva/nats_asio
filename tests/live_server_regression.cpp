// Regression tests that need a real nats-server rather than the fake
// protocol server used by connection_regression.cpp - for behavior that's
// only worth testing end-to-end against actual JetStream semantics (ack/nak
// redelivery timing, sequence numbering, etc).
//
// Skips (exit 125, recognized by CTest's SKIP_RETURN_CODE) rather than fails
// when no server is reachable on NATS_HOST:NATS_PORT (default
// 127.0.0.1:4222), so it doesn't break `ctest` for anyone without one
// running locally.
//
// nak_with_delay_survives_the_wait_for_redelivery guards against a real bug
// found in this codebase: js_subscription::send_ack() used to take its ack
// body by string_view. nak()'s delayed-NAK path built that body from a
// fmt::format() temporary, whose lifetime isn't extended through the call -
// if the underlying socket write didn't complete synchronously, the
// coroutine resumed reading already-freed memory. Fixed by taking the ack
// body by value instead, so it's owned by the coroutine frame for its full
// lifetime. This test publishes a message, NAKs it with a delay on first
// delivery (the exact path that used to dangle), and verifies it comes back
// redelivered after roughly the requested delay - which requires the NAK
// payload to have actually reached the server intact, not corrupted or
// crashed. Run under ASan in CI, which is what actually caught this bug.
//
// js_message_metadata_is_populated_from_ack_subject guards against a real
// bug: parse_js_message_metadata() only ever read Nats-* headers, but
// nats-server doesn't attach those to ordinary consumer deliveries - the
// actual, always-present carrier is the $JS.ACK reply-to subject, which the
// code never parsed. stream/consumer/stream_sequence/consumer_sequence/
// num_delivered/num_pending/timestamp were silently zero or empty on every
// JetStream message, for both push and pull consumers, until fixed. This
// test checks every one of those fields against known-correct values on a
// fresh stream's first message, then NAKs it and checks num_delivered
// increments to 2 on redelivery - not just "non-zero once" but genuinely
// tracking the server's own count, which a hardcoded or stale value
// wouldn't survive.

#include <nats_asio/nats_asio.hpp>
#include <asio/connect.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>

namespace {

bool server_reachable(const std::string& host, uint16_t port) {
    asio::io_context ioc;
    asio::ip::tcp::socket socket(ioc);
    asio::error_code ec;
    asio::ip::tcp::resolver resolver(ioc);
    auto endpoints = resolver.resolve(host, std::to_string(port), ec);
    if (ec) return false;
    asio::connect(socket, endpoints, ec);
    return !ec;
}

asio::awaitable<bool> nak_with_delay_survives_the_wait_for_redelivery(
    asio::io_context& ioc, const std::string& host, uint16_t port,
    std::shared_ptr<spdlog::logger> log) {
    auto conn = nats_asio::connect(ioc, host, port);

    asio::steady_timer timer(co_await asio::this_coro::executor);
    for (int i = 0; i < 100 && !conn->is_connected(); ++i) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }
    if (!conn->is_connected()) {
        log->error("could not connect to nats-server");
        co_return false;
    }

    const std::string stream = "REGRESSION_NAK_UAF";
    const std::string subject = "regression.nak_uaf.subj";

    // Fresh stream each run so there's never a leftover message from a
    // previous run confusing delivery-count bookkeeping.
    {
        std::string create_payload =
            "{\"name\":\"" + stream + "\",\"subjects\":[\"" + subject +
            "\"],\"retention\":\"limits\",\"storage\":\"file\",\"num_replicas\":1}";
        std::span<const char> payload(create_payload.data(), create_payload.size());
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.CREATE." + stream, payload,
                                                std::chrono::milliseconds(5000));
        (void)resp;
        if (s.failed()) {
            log->error("failed to create stream: {}", s.error());
            conn->stop();
            co_return false;
        }
    }

    bool ok = false;
    auto delivery_count = std::make_shared<int>(0);
    auto done = std::make_shared<bool>(false);
    auto delay_start = std::make_shared<std::chrono::steady_clock::time_point>();

    std::string payload_str = "hello-nak-regression";
    std::span<const char> payload(payload_str.data(), payload_str.size());
    auto [ack, pub_status] =
        co_await conn->js_publish(subject, payload, std::chrono::milliseconds(5000), true);
    if (pub_status.failed()) {
        log->error("js_publish failed: {}", pub_status.error());
        conn->stop();
        co_return false;
    }

    nats_asio::js_consumer_config config;
    config.stream = stream;
    config.filter_subject = subject;
    config.ack = nats_asio::js_ack_policy::explicit_;
    config.max_deliver = 5;
    config.ack_wait = std::chrono::seconds(30);

    auto [sub, sub_status] = co_await conn->js_subscribe(
        config,
        [&ok, done, delivery_count, delay_start, log](
            nats_asio::ijs_subscription& s,
            const nats_asio::js_message& msg) -> asio::awaitable<void> {
            ++(*delivery_count);

            if (*delivery_count == 1) {
                *delay_start = std::chrono::steady_clock::now();
                // The exact call that used to dangle: nak()'s delayed path
                // builds the ack body from a temporary internally.
                auto s2 = co_await s.nak(msg, std::chrono::milliseconds(300));
                if (s2.failed()) {
                    log->error("nak failed: {}", s2.error());
                    *done = true;
                }
            } else {
                auto elapsed = std::chrono::steady_clock::now() - *delay_start;
                auto elapsed_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
                // Redelivery should happen close to the requested 300ms, not
                // near-instantly (which would mean the NAK payload never
                // reached the server intact) and not after the 30s ack_wait
                // (which would mean the server never got a NAK at all).
                if (elapsed_ms < 100 || elapsed_ms > 10000) {
                    log->error("redelivery after {}ms - expected ~300ms", elapsed_ms);
                    *done = true;
                    co_return;
                }
                auto s2 = co_await s.ack(msg);
                if (s2.failed()) {
                    log->error("ack failed: {}", s2.error());
                } else {
                    ok = true;
                }
                *done = true;
            }
            co_return;
        });

    if (sub_status.failed()) {
        log->error("js_subscribe failed: {}", sub_status.error());
        conn->stop();
        co_return false;
    }

    for (int i = 0; i < 200 && !*done; ++i) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }
    if (!*done) {
        log->error("timed out waiting for redelivery after nak");
        ok = false;
    }

    // Best-effort cleanup; don't fail the test over it.
    {
        std::span<const char> empty;
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.DELETE." + stream, empty,
                                                std::chrono::milliseconds(3000));
        (void)resp;
        (void)s;
    }

    conn->stop();
    co_return ok;
}

asio::awaitable<bool> js_message_metadata_is_populated_from_ack_subject(
    asio::io_context& ioc, const std::string& host, uint16_t port,
    std::shared_ptr<spdlog::logger> log) {
    auto conn = nats_asio::connect(ioc, host, port);

    asio::steady_timer timer(co_await asio::this_coro::executor);
    for (int i = 0; i < 100 && !conn->is_connected(); ++i) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }
    if (!conn->is_connected()) {
        log->error("could not connect to nats-server");
        co_return false;
    }

    const std::string stream = "REGRESSION_JS_METADATA";
    const std::string subject = "regression.js_metadata.subj";

    // Fresh stream each run so stream_sequence/consumer_sequence == 1 on the
    // first delivery is a meaningful check, not a leftover from a prior run.
    {
        std::string create_payload =
            "{\"name\":\"" + stream + "\",\"subjects\":[\"" + subject +
            "\"],\"retention\":\"limits\",\"storage\":\"file\",\"num_replicas\":1}";
        std::span<const char> payload(create_payload.data(), create_payload.size());
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.CREATE." + stream, payload,
                                                std::chrono::milliseconds(5000));
        (void)resp;
        if (s.failed()) {
            log->error("failed to create stream: {}", s.error());
            conn->stop();
            co_return false;
        }
    }

    std::string payload_str = "hello-metadata-regression";
    std::span<const char> payload(payload_str.data(), payload_str.size());
    auto [ack, pub_status] =
        co_await conn->js_publish(subject, payload, std::chrono::milliseconds(5000), true);
    if (pub_status.failed()) {
        log->error("js_publish failed: {}", pub_status.error());
        conn->stop();
        co_return false;
    }

    nats_asio::js_consumer_config config;
    config.stream = stream;
    config.filter_subject = subject;
    config.ack = nats_asio::js_ack_policy::explicit_;
    config.max_deliver = 5;
    config.ack_wait = std::chrono::seconds(30);

    auto ok = std::make_shared<bool>(false);
    auto done = std::make_shared<bool>(false);

    auto [sub, sub_status] = co_await conn->js_subscribe(
        config,
        [ok, done, stream, log](nats_asio::ijs_subscription& s,
                                const nats_asio::js_message& msg) -> asio::awaitable<void> {
            if (msg.num_delivered <= 1) {
                // First delivery: every metadata field should reflect this
                // being the first (and so far only) message on a fresh
                // stream/consumer, parsed from the $JS.ACK reply-to subject.
                bool fields_ok = true;
                auto check = [&](bool cond, const char* what) {
                    if (!cond) {
                        log->error("metadata check failed: {}", what);
                        fields_ok = false;
                    }
                };
                check(msg.stream == stream, "stream");
                check(!msg.consumer.empty(), "consumer (server-assigned name)");
                check(msg.stream_sequence == 1, "stream_sequence == 1");
                check(msg.consumer_sequence == 1, "consumer_sequence == 1");
                check(msg.num_delivered == 1, "num_delivered == 1");
                check(msg.num_pending == 0, "num_pending == 0");

                auto now = std::chrono::system_clock::now();
                auto age = std::chrono::duration_cast<std::chrono::seconds>(now - msg.timestamp).count();
                check(age >= -5 && age <= 60, "timestamp is recent (not zero/epoch)");

                if (!fields_ok) {
                    *done = true;
                    co_return;
                }

                // NAK to trigger redelivery, so we can also confirm
                // num_delivered genuinely increments rather than being
                // hardcoded/stale.
                auto s2 = co_await s.nak(msg, std::chrono::milliseconds(200));
                if (s2.failed()) {
                    log->error("nak failed: {}", s2.error());
                    *done = true;
                }
            } else {
                if (msg.num_delivered != 2) {
                    log->error("expected num_delivered == 2 on redelivery, got {}", msg.num_delivered);
                    *done = true;
                    co_return;
                }
                auto s2 = co_await s.ack(msg);
                if (s2.failed()) {
                    log->error("ack failed: {}", s2.error());
                } else {
                    *ok = true;
                }
                *done = true;
            }
            co_return;
        });

    if (sub_status.failed()) {
        log->error("js_subscribe failed: {}", sub_status.error());
        conn->stop();
        co_return false;
    }

    for (int i = 0; i < 200 && !*done; ++i) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }
    if (!*done) {
        log->error("timed out waiting for redelivery");
        *ok = false;
    }

    // Best-effort cleanup; don't fail the test over it.
    {
        std::span<const char> empty;
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.DELETE." + stream, empty,
                                                std::chrono::milliseconds(3000));
        (void)resp;
        (void)s;
    }

    conn->stop();
    co_return *ok;
}

using test_case_fn = std::function<asio::awaitable<bool>(
    asio::io_context&, const std::string&, uint16_t, std::shared_ptr<spdlog::logger>)>;

// Runs one test case on its own fresh io_context (each case owns its
// connection's lifetime start to finish) under a hard timeout, and prints a
// PASS/FAIL line matching nats_tool_integration.sh's convention.
bool run_case(const std::string& name, const std::string& host, uint16_t port,
              const std::shared_ptr<spdlog::logger>& log, test_case_fn test_fn) {
    asio::io_context ioc;
    bool result = false;
    asio::steady_timer hard_timeout(ioc);
    hard_timeout.expires_after(std::chrono::seconds(20));
    hard_timeout.async_wait([&](const asio::error_code& ec) {
        if (!ec) ioc.stop();
    });

    asio::co_spawn(ioc,
        [&]() -> asio::awaitable<void> {
            result = co_await test_fn(ioc, host, port, log);
            hard_timeout.cancel();
        },
        asio::detached);

    ioc.run();

    std::cout << (result ? "PASS" : "FAIL") << ": " << name << "\n";
    return result;
}

} // namespace

int main() {
    constexpr int kSkipExitCode = 125;

    const char* host_env = std::getenv("NATS_HOST");
    const char* port_env = std::getenv("NATS_PORT");
    std::string host = host_env ? host_env : "127.0.0.1";
    uint16_t port = port_env ? static_cast<uint16_t>(std::atoi(port_env)) : 4222;

    if (!server_reachable(host, port)) {
        std::cout << "SKIP: no NATS server reachable on " << host << ":" << port << "\n";
        return kSkipExitCode;
    }

    auto log = spdlog::stdout_color_mt("live_server_regression");
    log->set_level(spdlog::level::info);

    bool all_ok = true;
    all_ok = run_case("nak_with_delay_survives_the_wait_for_redelivery", host, port, log,
                      nak_with_delay_survives_the_wait_for_redelivery) && all_ok;
    all_ok = run_case("js_message_metadata_is_populated_from_ack_subject", host, port, log,
                      js_message_metadata_is_populated_from_ack_subject) && all_ok;

    return all_ok ? 0 : 1;
}
