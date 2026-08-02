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
#include <fmt/format.h>
#include <js_stream_utils.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

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

// kv_get_impl has no CLI mode exposing it (nats_tool has kvcreate/kvupdate/
// kvkeys/kvhistory/kvpurge/kvrevert but no kvget), so unlike every other KV
// operation it had zero exercising test anywhere before this - only ever
// verified by reading the code. Checks the two things that actually matter
// for a "get": it returns the latest value after an update (not a stale
// one), and it reports a clean "not found" for a key that was never put,
// rather than some other error or a garbage entry.
asio::awaitable<bool> kv_get_returns_latest_value_and_reports_missing_keys(
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

    const std::string bucket = "regression_kv_get";
    const std::string stream = "KV_" + bucket;

    {
        std::string create_payload =
            "{\"name\":\"" + stream + "\",\"subjects\":[\"$KV." + bucket +
            ".>\"],\"retention\":\"limits\",\"max_msgs_per_subject\":10,\"discard\":\"new\","
            "\"storage\":\"file\",\"num_replicas\":1,\"allow_direct\":true,"
            "\"allow_rollup_hdrs\":true}";
        std::span<const char> payload(create_payload.data(), create_payload.size());
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.CREATE." + stream, payload,
                                                std::chrono::milliseconds(5000));
        (void)resp;
        if (s.failed()) {
            log->error("failed to create KV bucket stream: {}", s.error());
            conn->stop();
            co_return false;
        }
    }

    bool ok = true;

    // A key that was never put should come back as not-found, not some
    // other error and not a garbage/default-constructed entry mistaken for
    // success.
    {
        auto [entry, s] = co_await conn->kv_get(bucket, "never_put", std::chrono::milliseconds(5000));
        (void)entry;
        if (s.code() != nats_asio::error_code::key_not_found) {
            log->error("expected key_not_found for a never-put key, got: {}", s.error());
            ok = false;
        }
    }

    // Put twice, then get: must return the second (latest) value, not the
    // first - proving it's reading the current head of the key, not just
    // "a" revision.
    {
        std::string v1 = "first-value";
        std::span<const char> v1_span(v1.data(), v1.size());
        auto [rev1, s1] = co_await conn->kv_put(bucket, "mykey", v1_span, std::chrono::milliseconds(5000));
        if (s1.failed()) {
            log->error("kv_put (first) failed: {}", s1.error());
            ok = false;
        }

        std::string v2 = "second-value";
        std::span<const char> v2_span(v2.data(), v2.size());
        auto [rev2, s2] = co_await conn->kv_put(bucket, "mykey", v2_span, std::chrono::milliseconds(5000));
        if (s2.failed()) {
            log->error("kv_put (second) failed: {}", s2.error());
            ok = false;
        }

        auto [entry, s3] = co_await conn->kv_get(bucket, "mykey", std::chrono::milliseconds(5000));
        if (s3.failed()) {
            log->error("kv_get failed: {}", s3.error());
            ok = false;
        } else {
            std::string got(entry.value.begin(), entry.value.end());
            if (got != v2) {
                log->error("kv_get returned '{}', expected latest value '{}'", got, v2);
                ok = false;
            }
            if (entry.revision != rev2) {
                log->error("kv_get returned revision {}, expected latest revision {}", entry.revision, rev2);
                ok = false;
            }
            if (entry.op != nats_asio::kv_entry::operation::put) {
                log->error("kv_get returned unexpected op for a live key");
                ok = false;
            }
        }
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

// Guards against a real bug: unlike publish/subscribe, request() (and
// everything built on it - create_consumer and every KV op) had no redirect
// onto m_strand when called from a thread other than the connection's own,
// so the strand-bound state it touches (state->response written from the
// strand-bound subscribe callback, read back from the calling coroutine)
// could be accessed from two different threads with no ordering between
// them. Fixed by adding the same strand redirect every other method already
// had. The concurrency stress scenarios in nats_tool_integration.sh only
// exercise publish this way - this is the equivalent for request()/KV,
// firing many concurrent operations at a single shared connection from
// several independent OS threads, none of which is the connection's own.
// Meaningful mainly under TSan (see ci.yml): a clean exit alone only proves
// nothing crashed outright, not that there was no race.
asio::awaitable<bool> concurrent_request_and_kv_ops_from_multiple_threads_are_safe(
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

    const std::string subject = "regression.concurrent_req.subj";
    const std::string bucket = "regression_concurrent_kv";
    const std::string stream = "KV_" + bucket;

    // Echo responder for the request() stress, on the same connection.
    auto [sub, sub_status] = co_await conn->subscribe(
        subject,
        [conn](nats_asio::string_view /*s*/, nats_asio::optional<nats_asio::string_view> reply_to,
              std::span<const char> payload) -> asio::awaitable<void> {
            if (reply_to) {
                co_await conn->publish(*reply_to, payload, std::nullopt);
            }
            co_return;
        });
    if (sub_status.failed()) {
        log->error("subscribe failed: {}", sub_status.error());
        conn->stop();
        co_return false;
    }

    {
        std::string create_payload =
            "{\"name\":\"" + stream + "\",\"subjects\":[\"$KV." + bucket +
            ".>\"],\"retention\":\"limits\",\"max_msgs_per_subject\":10,\"discard\":\"new\","
            "\"storage\":\"file\",\"num_replicas\":1,\"allow_direct\":true,"
            "\"allow_rollup_hdrs\":true}";
        std::span<const char> payload(create_payload.data(), create_payload.size());
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.CREATE." + stream, payload,
                                                std::chrono::milliseconds(5000));
        (void)resp;
        if (s.failed()) {
            log->error("failed to create KV bucket stream: {}", s.error());
            conn->stop();
            co_return false;
        }
    }

    constexpr int num_threads = 4;
    constexpr int ops_per_thread = 25;
    auto req_success = std::make_shared<std::atomic<int>>(0);
    auto req_failure = std::make_shared<std::atomic<int>>(0);
    auto kv_success = std::make_shared<std::atomic<int>>(0);
    auto kv_failure = std::make_shared<std::atomic<int>>(0);
    auto threads_done = std::make_shared<std::atomic<int>>(0);

    std::vector<std::thread> workers;
    workers.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        workers.emplace_back([t, conn, subject, bucket, req_success, req_failure, kv_success,
                              kv_failure, threads_done]() {
            asio::io_context worker_ioc;
            // Bounds worker_ioc.run() regardless of outcome, so the join()
            // below is always safe to call unconditionally - a stuck
            // operation fails this test cleanly instead of risking hanging
            // the whole process past CTest's own process-level timeout.
            asio::steady_timer worker_timeout(worker_ioc);
            worker_timeout.expires_after(std::chrono::seconds(15));
            worker_timeout.async_wait([&](const asio::error_code& ec) {
                if (!ec) worker_ioc.stop();
            });

            asio::co_spawn(
                worker_ioc,
                [&]() -> asio::awaitable<void> {
                    for (int i = 0; i < ops_per_thread; ++i) {
                        std::string payload = fmt::format("t{}-i{}", t, i);
                        std::span<const char> pspan(payload.data(), payload.size());
                        auto [resp, s] = co_await conn->request(
                            subject, pspan, std::chrono::milliseconds(3000));
                        if (s.ok() &&
                            std::string(resp.payload.begin(), resp.payload.end()) == payload) {
                            req_success->fetch_add(1, std::memory_order_relaxed);
                        } else {
                            req_failure->fetch_add(1, std::memory_order_relaxed);
                        }

                        std::string key = fmt::format("t{}_key{}", t, i);
                        std::string value = fmt::format("t{}-value{}", t, i);
                        std::span<const char> vspan(value.data(), value.size());
                        // 5000ms, not 3000: matches the js_publish/stream-create
                        // timeouts elsewhere in this file, giving some margin
                        // for the very first op racing the freshly-created
                        // stream and all 4 threads' startup burst.
                        auto [rev, put_s] = co_await conn->kv_put(
                            bucket, key, vspan, std::chrono::milliseconds(5000));
                        if (put_s.failed()) {
                            kv_failure->fetch_add(1, std::memory_order_relaxed);
                            continue;
                        }
                        auto [entry, get_s] = co_await conn->kv_get(
                            bucket, key, std::chrono::milliseconds(5000));
                        std::string got(entry.value.begin(), entry.value.end());
                        if (get_s.ok() && got == value && entry.revision == rev) {
                            kv_success->fetch_add(1, std::memory_order_relaxed);
                        } else {
                            kv_failure->fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    threads_done->fetch_add(1, std::memory_order_relaxed);
                    worker_timeout.cancel();
                    co_return;
                },
                asio::detached);

            worker_ioc.run();
        });
    }

    // Poll rather than block: this coroutine runs on `ioc`, which is what
    // actually carries out the strand-redirected work the worker threads
    // above are waiting on, so it must keep yielding back to `ioc`'s event
    // loop between checks rather than blocking synchronously.
    for (int i = 0; i < 400 && threads_done->load(std::memory_order_relaxed) < num_threads; ++i) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }

    // Safe unconditionally: each worker's own hard-timeout bounds its
    // io_context::run(), so this can't hang even if the poll above timed
    // out with a thread stuck mid-operation.
    for (auto& w : workers) {
        if (w.joinable()) w.join();
    }

    bool ok = threads_done->load() == num_threads && req_failure->load() == 0 &&
              req_success->load() == num_threads * ops_per_thread && kv_failure->load() == 0 &&
              kv_success->load() == num_threads * ops_per_thread;
    if (!ok) {
        log->error("threads_done={}/{} req: {} ok / {} failed, kv: {} ok / {} failed",
                  threads_done->load(), num_threads, req_success->load(), req_failure->load(),
                  kv_success->load(), kv_failure->load());
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

namespace {
// Returns the "subjects" list from a stream's current config, or nullopt if
// the stream doesn't exist / the request failed.
asio::awaitable<std::optional<std::vector<std::string>>> fetch_stream_subjects(
    nats_asio::iconnection_sptr conn, const std::string& stream) {
    std::span<const char> empty;
    auto [resp, s] = co_await conn->request("$JS.API.STREAM.INFO." + stream, empty,
                                            std::chrono::milliseconds(5000));
    if (s.failed() || resp.payload.empty()) {
        co_return std::nullopt;
    }
    try {
        auto info = nlohmann::json::parse(std::string(resp.payload.begin(), resp.payload.end()));
        if (info.contains("error") || !info.contains("config") ||
            !info["config"].contains("subjects")) {
            co_return std::nullopt;
        }
        co_return info["config"]["subjects"].get<std::vector<std::string>>();
    } catch (const std::exception&) {
        co_return std::nullopt;
    }
}
}  // namespace

// Guards against real bugs in ensure_stream_for_subject() (samples-side,
// used by publisher.hpp's --create_stream flag on `pub --js`) - untested
// anywhere before this: neither the unit tests (it needs a live server for
// real JetStream STREAM.INFO/CREATE/UPDATE semantics) nor the shell
// integration suite (which always pre-creates streams manually, bypassing
// this code path entirely). Exercises all three branches: create a
// never-existing stream, no-op when the stream already includes the
// subject, and update an existing stream to add a new subject without
// losing the one it already had.
asio::awaitable<bool> ensure_stream_for_subject_creates_updates_and_leaves_existing_subjects(
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

    const std::string stream = "REGRESSION_ENSURE_STREAM";
    const std::string subject_a = "regression.ensure_stream.a";
    const std::string subject_b = "regression.ensure_stream.b";
    bool ok = true;

    // Fresh start: delete any leftover stream from a previous interrupted run.
    {
        std::span<const char> empty;
        auto [resp, s] = co_await conn->request("$JS.API.STREAM.DELETE." + stream, empty,
                                                std::chrono::milliseconds(3000));
        (void)resp;
        (void)s;
    }

    // 1. Stream doesn't exist yet -> should create it with subject_a.
    {
        bool created = co_await nats_tool::ensure_stream_for_subject(conn, stream, subject_a, log);
        if (!created) {
            log->error("ensure_stream_for_subject failed to create a new stream");
            ok = false;
        }
        auto subjects = co_await fetch_stream_subjects(conn, stream);
        if (!subjects || std::find(subjects->begin(), subjects->end(), subject_a) == subjects->end()) {
            log->error("newly created stream is missing subject_a");
            ok = false;
        }
    }

    // 2. Stream already includes subject_a -> should be a no-op, not
    // duplicate the subject or otherwise corrupt the stream config.
    {
        bool result = co_await nats_tool::ensure_stream_for_subject(conn, stream, subject_a, log);
        if (!result) {
            log->error("ensure_stream_for_subject failed on an already-included subject");
            ok = false;
        }
        auto subjects = co_await fetch_stream_subjects(conn, stream);
        std::size_t count_a =
            subjects ? std::count(subjects->begin(), subjects->end(), subject_a) : 0;
        if (count_a != 1) {
            log->error("subject_a appears {} times after a no-op call, expected exactly 1", count_a);
            ok = false;
        }
    }

    // 3. Stream exists but doesn't include subject_b -> should update the
    // stream to add it, keeping subject_a too.
    {
        bool updated = co_await nats_tool::ensure_stream_for_subject(conn, stream, subject_b, log);
        if (!updated) {
            log->error("ensure_stream_for_subject failed to update an existing stream");
            ok = false;
        }
        auto subjects = co_await fetch_stream_subjects(conn, stream);
        if (!subjects) {
            log->error("could not fetch stream config after update");
            ok = false;
        } else {
            bool has_a = std::find(subjects->begin(), subjects->end(), subject_a) != subjects->end();
            bool has_b = std::find(subjects->begin(), subjects->end(), subject_b) != subjects->end();
            if (!has_a || !has_b) {
                log->error("stream subjects after update: has_a={} has_b={}, expected both", has_a,
                          has_b);
                ok = false;
            }
        }
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
    all_ok = run_case("kv_get_returns_latest_value_and_reports_missing_keys", host, port, log,
                      kv_get_returns_latest_value_and_reports_missing_keys) && all_ok;
    all_ok = run_case("concurrent_request_and_kv_ops_from_multiple_threads_are_safe", host, port,
                      log, concurrent_request_and_kv_ops_from_multiple_threads_are_safe) && all_ok;
    all_ok = run_case("ensure_stream_for_subject_creates_updates_and_leaves_existing_subjects",
                      host, port, log,
                      ensure_stream_for_subject_creates_updates_and_leaves_existing_subjects) &&
             all_ok;

    return all_ok ? 0 : 1;
}
