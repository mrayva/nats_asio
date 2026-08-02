#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <modes/message_output.hpp>
#include <sstream>
#include <string>

#if defined(__SANITIZE_ADDRESS__)
#define NATS_TOOL_TEST_HAS_ASAN 1
#include <sanitizer/lsan_interface.h>
#endif

using namespace nats_tool;

namespace {

std::span<const char> as_span(const std::string& s) {
    return std::span<const char>(s.data(), s.size());
}

// zerialize::translate<JSON>() (used by deserialize_to_json() for every
// binary_format) allocates its destination yyjson document before reading
// the source, and doesn't free it if that read throws - which it always
// does on malformed/truncated input, even a completely empty payload. That
// makes it a real upstream leak (zerialize::json::RootSerializer is missing
// exception-safe cleanup, not anything in this repo), but it also means
// there's no "well-formed enough" bad payload that dodges it - any test of
// emit_message's deserialization-failure path hits it. Scope leak detection
// off just around those calls rather than losing ASan leak coverage for the
// rest of this binary.
struct scoped_lsan_disable {
#ifdef NATS_TOOL_TEST_HAS_ASAN
    scoped_lsan_disable() { __lsan_disable(); }
    ~scoped_lsan_disable() { __lsan_enable(); }
#endif
};

}  // namespace

// --- apply_translate_if_configured ---------------------------------------

TEST(apply_translate_if_configured, returns_payload_unchanged_with_no_copy_when_cmd_is_empty) {
    asio::io_context ioc;
    std::string payload = "hello";
    std::string storage;
    std::span<const char> result;

    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            result = co_await apply_translate_if_configured(
                "", "subj", as_span(payload), spdlog::default_logger(), storage);
        },
        asio::detached);
    ioc.run();

    // Zero-copy fast path: same backing storage as the input payload, not a
    // copy into `storage`.
    EXPECT_EQ(result.data(), payload.data());
    EXPECT_EQ(std::string(result.begin(), result.end()), "hello");
}

TEST(apply_translate_if_configured, runs_command_and_returns_a_view_into_storage) {
    asio::io_context ioc;
    std::string payload = "hello";
    std::string storage;
    std::span<const char> result;

    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            result = co_await apply_translate_if_configured(
                "tr a-z A-Z", "subj", as_span(payload), spdlog::default_logger(), storage);
        },
        asio::detached);
    ioc.run();

    EXPECT_NE(result.data(), payload.data());
    EXPECT_EQ(std::string(result.begin(), result.end()), "HELLO");
    EXPECT_EQ(storage, "HELLO");
}

TEST(apply_translate_if_configured, threads_subject_through_to_the_command) {
    asio::io_context ioc;
    std::string payload = "ignored";
    std::string storage;
    std::span<const char> result;

    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            result = co_await apply_translate_if_configured(
                "printf '%s' {{Subject}}", "orders.new", as_span(payload),
                spdlog::default_logger(), storage);
        },
        asio::detached);
    ioc.run();

    EXPECT_EQ(std::string(result.begin(), result.end()), "orders.new");
}

// --- emit_message ----------------------------------------------------------

TEST(emit_message, raw_mode_writes_line_prefix_then_payload) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    std::string payload = "the-payload";

    emit_message(out, output_mode::raw, "subj", as_span(payload), std::nullopt, stats,
                 spdlog::default_logger(), ioc, {}, {}, ">> ");

    EXPECT_EQ(out.str(), ">> the-payload\n");
}

TEST(emit_message, raw_mode_with_no_line_prefix) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    std::string payload = "the-payload";

    emit_message(out, output_mode::raw, "subj", as_span(payload), std::nullopt, stats,
                 spdlog::default_logger(), ioc);

    EXPECT_EQ(out.str(), "the-payload\n");
}

TEST(emit_message, normal_mode_writes_prefix_subject_and_payload) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    std::string payload = "the-payload";

    emit_message(out, output_mode::normal, "orders.new", as_span(payload), std::nullopt, stats,
                 spdlog::default_logger(), ioc, {}, {}, "[1] ");

    EXPECT_EQ(out.str(), "[1] [orders.new] the-payload\n");
}

TEST(emit_message, none_mode_writes_nothing) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    std::string payload = "the-payload";

    emit_message(out, output_mode::none, "subj", as_span(payload), std::nullopt, stats,
                 spdlog::default_logger(), ioc);

    EXPECT_TRUE(out.str().empty());
}

TEST(emit_message, json_mode_without_format_escapes_raw_payload) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    std::string payload = "line1\nline2\"quoted\"";

    emit_message(out, output_mode::json, "orders.new", as_span(payload), std::nullopt, stats,
                 spdlog::default_logger(), ioc);

    auto parsed = nlohmann::json::parse(out.str(), nullptr, /*allow_exceptions=*/false);
    ASSERT_FALSE(parsed.is_discarded()) << "not valid JSON: " << out.str();
    EXPECT_EQ(parsed["subject"], "orders.new");
    EXPECT_EQ(parsed["payload"], payload);
}

TEST(emit_message, json_mode_without_format_inserts_prefix_and_suffix_fields) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    std::string payload = "hi";

    emit_message(out, output_mode::json, "orders.new", as_span(payload), std::nullopt, stats,
                 spdlog::default_logger(), ioc, "\"ts\":123,", ",\"stream\":\"s\"");

    auto parsed = nlohmann::json::parse(out.str(), nullptr, /*allow_exceptions=*/false);
    ASSERT_FALSE(parsed.is_discarded()) << "not valid JSON: " << out.str();
    EXPECT_EQ(parsed["ts"], 123);
    EXPECT_EQ(parsed["subject"], "orders.new");
    EXPECT_EQ(parsed["stream"], "s");
    EXPECT_EQ(parsed["payload"], "hi");
}

TEST(emit_message, json_mode_with_format_writes_bare_deserialized_json_when_no_envelope_fields) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;

    auto bytes = serialize_from_json(R"({"a":1,"b":"x"})", binary_format::msgpack);
    ASSERT_TRUE(bytes.has_value());
    std::span<const char> payload(reinterpret_cast<const char*>(bytes->data()), bytes->size());

    emit_message(out, output_mode::json, "orders.new", payload, binary_format::msgpack, stats,
                 spdlog::default_logger(), ioc);

    auto parsed = nlohmann::json::parse(out.str(), nullptr, /*allow_exceptions=*/false);
    ASSERT_FALSE(parsed.is_discarded()) << "not valid JSON: " << out.str();
    // Written bare - no subject/payload envelope - since prefix/suffix are empty.
    EXPECT_FALSE(parsed.contains("subject"));
    EXPECT_EQ(parsed["a"], 1);
    EXPECT_EQ(parsed["b"], "x");
    EXPECT_EQ(stats.total_messages(), 1u);
    EXPECT_EQ(stats.bad_messages(), 0u);
}

TEST(emit_message, json_mode_with_format_wraps_deserialized_json_when_envelope_fields_given) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;

    auto bytes = serialize_from_json(R"({"a":1})", binary_format::msgpack);
    ASSERT_TRUE(bytes.has_value());
    std::span<const char> payload(reinterpret_cast<const char*>(bytes->data()), bytes->size());

    emit_message(out, output_mode::json, "orders.new", payload, binary_format::msgpack, stats,
                 spdlog::default_logger(), ioc, {}, ",\"stream\":\"s\"");

    auto parsed = nlohmann::json::parse(out.str(), nullptr, /*allow_exceptions=*/false);
    ASSERT_FALSE(parsed.is_discarded()) << "not valid JSON: " << out.str();
    EXPECT_EQ(parsed["subject"], "orders.new");
    EXPECT_EQ(parsed["stream"], "s");
    EXPECT_EQ(parsed["payload"]["a"], 1);
}

TEST(emit_message, json_mode_with_format_records_failure_and_writes_nothing_on_bad_payload) {
    asio::io_context ioc;
    deserializer_stats stats;
    std::ostringstream out;
    // Empty payload: no msgpack decoder can read even a type byte from zero
    // bytes, so this reliably fails deserialization. See scoped_lsan_disable
    // above for why any failing payload here needs leak detection scoped
    // off, not just this particular choice of "bad" input.
    std::string garbage;

    {
        scoped_lsan_disable lsan_guard;
        emit_message(out, output_mode::json, "orders.new", as_span(garbage), binary_format::msgpack,
                     stats, spdlog::default_logger(), ioc);
    }

    EXPECT_TRUE(out.str().empty());
    EXPECT_EQ(stats.total_messages(), 1u);
    EXPECT_EQ(stats.bad_messages(), 1u);
}

TEST(emit_message, json_mode_stops_ioc_when_bad_message_threshold_is_exceeded) {
    asio::io_context ioc;
    deserializer_stats stats(/*max_bad_messages=*/1, /*max_bad_percentage=*/0.0);
    std::ostringstream out;
    std::string garbage;

    ASSERT_FALSE(ioc.stopped());
    {
        scoped_lsan_disable lsan_guard;
        emit_message(out, output_mode::json, "orders.new", as_span(garbage), binary_format::msgpack,
                     stats, spdlog::default_logger(), ioc);
    }

    EXPECT_TRUE(ioc.stopped());
}
