#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <filesystem>
#include <fstream>
#include <modes/message_output.hpp>
#include <sstream>
#include <string>
#include <unistd.h>

using namespace nats_tool;

namespace {

std::span<const char> as_span(const std::string& s) {
    return std::span<const char>(s.data(), s.size());
}

std::filesystem::path unique_temp_path(const std::string& test_name) {
    return std::filesystem::temp_directory_path() /
           ("nats_asio_dump_file_test_" + test_name + "_" + std::to_string(::getpid()) + "_" +
            std::to_string(reinterpret_cast<uintptr_t>(&test_name)));
}

std::string read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

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
    // bytes, so this reliably fails deserialization.
    std::string garbage;

    emit_message(out, output_mode::json, "orders.new", as_span(garbage), binary_format::msgpack,
                 stats, spdlog::default_logger(), ioc);

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
    emit_message(out, output_mode::json, "orders.new", as_span(garbage), binary_format::msgpack,
                 stats, spdlog::default_logger(), ioc);

    EXPECT_TRUE(ioc.stopped());
}

// --- parse_format ------------------------------------------------------

TEST(parse_format, recognizes_all_supported_format_strings) {
    EXPECT_EQ(parse_format("msgpack"), binary_format::msgpack);
    EXPECT_EQ(parse_format("cbor"), binary_format::cbor);
    EXPECT_EQ(parse_format("flexbuffers"), binary_format::flexbuffers);
    EXPECT_EQ(parse_format("zera"), binary_format::zera);
    EXPECT_EQ(parse_format("bson"), binary_format::bson);
    EXPECT_EQ(parse_format("ion"), binary_format::ion);
    EXPECT_EQ(parse_format("beve"), binary_format::beve);
}

TEST(parse_format, returns_nullopt_for_unrecognized_strings) {
    EXPECT_EQ(parse_format(""), std::nullopt);
    EXPECT_EQ(parse_format("MsgPack"), std::nullopt);  // case-sensitive
    EXPECT_EQ(parse_format("json"), std::nullopt);     // not a binary format
    EXPECT_EQ(parse_format("bogus"), std::nullopt);
}

// --- serialize_from_json / deserialize_to_json ----------------------------

namespace {
std::string format_name(binary_format f) {
    switch (f) {
        case binary_format::msgpack: return "msgpack";
        case binary_format::cbor: return "cbor";
        case binary_format::flexbuffers: return "flexbuffers";
        case binary_format::zera: return "zera";
        case binary_format::bson: return "bson";
        case binary_format::ion: return "ion";
        case binary_format::beve: return "beve";
    }
    return "unknown";
}
}  // namespace

class binary_format_round_trip : public ::testing::TestWithParam<binary_format> {};

TEST_P(binary_format_round_trip, round_trips_a_json_object_through_the_binary_format) {
    binary_format format = GetParam();
    std::string original = R"({"a":1,"b":"hello","c":true,"d":[1,2,3]})";

    auto bytes = serialize_from_json(original, format);
    ASSERT_TRUE(bytes.has_value());
    EXPECT_FALSE(bytes->empty());

    std::span<const char> payload(reinterpret_cast<const char*>(bytes->data()), bytes->size());
    auto json_result = deserialize_to_json(payload, format);
    ASSERT_TRUE(json_result.has_value());

    // Compare parsed structure rather than raw strings: formats/toolchains
    // are free to reorder object keys or reformat whitespace, as long as
    // the actual data round-trips intact.
    EXPECT_EQ(nlohmann::json::parse(*json_result), nlohmann::json::parse(original));
}

TEST_P(binary_format_round_trip, deserialize_of_empty_payload) {
    binary_format format = GetParam();
    std::string empty;
    auto result = deserialize_to_json(as_span(empty), format);
    if (format == binary_format::flexbuffers) {
        // FlexBuffers represents a zero-length buffer as a valid "null"
        // root value per the format's own spec, unlike msgpack/cbor/zera
        // which need at least a type byte to decode anything - so this is
        // the one format where an empty payload is not an error.
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "null");
    } else {
        EXPECT_FALSE(result.has_value());
    }
}

INSTANTIATE_TEST_SUITE_P(
    all_formats, binary_format_round_trip,
    ::testing::Values(binary_format::msgpack, binary_format::cbor, binary_format::flexbuffers,
                      binary_format::zera, binary_format::bson, binary_format::ion,
                      binary_format::beve),
    [](const ::testing::TestParamInfo<binary_format>& info) { return format_name(info.param); });

TEST(serialize_from_json, fails_on_malformed_json_input) {
    std::string bad_json = "{not valid json";
    for (auto format : {binary_format::msgpack, binary_format::cbor, binary_format::flexbuffers,
                        binary_format::zera, binary_format::bson, binary_format::ion,
                        binary_format::beve}) {
        EXPECT_FALSE(serialize_from_json(bad_json, format).has_value())
            << "expected failure for format " << format_name(format);
    }
}

// --- deserializer_stats -----------------------------------------------------

TEST(deserializer_stats, disabled_thresholds_never_signal_exit) {
    deserializer_stats stats;  // both thresholds default to 0/0.0 (disabled)
    for (int i = 0; i < 200; ++i) {
        EXPECT_FALSE(stats.record_failure());
    }
    EXPECT_EQ(stats.total_messages(), 200u);
    EXPECT_EQ(stats.bad_messages(), 200u);
}

TEST(deserializer_stats, bad_percentage_is_zero_with_no_messages) {
    deserializer_stats stats;
    EXPECT_DOUBLE_EQ(stats.bad_percentage(), 0.0);
}

TEST(deserializer_stats, percentage_threshold_is_not_checked_before_100_total_messages) {
    deserializer_stats stats(/*max_bad_messages=*/0, /*max_bad_percentage=*/1.0);
    // 99 consecutive failures - 100% bad - but total_messages stays below
    // the 100-message minimum sample size the percentage check requires
    // (`m_total_messages >= 100`), so it must never signal exit no matter
    // how bad the ratio is yet.
    for (int i = 0; i < 99; ++i) {
        EXPECT_FALSE(stats.record_failure());
    }
    EXPECT_EQ(stats.total_messages(), 99u);
    EXPECT_DOUBLE_EQ(stats.bad_percentage(), 100.0);
}

TEST(deserializer_stats, percentage_threshold_triggers_once_100_messages_reached_and_ratio_exceeded) {
    deserializer_stats stats(/*max_bad_messages=*/0, /*max_bad_percentage=*/50.0);
    for (int i = 0; i < 99; ++i) {
        EXPECT_FALSE(stats.record_failure());
    }
    // 100th message: total_messages hits the 100 minimum and bad_percentage
    // is 100% (>= 50%), so this call must now signal exit.
    EXPECT_TRUE(stats.record_failure());
}

TEST(deserializer_stats, percentage_threshold_does_not_trigger_when_ratio_stays_below_it) {
    deserializer_stats stats(/*max_bad_messages=*/0, /*max_bad_percentage=*/50.0);
    for (int i = 0; i < 39; ++i) {
        EXPECT_FALSE(stats.record_failure());
    }
    for (int i = 0; i < 60; ++i) {
        stats.record_success();
    }
    EXPECT_EQ(stats.total_messages(), 99u);
    // 100th message: 40/100 = 40% bad, below the 50% threshold - must not trigger.
    EXPECT_FALSE(stats.record_failure());
    EXPECT_DOUBLE_EQ(stats.bad_percentage(), 40.0);
}

TEST(deserializer_stats, absolute_threshold_can_trigger_independently_of_percentage_threshold) {
    // max_bad_messages=5 fires well before the 100-message minimum the
    // percentage threshold requires, proving the two thresholds are
    // independent checks - either can trigger on its own.
    deserializer_stats stats(/*max_bad_messages=*/5, /*max_bad_percentage=*/90.0);
    for (int i = 0; i < 4; ++i) {
        EXPECT_FALSE(stats.record_failure());
    }
    EXPECT_TRUE(stats.record_failure());
    EXPECT_EQ(stats.total_messages(), 5u);
}

// --- dump_file_writer -------------------------------------------------------

TEST(dump_file_writer, falls_back_to_stdout_when_path_is_empty) {
    dump_file_writer writer("", spdlog::default_logger());
    EXPECT_EQ(&writer.stream(), &std::cout);
}

TEST(dump_file_writer, falls_back_to_stdout_when_path_cannot_be_opened) {
    // A path inside a directory that doesn't exist can never be opened.
    dump_file_writer writer("/nonexistent_dir_for_test/out.bin", spdlog::default_logger());
    EXPECT_EQ(&writer.stream(), &std::cout);
}

TEST(dump_file_writer, writes_to_the_file_when_path_is_valid) {
    auto path = unique_temp_path("writes");
    {
        dump_file_writer writer(path.string(), spdlog::default_logger());
        ASSERT_NE(&writer.stream(), &std::cout);
        writer.stream() << "hello";
        writer.on_message_written();
    }  // destructor closes (and flushes) the underlying ofstream.

    EXPECT_EQ(read_file(path), "hello");
    std::filesystem::remove(path);
}

TEST(dump_file_writer, flushes_to_disk_once_flush_every_messages_are_written) {
    auto path = unique_temp_path("flush_cadence");
    dump_file_writer writer(path.string(), spdlog::default_logger(), /*flush_every=*/3);
    ASSERT_NE(&writer.stream(), &std::cout);

    writer.stream() << "a";
    writer.on_message_written();
    writer.stream() << "b";
    writer.on_message_written();
    // Below the flush_every=3 threshold: not guaranteed visible to an
    // independent reader yet (still fine either way - this isn't asserted).

    writer.stream() << "c";
    writer.on_message_written();  // 3rd message hits the threshold.

    // Read via a completely independent handle: only an actual flush (not
    // just buffering inside `writer`'s own ofstream) makes this visible.
    EXPECT_EQ(read_file(path), "abc");

    std::filesystem::remove(path);
}

TEST(dump_file_writer, on_message_written_is_a_no_op_for_stdout_fallback) {
    dump_file_writer writer("", spdlog::default_logger());
    // Must not crash/misbehave when there's no dump file to flush.
    for (int i = 0; i < 5; ++i) {
        writer.on_message_written();
    }
    EXPECT_EQ(&writer.stream(), &std::cout);
}
