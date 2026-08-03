// string_utils.hpp pulls in inja (template rendering), which is only
// fetched/available when samples are built (see vcpkg.json's "samples"
// feature) - kept in its own TU, added to parser_test conditionally on
// nats_tool being built, rather than requiring inja for the
// BUILD_SAMPLES=OFF/BUILD_TESTS=ON combination (see CMakeLists.txt).

#include <gtest/gtest.h>

#include <cstdio>
#include <nlohmann/json.hpp>
#include <string>
#include <string_utils.hpp>
#include <vector>

#if defined(__unix__)
#include <unistd.h>
#endif

TEST(shell_quote, wraps_plain_text_in_single_quotes) {
    EXPECT_EQ(nats_tool::shell_quote("hello"), "'hello'");
}

TEST(shell_quote, handles_empty_string) {
    EXPECT_EQ(nats_tool::shell_quote(""), "''");
}

TEST(shell_quote, escapes_embedded_single_quotes) {
    // '\'' closes the quote, emits an escaped quote, reopens the quote.
    EXPECT_EQ(nats_tool::shell_quote("it's"), "'it'\\''s'");
    EXPECT_EQ(nats_tool::shell_quote("'''"), "''\\'''\\'''\\'''");
}

TEST(shell_quote, leaves_other_metacharacters_unescaped_since_they_are_inert_in_single_quotes) {
    EXPECT_EQ(nats_tool::shell_quote("$(whoami) `id` ; rm -rf / | cat > x < y & # \n"),
              "'$(whoami) `id` ; rm -rf / | cat > x < y & # \n'");
}

// Regression test for the --translate {{Subject}} shell injection fix: feed
// attacker-controlled subjects containing shell metacharacters through
// shell_quote() and an actual `/bin/sh -c`, and confirm the shell treats the
// quoted result as inert literal data rather than executing it.
TEST(shell_quote, output_is_inert_when_executed_by_a_real_shell) {
    const std::vector<std::string> payloads = {
        "plain",
        "it's a test",
        "$(touch /tmp/shell_quote_should_not_run_this)",
        "`touch /tmp/shell_quote_should_not_run_this`",
        "; touch /tmp/shell_quote_should_not_run_this ;",
        "$(echo pwned > /tmp/shell_quote_should_not_run_this)",
        "a'; touch /tmp/shell_quote_should_not_run_this; echo '",
        "with\nnewline",
    };

    for (const auto& payload : payloads) {
        std::string cmd = "printf '%s' " + nats_tool::shell_quote(payload);
        FILE* pipe = popen(cmd.c_str(), "r");
        ASSERT_NE(pipe, nullptr);
        std::string output;
        char buf[256];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), pipe)) > 0) {
            output.append(buf, n);
        }
        pclose(pipe);

        EXPECT_EQ(output, payload) << "shell_quote round-trip failed for: " << payload;
    }

    EXPECT_EQ(::access("/tmp/shell_quote_should_not_run_this", F_OK), -1)
        << "shell_quote failed to neutralize a shell injection payload";
}

TEST(escape_json_string, passes_through_plain_text_unchanged) {
    const std::string text = "hello world 123";
    EXPECT_EQ(nats_tool::escape_json_string(std::span<const char>(text.data(), text.size())), text);
}

TEST(escape_json_string, escapes_quotes_backslashes_and_common_control_chars) {
    const std::string text = "a\"b\\c\nd\re\tf";
    EXPECT_EQ(nats_tool::escape_json_string(std::span<const char>(text.data(), text.size())),
              "a\\\"b\\\\c\\nd\\re\\tf");
}

TEST(escape_json_string, escapes_other_control_chars_as_unicode_escapes) {
    const std::string text = std::string(1, '\x01') + std::string(1, '\x1f');
    EXPECT_EQ(nats_tool::escape_json_string(std::span<const char>(text.data(), text.size())),
              "\\u0001\\u001f");
}

// Regression test: the escaped output must be a valid JSON string literal
// that round-trips back to the original payload byte-for-byte, since
// grub/js_grub/js_fetch rely on this for --json output.
TEST(escape_json_string, escaped_output_round_trips_through_a_real_json_parser) {
    const std::vector<std::string> payloads = {
        "plain text",
        "quotes \" and backslash \\",
        "line1\nline2\r\ttabbed",
        std::string(1, '\x00') + "embedded-nul-ish",
        "control:" + std::string(1, '\x07') + "end",
        "unicode: \xc3\xa9\xc3\xa8",
    };

    for (const auto& payload : payloads) {
        std::string escaped =
            nats_tool::escape_json_string(std::span<const char>(payload.data(), payload.size()));
        std::string json_literal = "\"" + escaped + "\"";

        auto parsed = nlohmann::json::parse(json_literal, nullptr, /*allow_exceptions=*/false);
        ASSERT_FALSE(parsed.is_discarded()) << "escape_json_string produced invalid JSON for a "
                                                "payload containing: "
                                             << json_literal;
        EXPECT_EQ(parsed.get<std::string>(), payload);
    }
}

TEST(split_string, splits_on_delimiter_and_trims_whitespace) {
    EXPECT_EQ(nats_tool::split_string("a,b,c", ','),
              (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(nats_tool::split_string(" a , b ,c ", ','),
              (std::vector<std::string>{"a", "b", "c"}));
}

TEST(split_string, drops_empty_fields) {
    // Empty/whitespace-only fields are dropped entirely, not kept as "".
    EXPECT_EQ(nats_tool::split_string("a,,b", ','), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(nats_tool::split_string("", ','), std::vector<std::string>{});
    EXPECT_EQ(nats_tool::split_string(",  ,", ','), std::vector<std::string>{});
}

TEST(build_payload, dumps_whole_object_when_no_fields_selected) {
    nlohmann::json obj = {{"a", 1}, {"b", 2}};
    auto result = nlohmann::json::parse(nats_tool::build_payload(obj, {}));
    EXPECT_EQ(result, obj);
}

TEST(build_payload, includes_only_selected_fields_that_are_present) {
    nlohmann::json obj = {{"a", 1}, {"b", 2}, {"c", 3}};
    auto result = nlohmann::json::parse(nats_tool::build_payload(obj, {"a", "c", "missing"}));
    nlohmann::json expected = {{"a", 1}, {"c", 3}};
    EXPECT_EQ(result, expected);
}

TEST(apply_template, substitutes_fields_from_json_object) {
    nlohmann::json obj = {{"subject", "orders.new"}, {"id", 42}};
    EXPECT_EQ(nats_tool::apply_template("prefix.{{subject}}.{{id}}", obj), "prefix.orders.new.42");
}

TEST(apply_template, returns_template_unchanged_on_render_error) {
    nlohmann::json obj = {{"a", 1}};
    // Malformed placeholder syntax: inja fails to parse, apply_template must
    // fall back to returning the raw template rather than throwing/crashing.
    EXPECT_EQ(nats_tool::apply_template("{{unclosed", obj), "{{unclosed");
}

TEST(parse_csv_line, parses_simple_unquoted_fields) {
    auto obj = nats_tool::parse_csv_line("a,b,c", {"h1", "h2", "h3"});
    EXPECT_EQ(obj["h1"], "a");
    EXPECT_EQ(obj["h2"], "b");
    EXPECT_EQ(obj["h3"], "c");
}

TEST(parse_csv_line, trims_whitespace_around_unquoted_fields) {
    auto obj = nats_tool::parse_csv_line(" a , b ,c ", {"h1", "h2", "h3"});
    EXPECT_EQ(obj["h1"], "a");
    EXPECT_EQ(obj["h2"], "b");
    EXPECT_EQ(obj["h3"], "c");
}

TEST(parse_csv_line, handles_comma_inside_quoted_field) {
    auto obj = nats_tool::parse_csv_line("a,\"b,c\",d", {"h1", "h2", "h3"});
    EXPECT_EQ(obj["h1"], "a");
    EXPECT_EQ(obj["h2"], "b,c");
    EXPECT_EQ(obj["h3"], "d");
}

TEST(parse_csv_line, handles_doubled_quote_as_escaped_quote) {
    auto obj = nats_tool::parse_csv_line("a,\"he said \"\"hi\"\"\",c", {"h1", "h2", "h3"});
    EXPECT_EQ(obj["h1"], "a");
    EXPECT_EQ(obj["h2"], "he said \"hi\"");
    EXPECT_EQ(obj["h3"], "c");
}

TEST(parse_csv_line, ignores_extra_values_and_leaves_missing_headers_absent) {
    auto obj = nats_tool::parse_csv_line("a,b,c", {"h1", "h2"});
    EXPECT_EQ(obj["h1"], "a");
    EXPECT_EQ(obj["h2"], "b");
    EXPECT_EQ(obj.size(), 2u);

    auto obj2 = nats_tool::parse_csv_line("a,b", {"h1", "h2", "h3"});
    EXPECT_EQ(obj2["h1"], "a");
    EXPECT_EQ(obj2["h2"], "b");
    EXPECT_FALSE(obj2.contains("h3"));
}

// Documents current behavior rather than RFC 4180: the function's own
// comment says whitespace trimming is for unquoted fields only, but the
// trim loops run unconditionally after a field is closed regardless of
// whether it came from a quoted context, so leading/trailing whitespace
// *inside* quotes is stripped too. If that comment's intent is ever
// enforced, this test's expectation should flip to "  a  ".
TEST(parse_csv_line, strips_whitespace_even_inside_quotes_despite_the_unquoted_only_comment) {
    auto obj = nats_tool::parse_csv_line("\"  a  \",b", {"h1", "h2"});
    EXPECT_EQ(obj["h1"], "a");
    EXPECT_EQ(obj["h2"], "b");
}
