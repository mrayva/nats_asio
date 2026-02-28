#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <nats_asio/decompression_reader.hpp>
#include <nats_asio/http_reader.hpp>
#include <nats_asio/nats_asio.hpp>
#include <spdlog/spdlog.h>

using namespace nats_asio;

TEST(http_reader, parse_url_https_default_port) {
    asio::io_context ioc;
    input_source_config cfg;
    auto log = spdlog::default_logger();
    async_http_reader reader(ioc, cfg, log);

    std::string protocol;
    std::string host;
    std::string port;
    std::string path;
    ASSERT_TRUE(reader.parse_url("https://example.com/api/v1", protocol, host, port, path));

    EXPECT_EQ(protocol, "https");
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, "443");
    EXPECT_EQ(path, "/api/v1");
}

TEST(http_reader, parse_url_http_custom_port) {
    asio::io_context ioc;
    input_source_config cfg;
    auto log = spdlog::default_logger();
    async_http_reader reader(ioc, cfg, log);

    std::string protocol;
    std::string host;
    std::string port;
    std::string path;
    ASSERT_TRUE(reader.parse_url("http://localhost:8080", protocol, host, port, path));

    EXPECT_EQ(protocol, "http");
    EXPECT_EQ(host, "localhost");
    EXPECT_EQ(port, "8080");
    EXPECT_EQ(path, "/");
}

TEST(http_reader, parse_url_invalid_protocol) {
    asio::io_context ioc;
    input_source_config cfg;
    auto log = spdlog::default_logger();
    async_http_reader reader(ioc, cfg, log);

    std::string protocol;
    std::string host;
    std::string port;
    std::string path;
    EXPECT_FALSE(reader.parse_url("ftp://example.com/data", protocol, host, port, path));
}

TEST(compression_detection, detects_magic_bytes) {
    const char gzip_magic[4] = {static_cast<char>(0x1f), static_cast<char>(0x8b), 0x08, 0x00};
    const char zstd_magic[4] = {static_cast<char>(0x28), static_cast<char>(0xb5),
                                static_cast<char>(0x2f), static_cast<char>(0xfd)};
    const char plain[4] = {'t', 'e', 's', 't'};

    EXPECT_EQ(detect_compression(gzip_magic, sizeof(gzip_magic)), compression_format::gzip);
    EXPECT_EQ(detect_compression(zstd_magic, sizeof(zstd_magic)), compression_format::zstd);
    EXPECT_EQ(detect_compression(plain, sizeof(plain)), compression_format::none);
}

TEST(compression_detection, detects_from_filename) {
    EXPECT_EQ(detect_compression_from_filename("/tmp/a.gz"), compression_format::gzip);
    EXPECT_EQ(detect_compression_from_filename("/tmp/a.zst"), compression_format::zstd);
    EXPECT_EQ(detect_compression_from_filename("/tmp/a.zstd"), compression_format::zstd);
    EXPECT_EQ(detect_compression_from_filename("/tmp/a.txt"), compression_format::none);
}

TEST(parsing_utils, parse_int_valid_and_invalid) {
    int out = 0;
    EXPECT_TRUE(parse_int<int>("12345", out));
    EXPECT_EQ(out, 12345);
    EXPECT_FALSE(parse_int<int>("12x", out));
    EXPECT_EQ(parse_int_or<int>("99", 7), 99);
    EXPECT_EQ(parse_int_or<int>("invalid", 7), 7);
}

TEST(parsing_utils, parse_timestamp_valid_and_invalid) {
    auto valid = fast_parse_timestamp("2021-08-15T23:24:24.123456789Z");
    auto invalid = fast_parse_timestamp("invalid");

    EXPECT_NE(valid, std::chrono::system_clock::time_point{});
    EXPECT_EQ(invalid, std::chrono::system_clock::time_point{});
}
