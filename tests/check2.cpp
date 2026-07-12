#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <memory>
#include <nats_asio/compression.hpp>
#include <nats_asio/decompression_reader.hpp>
#include <nats_asio/http_reader.hpp>
#include <nats_asio/nats_asio.hpp>
#include <nats_asio/zip_extractor.hpp>
#include <spdlog/spdlog.h>
#include <type_traits>

using namespace nats_asio;

namespace {
struct file_closer {
    void operator()(FILE* file) const noexcept { std::fclose(file); }
};
using temp_file = std::unique_ptr<FILE, file_closer>;
} // namespace

static_assert(!std::is_copy_constructible_v<zstd_compressor>);
static_assert(!std::is_copy_assignable_v<zstd_compressor>);
static_assert(std::is_nothrow_move_constructible_v<zstd_compressor>);
static_assert(std::is_nothrow_move_assignable_v<zstd_compressor>);

TEST(zstd_compressor, remains_usable_after_moves) {
    const std::string payload = "move-only compressor round trip";
    zstd_compressor source;
    zstd_compressor compressor(std::move(source));

    auto compressed = compressor.compress(std::span(payload.data(), payload.size()));
    ASSERT_FALSE(compressed.empty());

    zstd_compressor decompressor;
    decompressor = std::move(compressor);
    auto decompressed = decompressor.decompress(compressed);
    EXPECT_EQ(std::string(decompressed.begin(), decompressed.end()), payload);
}

TEST(decompression_reader, rejects_truncated_zstd_frame) {
    const std::string payload(4096, 'x');
    zstd_compressor compressor;
    auto compressed = compressor.compress(std::span(payload.data(), payload.size()));
    ASSERT_GT(compressed.size(), 1u);
    compressed.pop_back();

    temp_file file(std::tmpfile());
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(std::fwrite(compressed.data(), 1, compressed.size(), file.get()), compressed.size());
    std::rewind(file.get());

    decompression_reader reader(fileno(file.get()), compression_format::zstd,
                                spdlog::default_logger());
    std::vector<char> output(payload.size() * 2);
    auto [bytes_read, eof, error] = reader.read(output.data(), output.size());
    EXPECT_EQ(bytes_read, 0);
    EXPECT_TRUE(eof);
    EXPECT_TRUE(error);
}

TEST(decompression_reader, reads_concatenated_zstd_frames) {
    const std::string first = "first frame\n";
    const std::string second = "second frame\n";
    zstd_compressor compressor;
    auto compressed = compressor.compress(std::span(first.data(), first.size()));
    auto second_frame = compressor.compress(std::span(second.data(), second.size()));
    compressed.insert(compressed.end(), second_frame.begin(), second_frame.end());

    temp_file file(std::tmpfile());
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(std::fwrite(compressed.data(), 1, compressed.size(), file.get()), compressed.size());
    std::rewind(file.get());

    decompression_reader reader(fileno(file.get()), compression_format::zstd,
                                spdlog::default_logger());
    std::vector<char> output(1024);
    auto [bytes_read, eof, error] = reader.read(output.data(), output.size());
    ASSERT_FALSE(error);
    EXPECT_TRUE(eof);
    EXPECT_EQ(std::string(output.data(), static_cast<size_t>(bytes_read)), first + second);
}

TEST(zip_extractor, validates_entry_names) {
    EXPECT_FALSE(zip_entry_filename(""));
    EXPECT_FALSE(zip_entry_filename("directory/"));
    auto filename = zip_entry_filename("directory/data.json");
    ASSERT_TRUE(filename);
    EXPECT_EQ(*filename, "data.json");
}

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
