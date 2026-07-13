#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <memory>
#include <nats_asio/compression.hpp>
#include <nats_asio/decompression_reader.hpp>
#include <nats_asio/http_reader.hpp>
#include <nats_asio/input_reader.hpp>
#include <nats_asio/multi_file_reader.hpp>
#include <nats_asio/nats_asio.hpp>
#include <nats_asio/zip_extractor.hpp>
#include <spdlog/spdlog.h>
#include <thread>
#include <type_traits>

using namespace nats_asio;

namespace {
struct file_closer {
    void operator()(FILE* file) const noexcept { std::fclose(file); }
};
using temp_file = std::unique_ptr<FILE, file_closer>;

std::vector<char> gzip_compress(std::string_view input) {
    z_stream stream{};
    if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
        return {};
    }
    std::vector<char> output(compressBound(input.size()) + 64);
    stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
    stream.avail_in = static_cast<uInt>(input.size());
    stream.next_out = reinterpret_cast<Bytef*>(output.data());
    stream.avail_out = static_cast<uInt>(output.size());
    const int result = deflate(&stream, Z_FINISH);
    const size_t output_size = stream.total_out;
    deflateEnd(&stream);
    if (result != Z_STREAM_END) return {};
    output.resize(output_size);
    return output;
}

bool create_test_zip(
    const std::filesystem::path& path,
    const std::vector<std::pair<std::string, std::string>>& entries) {
    int error = 0;
    zip_t* archive = zip_open(path.c_str(), ZIP_CREATE | ZIP_TRUNCATE, &error);
    if (!archive) return false;
    for (const auto& [name, payload] : entries) {
        zip_source_t* source = zip_source_buffer(archive, payload.data(), payload.size(), 0);
        if (!source || zip_file_add(archive, name.c_str(), source, ZIP_FL_OVERWRITE) < 0) {
            if (source) zip_source_free(source);
            zip_discard(archive);
            return false;
        }
    }
    if (zip_close(archive) == 0) return true;
    zip_discard(archive);
    return false;
}

bool create_test_zip(const std::filesystem::path& path, std::string_view payload) {
    return create_test_zip(path, {{"data.txt", std::string(payload)}});
}

struct http_read_result {
    bool initialized = false;
    std::tuple<std::string, bool, bool> line;
};

http_read_result read_test_http_response(
    std::string response,
    input_source_config config,
    int timeout_ms = 1000,
    std::chrono::milliseconds response_delay = {}) {
    asio::io_context server_ioc;
    asio::ip::tcp::acceptor acceptor(
        server_ioc, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    config.http_url = "http://127.0.0.1:" +
        std::to_string(acceptor.local_endpoint().port()) + "/";
    config.http_method = "GET";
    config.http_timeout_ms = timeout_ms;

    std::thread server([&] {
        asio::ip::tcp::socket socket(server_ioc);
        asio::error_code ec;
        acceptor.accept(socket, ec);
        if (ec) return;
        asio::streambuf request;
        asio::read_until(socket, request, "\r\n\r\n", ec);
        if (!ec) {
            std::this_thread::sleep_for(response_delay);
            asio::write(socket, asio::buffer(response), ec);
        }
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    });

    asio::io_context client_ioc;
    async_http_reader reader(client_ioc, config, spdlog::default_logger());
    http_read_result result;
    std::exception_ptr exception;
    asio::co_spawn(
        client_ioc,
        [&]() -> asio::awaitable<void> {
            result.initialized = co_await reader.init();
            if (result.initialized) result.line = co_await reader.read_line();
        },
        [&](std::exception_ptr error) { exception = error; });
    client_ioc.run();
    server.join();
    if (exception) std::rethrow_exception(exception);
    return result;
}

bool initialize_http_reader(input_source_config config) {
    asio::io_context ioc;
    async_http_reader reader(ioc, config, spdlog::default_logger());
    bool initialized = false;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            initialized = co_await reader.init();
        },
        asio::detached);
    ioc.run();
    return initialized;
}
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

TEST(zstd_compressor, enforces_decompression_output_limit) {
    const std::string payload = "bounded decompression";
    zstd_compressor compressor;
    auto compressed = compressor.compress(std::span(payload.data(), payload.size()));
    ASSERT_FALSE(compressed.empty());

    EXPECT_TRUE(compressor.decompress(compressed, payload.size() - 1).empty());
    auto decompressed = compressor.decompress(compressed, payload.size());
    EXPECT_EQ(std::string(decompressed.begin(), decompressed.end()), payload);
}

TEST(zstd_compressor, detects_wire_format_magic) {
    const std::string payload = "compressed payload";
    zstd_compressor compressor;
    auto compressed = compressor.compress(std::span(payload.data(), payload.size()));
    ASSERT_FALSE(compressed.empty());

    EXPECT_TRUE(zstd_compressor::is_compressed(compressed));
    const std::array<char, 4> plain{'t', 'e', 'x', 't'};
    EXPECT_FALSE(zstd_compressor::is_compressed(plain));
    EXPECT_FALSE(zstd_compressor::is_compressed(std::span(plain.data(), 3)));
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

TEST(decompression_reader, reads_concatenated_gzip_members) {
    const std::string first = "first gzip member\n";
    const std::string second = "second gzip member\n";
    auto compressed = gzip_compress(first);
    auto second_member = gzip_compress(second);
    ASSERT_FALSE(compressed.empty());
    ASSERT_FALSE(second_member.empty());
    compressed.insert(compressed.end(), second_member.begin(), second_member.end());

    temp_file file(std::tmpfile());
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(std::fwrite(compressed.data(), 1, compressed.size(), file.get()), compressed.size());
    std::rewind(file.get());

    decompression_reader reader(fileno(file.get()), compression_format::gzip,
                                spdlog::default_logger());
    std::vector<char> output(1024);
    auto [bytes_read, eof, error] = reader.read(output.data(), output.size());
    ASSERT_FALSE(error);
    EXPECT_TRUE(eof);
    EXPECT_EQ(std::string(output.data(), static_cast<size_t>(bytes_read)), first + second);
}

TEST(decompression_reader, rejects_truncated_gzip_member) {
    auto compressed = gzip_compress(std::string(4096, 'x'));
    ASSERT_GT(compressed.size(), 1u);
    compressed.pop_back();

    temp_file file(std::tmpfile());
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(std::fwrite(compressed.data(), 1, compressed.size(), file.get()), compressed.size());
    std::rewind(file.get());

    decompression_reader reader(fileno(file.get()), compression_format::gzip,
                                spdlog::default_logger());
    std::vector<char> output(8192);
    auto [bytes_read, eof, error] = reader.read(output.data(), output.size());
    EXPECT_EQ(bytes_read, 0);
    EXPECT_TRUE(eof);
    EXPECT_TRUE(error);
}

TEST(decompression_reader, rejects_truncated_second_gzip_member) {
    auto compressed = gzip_compress("complete member");
    auto truncated = gzip_compress(std::string(4096, 'x'));
    ASSERT_GT(truncated.size(), 1u);
    truncated.pop_back();
    compressed.insert(compressed.end(), truncated.begin(), truncated.end());

    temp_file file(std::tmpfile());
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(std::fwrite(compressed.data(), 1, compressed.size(), file.get()), compressed.size());
    std::rewind(file.get());

    decompression_reader reader(fileno(file.get()), compression_format::gzip,
                                spdlog::default_logger());
    std::vector<char> output(8192);
    auto [bytes_read, eof, error] = reader.read(output.data(), output.size());
    EXPECT_EQ(bytes_read, 0);
    EXPECT_TRUE(eof);
    EXPECT_TRUE(error);
}

TEST(zip_extractor, validates_entry_names) {
    EXPECT_FALSE(zip_entry_filename(""));
    EXPECT_FALSE(zip_entry_filename("directory/"));
    auto filename = zip_entry_filename("directory/data.json");
    ASSERT_TRUE(filename);
    EXPECT_EQ(*filename, "data.json");
}

TEST(zip_extractor, creates_unique_temp_directories) {
    auto first = create_zip_temp_directory();
    auto second = create_zip_temp_directory();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_NE(*first, *second);
    EXPECT_TRUE(std::filesystem::is_directory(*first));
    EXPECT_TRUE(std::filesystem::is_directory(*second));

    {
        zip_temp_cleanup cleanup(*first, spdlog::default_logger());
    }
    EXPECT_FALSE(std::filesystem::exists(*first));
    EXPECT_TRUE(std::filesystem::exists(*second));
    {
        zip_temp_cleanup cleanup(*second, spdlog::default_logger());
    }
    EXPECT_FALSE(std::filesystem::exists(*second));
}

TEST(zip_extractor, preserves_entries_after_flattened_name_collisions) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto archive_path = *source_dir / "collisions.zip";
    ASSERT_TRUE(create_test_zip(
        archive_path,
        {{"one/x.txt", "first"}, {"two_x.txt", "second"}, {"two/x.txt", "third"}}));

    std::filesystem::path extraction_dir;
    auto files = extract_zip_to_temp(archive_path, spdlog::default_logger(), &extraction_dir);
    ASSERT_EQ(files.size(), 3u);

    std::set<std::string> contents;
    for (const auto& file : files) {
        std::ifstream input(file);
        contents.emplace(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    EXPECT_EQ(contents, (std::set<std::string>{"first", "second", "third"}));

    zip_temp_cleanup cleanup(std::move(extraction_dir), spdlog::default_logger());
    std::filesystem::remove_all(*source_dir);
}

TEST(zip_extractor, enforces_extraction_limits) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto archive_path = *source_dir / "limits.zip";
    ASSERT_TRUE(create_test_zip(archive_path, {{"one.txt", "123"}, {"two.txt", "456"}}));

    std::filesystem::path extraction_dir;
    zip_extraction_limits entry_count_limit;
    entry_count_limit.max_entries = 1;
    EXPECT_TRUE(extract_zip_to_temp(archive_path, spdlog::default_logger(),
                                    &extraction_dir, entry_count_limit).empty());
    EXPECT_TRUE(extraction_dir.empty());

    zip_extraction_limits entry_size_limit;
    entry_size_limit.max_entry_bytes = 2;
    EXPECT_TRUE(extract_zip_to_temp(archive_path, spdlog::default_logger(),
                                    &extraction_dir, entry_size_limit).empty());
    EXPECT_TRUE(extraction_dir.empty());

    zip_extraction_limits total_size_limit;
    total_size_limit.max_total_bytes = 5;
    EXPECT_TRUE(extract_zip_to_temp(archive_path, spdlog::default_logger(),
                                    &extraction_dir, total_size_limit).empty());
    EXPECT_TRUE(extraction_dir.empty());

    std::filesystem::remove_all(*source_dir);
}

TEST(multi_file_reader, preserves_extracted_zip_files_across_rescans) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto archive_path = *source_dir / "input.zip";
    ASSERT_TRUE(create_test_zip(archive_path, "line\n"));

    asio::io_context ioc;
    async_multi_file_reader reader(ioc, {archive_path.string()}, true, 1,
                                   spdlog::default_logger());
    ASSERT_TRUE(reader.init());
    ASSERT_EQ(reader.file_count(), 1u);

    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            co_await reader.read_line();
        },
        asio::detached);
    ioc.run();
    EXPECT_EQ(reader.file_count(), 1u);

    ASSERT_TRUE(std::filesystem::remove(archive_path));
    ioc.restart();
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            co_await reader.read_line();
        },
        asio::detached);
    ioc.run();
    EXPECT_EQ(reader.file_count(), 0u);

    std::filesystem::remove_all(*source_dir);
}

TEST(multi_file_reader, reads_newly_discovered_files_from_beginning) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto initial_path = *source_dir / "initial.log";
    const auto new_path = *source_dir / "new.log";
    {
        std::ofstream initial(initial_path);
        initial << "existing initial content\n";
    }

    asio::io_context ioc;
    async_multi_file_reader reader(ioc, {(source_dir->string() + "/*.log")}, true, 1,
                                   spdlog::default_logger());
    ASSERT_TRUE(reader.init());

    std::string line;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto [value, path, eof, error] = co_await reader.read_line();
            (void)path;
            (void)eof;
            if (!error) line = std::move(value);
        },
        asio::detached);

    std::thread writer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        {
            std::ofstream new_file(new_path);
            new_file << "new file content\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::ofstream initial(initial_path, std::ios::app);
        initial << "old file fallback\n";
    });

    ioc.run();
    writer.join();
    EXPECT_EQ(line, "new file content");
    std::filesystem::remove_all(*source_dir);
}

TEST(multi_file_reader, reextracts_replaced_zip_archives) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto archive_path = *source_dir / "input.zip";
    const auto replacement_path = *source_dir / "replacement.tmp";
    ASSERT_TRUE(create_test_zip(archive_path, "initial content\n"));

    asio::io_context ioc;
    async_multi_file_reader reader(ioc, {archive_path.string()}, true, 1,
                                   spdlog::default_logger());
    ASSERT_TRUE(reader.init());

    std::string line;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto [value, path, eof, error] = co_await reader.read_line();
            (void)path;
            (void)eof;
            if (!error) line = std::move(value);
        },
        asio::detached);

    std::thread writer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (create_test_zip(replacement_path, "replacement content\n")) {
            std::filesystem::rename(replacement_path, archive_path);
        }
    });

    ioc.run();
    writer.join();
    EXPECT_EQ(line, "replacement content");
    std::filesystem::remove_all(*source_dir);
}

TEST(multi_file_reader, clears_partial_lines_after_truncation) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto input_path = *source_dir / "truncate.log";
    {
        std::ofstream input(input_path);
    }

    asio::io_context ioc;
    async_multi_file_reader reader(ioc, {input_path.string()}, true, 1,
                                   spdlog::default_logger());
    ASSERT_TRUE(reader.init());

    std::string line;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto [value, path, eof, error] = co_await reader.read_line();
            (void)path;
            (void)eof;
            if (!error) line = std::move(value);
        },
        asio::detached);

    std::thread writer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        {
            std::ofstream input(input_path, std::ios::app);
            input << "partial";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::ofstream replacement(input_path, std::ios::trunc);
        replacement << "new\n";
    });

    ioc.run();
    writer.join();
    EXPECT_EQ(line, "new");
    std::filesystem::remove_all(*source_dir);
}

TEST(input_reader, follows_replacement_inode) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto input_path = *source_dir / "input.log";
    const auto rotated_path = *source_dir / "input.log.1";
    {
        std::ofstream input(input_path);
        input << "existing\n";
    }

    asio::io_context ioc;
    input_source_config config;
    config.file_path = input_path.string();
    config.follow = true;
    config.poll_interval_ms = 1;
    async_input_reader reader(ioc, config, spdlog::default_logger());
    ASSERT_TRUE(reader.init());

    std::string line;
    bool error = false;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto [value, eof, read_error] = co_await reader.read_line();
            (void)eof;
            line = std::move(value);
            error = read_error;
        },
        asio::detached);

    std::thread writer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::filesystem::rename(input_path, rotated_path);
        {
            std::ofstream replacement(input_path);
            replacement << "replacement\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::ofstream old_file(rotated_path, std::ios::app);
        old_file << "old inode\n";
    });

    ioc.run();
    writer.join();
    EXPECT_FALSE(error);
    EXPECT_EQ(line, "replacement");
    std::filesystem::remove_all(*source_dir);
}

TEST(input_reader, disables_follow_for_compressed_files) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto input_path = *source_dir / "input.gz";
    auto compressed = gzip_compress("first compressed line\nsecond compressed line\n");
    ASSERT_FALSE(compressed.empty());
    {
        std::ofstream input(input_path, std::ios::binary);
        input.write(compressed.data(), static_cast<std::streamsize>(compressed.size()));
    }

    asio::io_context ioc;
    input_source_config config;
    config.file_path = input_path.string();
    config.follow = true;
    async_input_reader reader(ioc, config, spdlog::default_logger());
    ASSERT_TRUE(reader.init());
    EXPECT_FALSE(reader.is_follow_mode());

    std::vector<std::string> lines;
    bool error = false;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            for (int i = 0; i < 2; ++i) {
                auto [value, eof, read_error] = co_await reader.read_line();
                (void)eof;
                lines.push_back(std::move(value));
                error = error || read_error;
            }
        },
        asio::detached);
    ioc.run();

    EXPECT_FALSE(error);
    EXPECT_EQ(lines, (std::vector<std::string>{"first compressed line",
                                               "second compressed line"}));
    std::filesystem::remove_all(*source_dir);
}

TEST(input_reader, rejects_oversized_lines) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto input_path = *source_dir / "oversized.log";
    {
        std::ofstream input(input_path);
        input << "12345";
    }

    asio::io_context ioc;
    input_source_config config;
    config.file_path = input_path.string();
    config.input_max_line_size = 4;
    async_input_reader reader(ioc, config, spdlog::default_logger());
    ASSERT_TRUE(reader.init());

    bool error = false;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto [line, eof, read_error] = co_await reader.read_line();
            (void)line;
            (void)eof;
            error = read_error;
        },
        asio::detached);
    ioc.run();
    EXPECT_TRUE(error);
    std::filesystem::remove_all(*source_dir);
}

TEST(multi_file_reader, propagates_oversized_line_errors) {
    auto source_dir = create_zip_temp_directory();
    ASSERT_TRUE(source_dir);
    const auto input_path = *source_dir / "oversized.log";
    {
        std::ofstream input(input_path);
        input << "12345";
    }

    asio::io_context ioc;
    async_multi_file_reader reader(ioc, {input_path.string()}, false, 1,
                                   spdlog::default_logger(), 4);
    ASSERT_TRUE(reader.init());

    bool error = false;
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            auto [line, path, eof, read_error] = co_await reader.read_line();
            (void)line;
            (void)path;
            (void)eof;
            error = read_error;
        },
        asio::detached);
    ioc.run();
    EXPECT_TRUE(error);
    std::filesystem::remove_all(*source_dir);
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

TEST(http_reader, parse_url_query_without_path) {
    asio::io_context ioc;
    input_source_config cfg;
    async_http_reader reader(ioc, cfg, spdlog::default_logger());

    std::string protocol, host, port, path;
    ASSERT_TRUE(reader.parse_url("http://example.com?limit=10#ignored", protocol, host, port, path));
    EXPECT_EQ(host, "example.com");
    EXPECT_EQ(port, "80");
    EXPECT_EQ(path, "/?limit=10");
}

TEST(http_reader, parse_url_ipv6_with_port) {
    asio::io_context ioc;
    input_source_config cfg;
    async_http_reader reader(ioc, cfg, spdlog::default_logger());

    std::string protocol, host, port, path;
    ASSERT_TRUE(reader.parse_url("http://[::1]:8080/events", protocol, host, port, path));
    EXPECT_EQ(host, "::1");
    EXPECT_EQ(port, "8080");
    EXPECT_EQ(path, "/events");
}

TEST(http_reader, parse_url_rejects_invalid_authority) {
    asio::io_context ioc;
    input_source_config cfg;
    async_http_reader reader(ioc, cfg, spdlog::default_logger());

    std::string protocol, host, port, path;
    EXPECT_FALSE(reader.parse_url("http://", protocol, host, port, path));
    EXPECT_FALSE(reader.parse_url("http://example.com:", protocol, host, port, path));
    EXPECT_FALSE(reader.parse_url("http://example.com:70000", protocol, host, port, path));
    EXPECT_FALSE(reader.parse_url("http://::1/events", protocol, host, port, path));
    EXPECT_FALSE(reader.parse_url("http://example.com\r\nInjected:yes/", protocol, host,
                                  port, path));
}

TEST(http_reader, rejects_request_framing_injection) {
    input_source_config config;
    config.http_url = "http://127.0.0.1:1/";
    config.http_method = "GET\r\nInjected: yes";
    EXPECT_FALSE(initialize_http_reader(config));

    config.http_method = "GET";
    config.http_headers = {{"X-Test", "value\r\nInjected: yes"}};
    EXPECT_FALSE(initialize_http_reader(config));

    config.http_headers = {{"Invalid Header", "value"}};
    EXPECT_FALSE(initialize_http_reader(config));

    config.http_headers.clear();
    config.http_url = "http://127.0.0.1:1/path with space";
    EXPECT_FALSE(initialize_http_reader(config));
}

TEST(http_reader, rejects_oversized_response_line) {
    input_source_config config;
    config.http_max_line_size = 4;
    auto result = read_test_http_response("HTTP/1.1 200 OK\r\n\r\n12345", config);

    ASSERT_TRUE(result.initialized);
    EXPECT_TRUE(std::get<1>(result.line));
    EXPECT_TRUE(std::get<2>(result.line));
}

TEST(http_reader, times_out_delayed_response) {
    input_source_config config;
    auto result = read_test_http_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", config, 10,
        std::chrono::milliseconds(50));
    EXPECT_FALSE(result.initialized);
}

TEST(http_reader, rejects_oversized_chunk_metadata) {
    input_source_config config;
    config.http_max_chunk_metadata_size = 4;
    auto result = read_test_http_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n12345", config);

    EXPECT_FALSE(result.initialized);
}

TEST(http_reader, rejects_truncated_content_length_body) {
    input_source_config config;
    auto result = read_test_http_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc", config);

    ASSERT_TRUE(result.initialized);
    EXPECT_TRUE(std::get<1>(result.line));
    EXPECT_TRUE(std::get<2>(result.line));
}

TEST(http_reader, rejects_excess_content_length_body) {
    input_source_config config;
    auto result = read_test_http_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nabc", config);

    EXPECT_FALSE(result.initialized);
}

TEST(http_reader, rejects_ambiguous_content_length) {
    input_source_config config;
    auto conflicting = read_test_http_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nabc",
        config);
    EXPECT_FALSE(conflicting.initialized);

    auto chunked = read_test_http_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n"
        "3\r\nabc\r\n0\r\n\r\n",
        config);
    EXPECT_FALSE(chunked.initialized);
}

TEST(http_reader, completes_at_declared_content_length) {
    input_source_config config;
    auto result = read_test_http_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", config);

    ASSERT_TRUE(result.initialized);
    EXPECT_EQ(std::get<0>(result.line), "abc");
    EXPECT_TRUE(std::get<1>(result.line));
    EXPECT_FALSE(std::get<2>(result.line));
}

TEST(http_reader, rejects_invalid_transfer_encoding_tokens) {
    input_source_config config;
    auto substring = read_test_http_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: xchunked\r\n\r\nbody", config);
    EXPECT_FALSE(substring.initialized);

    auto unsupported = read_test_http_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", config);
    EXPECT_FALSE(unsupported.initialized);
}

TEST(http_reader, validates_chunk_trailers) {
    input_source_config config;
    auto malformed = read_test_http_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        "2\r\na\n\r\n0\r\ninvalid\r\n\r\n",
        config);
    EXPECT_FALSE(malformed.initialized);

    auto forbidden = read_test_http_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        "2\r\na\n\r\n0\r\nContent-Length: 2\r\n\r\n",
        config);
    EXPECT_FALSE(forbidden.initialized);

    auto valid = read_test_http_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        "2\r\na\n\r\n0\r\nX-Checksum: ok\r\n\r\n",
        config);
    ASSERT_TRUE(valid.initialized);
    EXPECT_EQ(std::get<0>(valid.line), "a");
    EXPECT_FALSE(std::get<2>(valid.line));
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
