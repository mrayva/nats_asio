/*
MIT License

Copyright (c) 2019 Vladislav Troinich

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#pragma once

#include "decompression_reader.hpp"
#include "http_reader.hpp"  // For input_source_config
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <fcntl.h>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <sys/stat.h>
#include <tuple>
#include <unistd.h>

namespace nats_asio {

// ============================================================================
// Async line reader - reads from stdin or files, with follow mode support (tail -f)
// ============================================================================

class async_input_reader {
public:
    async_input_reader(asio::io_context& ioc, const input_source_config& config,
                       std::shared_ptr<spdlog::logger> log)
        : m_ioc(ioc), m_config(config), m_log(std::move(log)) {}

    ~async_input_reader() {
        m_decompressor.reset();
        m_stream.reset();  // stream_descriptor closes the fd it owns
    }

    async_input_reader(const async_input_reader&) = delete;
    async_input_reader& operator=(const async_input_reader&) = delete;

    // Initialize the reader - must be called before reading
    bool init() {
        if (m_config.file_path.empty()) {
            // Use stdin
            m_fd = ::dup(STDIN_FILENO);
            if (m_fd < 0) {
                m_log->error("Failed to dup stdin");
                return false;
            }
            m_stream = std::make_unique<asio::posix::stream_descriptor>(m_ioc, m_fd);
            m_is_stdin = true;
        } else {
            // Open file
            m_fd = ::open(m_config.file_path.c_str(), O_RDONLY | O_NONBLOCK);
            if (m_fd < 0) {
                m_log->error("Failed to open file: {}", m_config.file_path);
                return false;
            }
            m_stream = std::make_unique<asio::posix::stream_descriptor>(m_ioc, m_fd);
            m_is_stdin = false;

            // Detect compression format
            compression_format fmt = detect_compression_from_filename(m_config.file_path);
            if (fmt == compression_format::none) {
                // Try magic bytes detection
                char magic[4];
                ssize_t bytes = ::read(m_fd, magic, sizeof(magic));
                if (bytes == sizeof(magic)) {
                    fmt = detect_compression(magic, sizeof(magic));
                }
                // Reset file position
                if (::lseek(m_fd, 0, SEEK_SET) < 0) {
                    m_log->error("Failed to reset file position: {}", m_config.file_path);
                    return false;
                }
            }

            if (fmt != compression_format::none) {
                m_log->info("Detected {} compressed file: {}",
                           fmt == compression_format::gzip ? "gzip" : "zstd",
                           m_config.file_path);
                m_decompressor = std::make_unique<decompression_reader>(m_fd, fmt, m_log);
            }

            // For follow mode, seek to end of file to start reading new data
            // Note: Follow mode not supported for compressed files
            if (m_config.follow) {
                if (m_decompressor) {
                    m_log->warn("Follow mode not supported for compressed files, ignoring --follow");
                } else {
                    m_file_pos = ::lseek(m_fd, 0, SEEK_END);
                    if (m_file_pos < 0) m_file_pos = 0;
                }
            }
        }
        return true;
    }

    // Read next line asynchronously
    // Returns: {line, eof_reached, error_occurred}
    asio::awaitable<std::tuple<std::string, bool, bool>> read_line() {
        if (!m_stream) {
            co_return std::make_tuple("", true, true);
        }

        // Check if we have a complete line in the buffer
        auto newline_pos = m_buffer.find('\n');
        if (newline_pos != std::string::npos) {
            std::string line = m_buffer.substr(0, newline_pos);
            m_buffer.erase(0, newline_pos + 1);
            // Remove trailing \r if present (Windows line endings)
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            co_return std::make_tuple(std::move(line), false, false);
        }

        // Need to read more data
        char read_buf[8192];

        for (;;) {
            if (m_config.follow && !m_is_stdin) {
                // Follow mode for files - poll for new data
                co_return co_await read_line_follow_mode(read_buf, sizeof(read_buf));
            } else if (m_decompressor) {
                // Decompressed read
                auto [bytes_read, eof, error] = m_decompressor->read(read_buf, sizeof(read_buf));

                if (error) {
                    m_log->error("Decompression error");
                    co_return std::make_tuple("", true, true);
                }

                if (bytes_read == 0 && eof) {
                    // EOF - return any remaining data in buffer
                    if (!m_buffer.empty()) {
                        std::string line = std::move(m_buffer);
                        m_buffer.clear();
                        if (!line.empty() && line.back() == '\r') line.pop_back();
                        co_return std::make_tuple(std::move(line), true, false);
                    }
                    co_return std::make_tuple("", true, false);
                }

                if (bytes_read == 0) {
                    // No data available, yield briefly
                    asio::steady_timer timer(co_await asio::this_coro::executor);
                    timer.expires_after(std::chrono::milliseconds(1));
                    co_await timer.async_wait(asio::use_awaitable);
                    continue;
                }

                m_buffer.append(read_buf, bytes_read);
            } else {
                // Normal read (stdin or non-follow file)
                auto [ec, bytes_read] = co_await m_stream->async_read_some(
                    asio::buffer(read_buf, sizeof(read_buf)),
                    asio::as_tuple(asio::use_awaitable));

                if (ec) {
                    if (ec == asio::error::eof || ec == asio::error::not_found) {
                        // EOF - return any remaining data in buffer
                        if (!m_buffer.empty()) {
                            std::string line = std::move(m_buffer);
                            m_buffer.clear();
                            if (!line.empty() && line.back() == '\r') line.pop_back();
                            co_return std::make_tuple(std::move(line), true, false);
                        }
                        co_return std::make_tuple("", true, false);
                    }
                    m_log->error("Read error: {}", ec.message());
                    co_return std::make_tuple("", true, true);
                }

                if (bytes_read == 0) {
                    // No more data
                    if (!m_buffer.empty()) {
                        std::string line = std::move(m_buffer);
                        m_buffer.clear();
                        if (!line.empty() && line.back() == '\r') line.pop_back();
                        co_return std::make_tuple(std::move(line), true, false);
                    }
                    co_return std::make_tuple("", true, false);
                }

                m_buffer.append(read_buf, bytes_read);

                // Check for complete line
                newline_pos = m_buffer.find('\n');
                if (newline_pos != std::string::npos) {
                    std::string line = m_buffer.substr(0, newline_pos);
                    m_buffer.erase(0, newline_pos + 1);
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    co_return std::make_tuple(std::move(line), false, false);
                }
            }
        }
    }

    bool is_follow_mode() const { return m_config.follow; }

private:
    // Read with follow mode (tail -f behavior)
    asio::awaitable<std::tuple<std::string, bool, bool>> read_line_follow_mode(
        char* read_buf, size_t buf_size) {

        asio::steady_timer timer(co_await asio::this_coro::executor);

        for (;;) {
            // Check current file size
            struct stat st;
            if (::fstat(m_fd, &st) < 0) {
                m_log->error("fstat failed");
                co_return std::make_tuple("", true, true);
            }

            // Check if file was truncated (e.g., log rotation)
            if (st.st_size < m_file_pos) {
                m_log->info("File truncated, seeking to beginning");
                m_file_pos = 0;
                ::lseek(m_fd, 0, SEEK_SET);
            }

            // Read any new data
            if (st.st_size > m_file_pos) {
                ::lseek(m_fd, m_file_pos, SEEK_SET);
                ssize_t bytes_read = ::read(m_fd, read_buf, buf_size);

                if (bytes_read > 0) {
                    m_file_pos += bytes_read;
                    m_buffer.append(read_buf, bytes_read);

                    // Check for complete line
                    auto newline_pos = m_buffer.find('\n');
                    if (newline_pos != std::string::npos) {
                        std::string line = m_buffer.substr(0, newline_pos);
                        m_buffer.erase(0, newline_pos + 1);
                        if (!line.empty() && line.back() == '\r') line.pop_back();
                        co_return std::make_tuple(std::move(line), false, false);
                    }
                }
            }

            // No new data, wait and poll again
            timer.expires_after(std::chrono::milliseconds(m_config.poll_interval_ms));
            auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
            if (ec) {
                co_return std::make_tuple("", true, false);
            }
        }
    }

    asio::io_context& m_ioc;
    input_source_config m_config;
    std::shared_ptr<spdlog::logger> m_log;
    std::unique_ptr<asio::posix::stream_descriptor> m_stream;
    std::unique_ptr<decompression_reader> m_decompressor;
    int m_fd = -1;
    bool m_is_stdin = false;
    std::string m_buffer;
    off_t m_file_pos = 0;
};

} // namespace nats_asio
