/*
MIT License

Copyright (c) 2019 Vladislav Troinich
Copyright (c) 2024-2026 mrayva

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
#include "zip_extractor.hpp"
#include <asio/awaitable.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <fcntl.h>
#include <glob.h>
#include <map>
#include <memory>
#include <set>
#include <spdlog/spdlog.h>
#include <string>
#include <sys/stat.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace nats_asio {

// ============================================================================
// Multi-file reader with glob pattern support and watch mode
// ============================================================================

class async_multi_file_reader {
public:
    struct tracked_file {
        std::string path;
        int fd = -1;
        ino_t inode = 0;           // For rotation detection
        off_t position = 0;
        std::string buffer;
        bool eof_reached = false;
        bool has_error = false;
        std::unique_ptr<decompression_reader> decompressor;
        compression_format format = compression_format::none;
    };

    async_multi_file_reader(asio::io_context& ioc, std::vector<std::string> patterns,
                            bool follow, int poll_interval_ms,
                            std::shared_ptr<spdlog::logger> log)
        : m_ioc(ioc), m_patterns(std::move(patterns)), m_follow(follow),
          m_poll_interval_ms(poll_interval_ms), m_log(std::move(log)) {}

    ~async_multi_file_reader() {
        close_all_files();
    }

    // Initialize - scan for files and open them
    bool init() {
        // Expand all glob patterns
        if (!scan_for_files()) {
            m_log->error("Failed to find any files matching patterns");
            return false;
        }

        if (m_files.empty()) {
            m_log->error("No files found matching patterns");
            return false;
        }

        m_log->info("Found {} files to read", m_files.size());
        for (const auto& [path, _] : m_files) {
            m_log->debug("  - {}", path);
        }

        return true;
    }

    // Read next line from any file (round-robin)
    // Returns: {line, file_path, eof_reached, error_occurred}
    asio::awaitable<std::tuple<std::string, std::string, bool, bool>> read_line() {
        if (m_files.empty()) {
            co_return std::make_tuple("", "", true, false);
        }

        asio::steady_timer timer(co_await asio::this_coro::executor);
        int consecutive_eofs = 0;
        const int max_wait_iterations = 100;  // Avoid infinite loop
        int wait_iterations = 0;

        for (;;) {
            // Try to read from all files in round-robin
            bool got_line = false;
            std::string line, file_path;

            for (auto& [path, file] : m_files) {
                if (file.has_error) continue;

                auto result = try_read_line_from_file(file);
                if (std::get<0>(result)) {
                    // Got a line!
                    line = std::move(std::get<1>(result));
                    file_path = path;
                    got_line = true;
                    consecutive_eofs = 0;
                    break;
                }

                if (std::get<2>(result)) {
                    // EOF reached on this file
                    file.eof_reached = true;
                }
            }

            if (got_line) {
                co_return std::make_tuple(std::move(line), std::move(file_path), false, false);
            }

            // All files at EOF
            consecutive_eofs++;

            if (m_follow) {
                // In follow mode, check for new data or new files
                check_file_rotations();
                scan_for_files();  // Look for new files

                // Check if any files have new data
                bool any_new_data = false;
                for (auto& [path, file] : m_files) {
                    if (file.has_error) continue;

                    struct stat st;
                    if (::fstat(file.fd, &st) == 0) {
                        if (st.st_size > file.position) {
                            file.eof_reached = false;
                            any_new_data = true;
                        }
                    }
                }

                if (any_new_data) {
                    continue;  // Try reading again
                }

                // No new data, wait and poll again
                timer.expires_after(std::chrono::milliseconds(m_poll_interval_ms));
                co_await timer.async_wait(asio::use_awaitable);
                wait_iterations++;

                if (wait_iterations > max_wait_iterations) {
                    // Yield an empty line to allow caller to do other work
                    co_return std::make_tuple("", "", false, false);
                }
            } else {
                // Not in follow mode and all files at EOF
                co_return std::make_tuple("", "", true, false);
            }
        }
    }

    bool is_follow_mode() const { return m_follow; }
    size_t file_count() const { return m_files.size(); }

private:
    // Try to read a line from a specific file
    // Returns: {got_line, line_content, eof_reached}
    std::tuple<bool, std::string, bool> try_read_line_from_file(tracked_file& file) {
        // Check if we have a complete line in the buffer
        auto newline_pos = file.buffer.find('\n');
        if (newline_pos != std::string::npos) {
            std::string line = file.buffer.substr(0, newline_pos);
            file.buffer.erase(0, newline_pos + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return std::make_tuple(true, std::move(line), false);
        }

        if (file.eof_reached) {
            return std::make_tuple(false, "", true);
        }

        // Try to read more data
        char read_buf[8192];
        ssize_t bytes_read = 0;
        bool eof = false;
        bool error = false;

        if (file.decompressor) {
            // Decompressed read
            auto [bytes, is_eof, is_error] = file.decompressor->read(read_buf, sizeof(read_buf));
            bytes_read = bytes;
            eof = is_eof;
            error = is_error;
        } else {
            // Regular read
            bytes_read = ::read(file.fd, read_buf, sizeof(read_buf));
            if (bytes_read < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No data available right now
                    return std::make_tuple(false, "", false);
                }
                error = true;
            } else if (bytes_read == 0) {
                eof = true;
            }
        }

        if (error) {
            m_log->error("Read error on {}: {}", file.path,
                        file.decompressor ? "decompression error" : strerror(errno));
            file.has_error = true;
            return std::make_tuple(false, "", true);
        }

        if (bytes_read == 0 && eof) {
            // EOF reached
            if (!file.buffer.empty()) {
                std::string line = std::move(file.buffer);
                file.buffer.clear();
                if (!line.empty() && line.back() == '\r') line.pop_back();
                file.eof_reached = true;
                return std::make_tuple(true, std::move(line), true);
            }
            file.eof_reached = true;
            return std::make_tuple(false, "", true);
        }

        if (bytes_read == 0) {
            // No data available right now
            return std::make_tuple(false, "", false);
        }

        if (!file.decompressor) {
            file.position += bytes_read;
        }
        file.buffer.append(read_buf, bytes_read);

        // Check for complete line again
        newline_pos = file.buffer.find('\n');
        if (newline_pos != std::string::npos) {
            std::string line = file.buffer.substr(0, newline_pos);
            file.buffer.erase(0, newline_pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            return std::make_tuple(true, std::move(line), false);
        }

        // No complete line yet
        return std::make_tuple(false, "", false);
    }

    // Scan for files matching patterns and open new ones
    bool scan_for_files() {
        std::set<std::string> current_paths;

        for (const auto& pattern : m_patterns) {
            glob_t glob_result;
            memset(&glob_result, 0, sizeof(glob_result));

            int ret = glob(pattern.c_str(), GLOB_TILDE | GLOB_BRACE, nullptr, &glob_result);

            if (ret != 0) {
                if (ret == GLOB_NOMATCH) {
                    m_log->debug("No files match pattern: {}", pattern);
                } else {
                    m_log->warn("glob() failed for pattern {}: {}", pattern, ret);
                }
                globfree(&glob_result);
                continue;
            }

            for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
                std::string path = glob_result.gl_pathv[i];

                // Check if this is a zip file
                if (is_zip_file(path) || is_zip_file_magic(path)) {
                    // Check if we've already extracted this zip
                    if (m_extracted_zips.find(path) == m_extracted_zips.end()) {
                        m_log->info("Detected zip archive: {}", path);
                        auto extracted = extract_zip_to_temp(path, m_log);

                        if (!extracted.empty()) {
                            // Track this zip as extracted
                            m_extracted_zips.insert(path);

                            // Add cleanup handler
                            m_zip_cleanups.push_back(
                                std::make_unique<zip_temp_cleanup>(path, m_log));

                            // Add all extracted files to current paths and open them
                            for (const auto& extracted_path : extracted) {
                                current_paths.insert(extracted_path);
                                if (m_files.find(extracted_path) == m_files.end()) {
                                    if (open_file(extracted_path)) {
                                        m_log->info("Started tracking extracted file: {}", extracted_path);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Regular file
                    current_paths.insert(path);

                    // Check if this is a new file
                    if (m_files.find(path) == m_files.end()) {
                        if (open_file(path)) {
                            m_log->info("Started tracking file: {}", path);
                        }
                    }
                }
            }

            globfree(&glob_result);
        }

        // Remove files that no longer match (deleted files)
        std::vector<std::string> to_remove;
        for (const auto& [path, file] : m_files) {
            if (current_paths.find(path) == current_paths.end()) {
                to_remove.push_back(path);
            }
        }

        for (const auto& path : to_remove) {
            m_log->info("File no longer matches pattern, closing: {}", path);
            close_file(path);
        }

        return !m_files.empty();
    }

    // Open and track a new file
    bool open_file(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY | O_NONBLOCK);
        if (fd < 0) {
            m_log->error("Failed to open file: {}", path);
            return false;
        }

        // Get inode for rotation detection
        struct stat st;
        if (::fstat(fd, &st) < 0) {
            m_log->error("fstat failed for {}: {}", path, strerror(errno));
            ::close(fd);
            return false;
        }

        tracked_file file;
        file.path = path;
        file.fd = fd;
        file.inode = st.st_ino;
        file.position = 0;
        file.eof_reached = false;
        file.has_error = false;

        // Detect compression format
        compression_format fmt = detect_compression_from_filename(path);
        if (fmt == compression_format::none) {
            // Try magic bytes detection
            char magic[4];
            ssize_t bytes = ::read(fd, magic, sizeof(magic));
            if (bytes == sizeof(magic)) {
                fmt = detect_compression(magic, sizeof(magic));
            }
            // Reset file position
            if (::lseek(fd, 0, SEEK_SET) < 0) {
                m_log->error("Failed to reset file position for {}: {}", path, strerror(errno));
                ::close(fd);
                return false;
            }
        }

        if (fmt != compression_format::none) {
            m_log->debug("Detected {} compressed file: {}",
                        fmt == compression_format::gzip ? "gzip" : "zstd", path);
            file.decompressor = std::make_unique<decompression_reader>(fd, fmt, m_log);
            file.format = fmt;
        }

        // For follow mode, start at end of file (like tail -f)
        // Note: Follow mode not fully supported for compressed files
        if (m_follow && !file.decompressor) {
            file.position = ::lseek(fd, 0, SEEK_END);
            if (file.position < 0) file.position = 0;
        }

        m_files[path] = std::move(file);
        return true;
    }

    // Close a specific file
    void close_file(const std::string& path) {
        auto it = m_files.find(path);
        if (it != m_files.end()) {
            if (it->second.fd >= 0) {
                ::close(it->second.fd);
            }
            m_files.erase(it);
        }
    }

    // Close all tracked files
    void close_all_files() {
        for (auto& [path, file] : m_files) {
            if (file.fd >= 0) {
                ::close(file.fd);
                file.fd = -1;
            }
        }
        m_files.clear();
    }

    // Check for file rotations (inode changes)
    void check_file_rotations() {
        std::vector<std::string> deleted_paths;
        std::vector<std::string> rotated_paths;

        for (auto& [path, file] : m_files) {
            if (file.has_error) continue;

            struct stat st;
            if (::stat(path.c_str(), &st) < 0) {
                m_log->info("File deleted: {}", path);
                deleted_paths.push_back(path);
                continue;
            }

            if (st.st_ino != file.inode) {
                m_log->info("File rotated: {} (inode {} -> {})", path, file.inode, st.st_ino);
                rotated_paths.push_back(path);
                continue;
            }

            if (st.st_size < file.position) {
                m_log->info("File truncated: {}", path);
                file.position = 0;
                ::lseek(file.fd, 0, SEEK_SET);
                file.eof_reached = false;
            }
        }

        for (const auto& path : deleted_paths) {
            close_file(path);
        }

        for (const auto& path : rotated_paths) {
            auto it = m_files.find(path);
            if (it != m_files.end()) {
                ::close(it->second.fd);
                m_files.erase(it);
            }
            if (!open_file(path)) {
                m_log->error("Failed to reopen rotated file: {}", path);
            }
        }
    }

    asio::io_context& m_ioc;
    std::vector<std::string> m_patterns;
    bool m_follow;
    int m_poll_interval_ms;
    std::shared_ptr<spdlog::logger> m_log;

    std::map<std::string, tracked_file> m_files;  // path -> file state
    std::set<std::string> m_extracted_zips;       // zip files already extracted
    std::vector<std::unique_ptr<zip_temp_cleanup>> m_zip_cleanups;  // cleanup handlers
};

} // namespace nats_asio
