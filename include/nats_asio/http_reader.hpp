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

#include <algorithm>
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/connect.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/write.hpp>
#include <cctype>
#include <charconv>
#include <chrono>
#include <memory>
#include <openssl/ssl.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace nats_asio {

// ============================================================================
// Generic input reader configuration - supports stdin, file, HTTP, and follow mode
// ============================================================================

struct input_source_config {
    std::string file_path;                      // Single file (deprecated, use file_patterns)
    std::vector<std::string> file_patterns;     // Glob patterns for multiple files
    bool follow = false;                        // Continuously read new data (like tail -f)
    int poll_interval_ms = 100;                 // Poll interval for follow mode

    // HTTP source options
    std::string http_url;               // HTTP URL to fetch from (empty = not HTTP)
    std::string http_method = "POST";   // HTTP method (GET, POST)
    std::string http_body;              // Request body for POST
    std::vector<std::pair<std::string, std::string>> http_headers;  // Custom headers
    int http_timeout_ms = 30000;        // Connection timeout
    bool http_insecure = false;         // Skip SSL verification

    // Helper to check if using multiple file patterns
    bool is_multi_file() const {
        return !file_patterns.empty() || file_path.find('*') != std::string::npos ||
               file_path.find('?') != std::string::npos;
    }

    // Get all patterns (including single file_path for backward compat)
    std::vector<std::string> get_patterns() const {
        if (!file_patterns.empty()) {
            return file_patterns;
        }
        if (!file_path.empty()) {
            return {file_path};
        }
        return {};
    }
};

// ============================================================================
// HTTP streaming reader - reads line-delimited data from HTTP endpoint (standalone ASIO)
// ============================================================================

class async_http_reader {
public:
    async_http_reader(asio::io_context& ioc, const input_source_config& config,
                      std::shared_ptr<spdlog::logger> log)
        : m_ioc(ioc), m_config(config), m_log(std::move(log)),
          m_resolver(ioc), m_ssl_ctx(asio::ssl::context::tlsv12_client) {
        if (m_config.http_insecure) {
            m_ssl_ctx.set_verify_mode(asio::ssl::verify_none);
        } else {
            m_ssl_ctx.set_default_verify_paths();
            m_ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
        }
    }

    // Parse URL into components
    bool parse_url(const std::string& url, std::string& protocol, std::string& host,
                   std::string& port, std::string& path) {
        std::string u = url;
        if (u.find("https://") == 0) {
            protocol = "https";
            u = u.substr(8);
        } else if (u.find("http://") == 0) {
            protocol = "http";
            u = u.substr(7);
        } else {
            m_log->error("Invalid URL protocol: {}", url);
            return false;
        }

        auto path_pos = u.find('/');
        if (path_pos != std::string::npos) {
            path = u.substr(path_pos);
            u = u.substr(0, path_pos);
        } else {
            path = "/";
        }

        auto port_pos = u.find(':');
        if (port_pos != std::string::npos) {
            host = u.substr(0, port_pos);
            port = u.substr(port_pos + 1);
        } else {
            host = u;
            port = (protocol == "https") ? "443" : "80";
        }

        return true;
    }

    // Initialize and connect
    asio::awaitable<bool> init() {
        std::string protocol, host, port, path;
        if (!parse_url(m_config.http_url, protocol, host, port, path)) {
            co_return false;
        }

        m_host = host;
        m_path = path;
        m_use_ssl = (protocol == "https");

        asio::ip::tcp::resolver::results_type endpoints;
        auto [resolve_ec, resolve_timeout] = co_await run_with_timeout([&]() -> asio::awaitable<asio::error_code> {
            auto [ec, resolved] = co_await m_resolver.async_resolve(host, port, asio::as_tuple(asio::use_awaitable));
            if (!ec) {
                endpoints = std::move(resolved);
            }
            co_return ec;
        });

        if (resolve_timeout) {
            m_log->error("HTTP resolve timeout after {}ms", timeout_duration().count());
            co_return false;
        }
        if (resolve_ec) {
            m_log->error("HTTP resolve failed: {}", resolve_ec.message());
            co_return false;
        }

        if (m_use_ssl) {
            m_ssl_socket = std::make_unique<asio::ssl::stream<asio::ip::tcp::socket>>(m_ioc, m_ssl_ctx);

            if (!SSL_set_tlsext_host_name(m_ssl_socket->native_handle(), host.c_str())) {
                m_log->error("Failed to set SNI hostname");
                co_return false;
            }

            auto [connect_ec, connect_timeout] =
                co_await run_with_timeout([&]() -> asio::awaitable<asio::error_code> {
                    auto [ec, ignored_endpoint] = co_await asio::async_connect(
                        m_ssl_socket->lowest_layer(), endpoints, asio::as_tuple(asio::use_awaitable));
                    (void)ignored_endpoint;
                    co_return ec;
                });

            if (connect_timeout) {
                m_log->error("HTTP connect timeout after {}ms", timeout_duration().count());
                co_return false;
            }
            if (connect_ec) {
                m_log->error("HTTP connect failed: {}", connect_ec.message());
                co_return false;
            }

            auto [handshake_ec, handshake_timeout] =
                co_await run_with_timeout([&]() -> asio::awaitable<asio::error_code> {
                    auto [ec] = co_await m_ssl_socket->async_handshake(
                        asio::ssl::stream_base::client, asio::as_tuple(asio::use_awaitable));
                    co_return ec;
                });

            if (handshake_timeout) {
                m_log->error("HTTP TLS handshake timeout after {}ms", timeout_duration().count());
                co_return false;
            }
            if (handshake_ec) {
                m_log->error("HTTP TLS handshake failed: {}", handshake_ec.message());
                co_return false;
            }
        } else {
            m_socket = std::make_unique<asio::ip::tcp::socket>(m_ioc);

            auto [connect_ec, connect_timeout] =
                co_await run_with_timeout([&]() -> asio::awaitable<asio::error_code> {
                    auto [ec, ignored_endpoint] = co_await asio::async_connect(
                        *m_socket, endpoints, asio::as_tuple(asio::use_awaitable));
                    (void)ignored_endpoint;
                    co_return ec;
                });

            if (connect_timeout) {
                m_log->error("HTTP connect timeout after {}ms", timeout_duration().count());
                co_return false;
            }
            if (connect_ec) {
                m_log->error("HTTP connect failed: {}", connect_ec.message());
                co_return false;
            }
        }

        if (!co_await send_request()) {
            co_return false;
        }

        if (!co_await read_response_headers()) {
            co_return false;
        }

        m_connected = true;
        co_return true;
    }

    // Read next line from HTTP response body
    asio::awaitable<std::tuple<std::string, bool, bool>> read_line() {
        if (!m_connected) {
            co_return std::make_tuple("", true, true);
        }

        char read_buf[8192];

        for (;;) {
            auto newline_pos = m_buffer.find('\n');
            if (newline_pos != std::string::npos) {
                std::string line = m_buffer.substr(0, newline_pos);
                m_buffer.erase(0, newline_pos + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                co_return std::make_tuple(std::move(line), false, false);
            }

            if (m_chunked && m_chunk_stream_complete) {
                if (!m_buffer.empty()) {
                    std::string line = std::move(m_buffer);
                    m_buffer.clear();
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    co_return std::make_tuple(std::move(line), true, false);
                }
                co_return std::make_tuple("", true, false);
            }

            auto [ec, bytes_read, timed_out] = co_await read_some_with_timeout(read_buf, sizeof(read_buf));
            if (timed_out) {
                m_log->error("HTTP read timeout after {}ms", timeout_duration().count());
                co_return std::make_tuple("", true, true);
            }

            if (ec) {
                if (ec == asio::error::eof) {
                    if (m_chunked) {
                        if (!decode_chunked_body()) {
                            m_log->error("HTTP chunked body decode failed");
                            co_return std::make_tuple("", true, true);
                        }
                        if (!m_chunk_stream_complete) {
                            m_log->error("HTTP chunked body ended before terminal chunk");
                            co_return std::make_tuple("", true, true);
                        }
                    }

                    if (!m_buffer.empty()) {
                        std::string line = std::move(m_buffer);
                        m_buffer.clear();
                        if (!line.empty() && line.back() == '\r') line.pop_back();
                        co_return std::make_tuple(std::move(line), true, false);
                    }
                    co_return std::make_tuple("", true, false);
                }

                m_log->error("HTTP read error: {}", ec.message());
                co_return std::make_tuple("", true, true);
            }

            if (bytes_read == 0) {
                if (!m_buffer.empty()) {
                    std::string line = std::move(m_buffer);
                    m_buffer.clear();
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    co_return std::make_tuple(std::move(line), true, false);
                }
                co_return std::make_tuple("", true, false);
            }

            if (m_chunked) {
                m_chunk_raw_buffer.append(read_buf, bytes_read);
                if (!decode_chunked_body()) {
                    m_log->error("HTTP chunked body decode failed");
                    co_return std::make_tuple("", true, true);
                }
            } else {
                m_buffer.append(read_buf, bytes_read);
            }
        }
    }

    bool is_follow_mode() const { return true; }  // HTTP streaming is always follow-like

private:
    static std::string to_lower_copy(std::string_view sv) {
        std::string out(sv);
        std::transform(out.begin(), out.end(), out.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return out;
    }

    static std::string trim_copy(std::string_view sv) {
        size_t start = 0;
        size_t end = sv.size();
        while (start < end && std::isspace(static_cast<unsigned char>(sv[start]))) {
            ++start;
        }
        while (end > start && std::isspace(static_cast<unsigned char>(sv[end - 1]))) {
            --end;
        }
        return std::string(sv.substr(start, end - start));
    }

    static int parse_status_code(const std::string& status_line) {
        auto first_sp = status_line.find(' ');
        if (first_sp == std::string::npos) {
            return -1;
        }

        auto second_sp = status_line.find(' ', first_sp + 1);
        const size_t code_begin = first_sp + 1;
        const size_t code_len = (second_sp == std::string::npos)
            ? status_line.size() - code_begin
            : second_sp - code_begin;

        int status_code = 0;
        std::string_view code_sv(status_line.data() + code_begin, code_len);
        auto [ptr, ec] = std::from_chars(code_sv.data(), code_sv.data() + code_sv.size(), status_code);
        if (ec != std::errc{} || ptr != code_sv.data() + code_sv.size()) {
            return -1;
        }

        return status_code;
    }

    static bool has_chunked_transfer_encoding(const std::string& headers) {
        std::istringstream lines(headers);
        std::string line;

        bool is_first_line = true;
        while (std::getline(lines, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (is_first_line) {
                is_first_line = false;
                continue;
            }

            auto colon_pos = line.find(':');
            if (colon_pos == std::string::npos) {
                continue;
            }

            std::string name = to_lower_copy(trim_copy(std::string_view(line.data(), colon_pos)));
            if (name != "transfer-encoding") {
                continue;
            }

            std::string value = to_lower_copy(trim_copy(std::string_view(line.data() + colon_pos + 1, line.size() - colon_pos - 1)));
            if (value.find("chunked") != std::string::npos) {
                return true;
            }
        }

        return false;
    }

    std::chrono::milliseconds timeout_duration() const {
        if (m_config.http_timeout_ms <= 0) {
            return std::chrono::milliseconds(30000);
        }
        return std::chrono::milliseconds(m_config.http_timeout_ms);
    }

    void cancel_pending_io() {
        m_resolver.cancel();
        if (m_socket) {
            asio::error_code ignored;
            m_socket->cancel(ignored);
        }
        if (m_ssl_socket) {
            asio::error_code ignored;
            m_ssl_socket->lowest_layer().cancel(ignored);
        }
    }

    template <typename Op>
    asio::awaitable<std::tuple<asio::error_code, bool>> run_with_timeout(Op&& op) {
        bool timed_out = false;
        asio::error_code op_ec;

        asio::steady_timer timer(co_await asio::this_coro::executor);
        timer.expires_after(timeout_duration());
        timer.async_wait([this, &timed_out](const asio::error_code& ec) {
            if (!ec) {
                timed_out = true;
                cancel_pending_io();
            }
        });

        op_ec = co_await op();
        asio::error_code ignored;
        timer.cancel(ignored);

        if (timed_out && op_ec == asio::error::operation_aborted) {
            op_ec = asio::error::timed_out;
        }

        co_return std::make_tuple(op_ec, timed_out);
    }

    asio::awaitable<std::tuple<asio::error_code, std::size_t, bool>>
    read_some_with_timeout(char* read_buf, size_t read_buf_size) {
        std::size_t bytes_read = 0;

        auto [ec, timed_out] = co_await run_with_timeout([&]() -> asio::awaitable<asio::error_code> {
            if (m_use_ssl) {
                auto [read_ec, n] = co_await m_ssl_socket->async_read_some(
                    asio::buffer(read_buf, read_buf_size), asio::as_tuple(asio::use_awaitable));
                bytes_read = n;
                co_return read_ec;
            }

            auto [read_ec, n] = co_await m_socket->async_read_some(
                asio::buffer(read_buf, read_buf_size), asio::as_tuple(asio::use_awaitable));
            bytes_read = n;
            co_return read_ec;
        });

        co_return std::make_tuple(ec, bytes_read, timed_out);
    }

    asio::awaitable<bool> send_request() {
        std::ostringstream request;

        request << m_config.http_method << " " << m_path << " HTTP/1.1\r\n";
        request << "Host: " << m_host << "\r\n";
        request << "Connection: keep-alive\r\n";
        request << "Accept: */*\r\n";

        for (const auto& [key, value] : m_config.http_headers) {
            request << key << ": " << value << "\r\n";
        }

        if (!m_config.http_body.empty()) {
            request << "Content-Length: " << m_config.http_body.size() << "\r\n";
            if (std::find_if(m_config.http_headers.begin(), m_config.http_headers.end(),
                    [](const auto& h) { return h.first == "Content-Type"; }) == m_config.http_headers.end()) {
                request << "Content-Type: application/json\r\n";
            }
        }

        request << "\r\n";
        if (!m_config.http_body.empty()) {
            request << m_config.http_body;
        }

        std::string req_str = request.str();
        m_log->debug("HTTP request:\n{}", req_str);

        std::size_t bytes_written = 0;
        auto [ec, timed_out] = co_await run_with_timeout([&]() -> asio::awaitable<asio::error_code> {
            if (m_use_ssl) {
                auto [write_ec, n] = co_await asio::async_write(
                    *m_ssl_socket, asio::buffer(req_str), asio::as_tuple(asio::use_awaitable));
                bytes_written = n;
                co_return write_ec;
            }

            auto [write_ec, n] = co_await asio::async_write(
                *m_socket, asio::buffer(req_str), asio::as_tuple(asio::use_awaitable));
            bytes_written = n;
            co_return write_ec;
        });

        if (timed_out) {
            m_log->error("HTTP write timeout after {}ms", timeout_duration().count());
            co_return false;
        }
        if (ec) {
            m_log->error("HTTP write failed: {}", ec.message());
            co_return false;
        }

        if (bytes_written != req_str.size()) {
            m_log->error("HTTP write incomplete: wrote {} of {} bytes", bytes_written, req_str.size());
            co_return false;
        }

        co_return true;
    }

    bool decode_chunked_body() {
        for (;;) {
            if (m_chunk_stream_complete) {
                return true;
            }

            if (m_chunk_bytes_remaining == 0) {
                auto line_end = m_chunk_raw_buffer.find("\r\n");
                if (line_end == std::string::npos) {
                    return true;
                }

                std::string chunk_size_line = m_chunk_raw_buffer.substr(0, line_end);
                m_chunk_raw_buffer.erase(0, line_end + 2);

                auto semicolon_pos = chunk_size_line.find(';');
                if (semicolon_pos != std::string::npos) {
                    chunk_size_line.resize(semicolon_pos);
                }

                chunk_size_line = trim_copy(chunk_size_line);
                if (chunk_size_line.empty()) {
                    return false;
                }

                unsigned long long chunk_size = 0;
                auto [ptr, ec] = std::from_chars(
                    chunk_size_line.data(),
                    chunk_size_line.data() + chunk_size_line.size(),
                    chunk_size,
                    16);
                if (ec != std::errc{} || ptr != chunk_size_line.data() + chunk_size_line.size()) {
                    return false;
                }

                if (chunk_size == 0) {
                    if (m_chunk_raw_buffer.size() >= 2 &&
                        m_chunk_raw_buffer[0] == '\r' &&
                        m_chunk_raw_buffer[1] == '\n') {
                        m_chunk_raw_buffer.erase(0, 2);
                        m_chunk_stream_complete = true;
                        return true;
                    }

                    auto trailer_end = m_chunk_raw_buffer.find("\r\n\r\n");
                    if (trailer_end == std::string::npos) {
                        return true;
                    }

                    m_chunk_raw_buffer.erase(0, trailer_end + 4);
                    m_chunk_stream_complete = true;
                    return true;
                }

                m_chunk_bytes_remaining = static_cast<size_t>(chunk_size);
            }

            if (m_chunk_raw_buffer.size() < m_chunk_bytes_remaining + 2) {
                return true;
            }

            m_buffer.append(m_chunk_raw_buffer.data(), m_chunk_bytes_remaining);
            m_chunk_raw_buffer.erase(0, m_chunk_bytes_remaining);

            if (m_chunk_raw_buffer.size() < 2 ||
                m_chunk_raw_buffer[0] != '\r' ||
                m_chunk_raw_buffer[1] != '\n') {
                return false;
            }

            m_chunk_raw_buffer.erase(0, 2);
            m_chunk_bytes_remaining = 0;
        }
    }

    asio::awaitable<bool> read_response_headers() {
        char read_buf[4096];
        std::string headers;
        static constexpr size_t max_headers_size = 64 * 1024;

        while (headers.find("\r\n\r\n") == std::string::npos) {
            auto [ec, bytes_read, timed_out] = co_await read_some_with_timeout(read_buf, sizeof(read_buf));
            if (timed_out) {
                m_log->error("HTTP header read timeout after {}ms", timeout_duration().count());
                co_return false;
            }
            if (ec) {
                m_log->error("HTTP header read failed: {}", ec.message());
                co_return false;
            }
            if (bytes_read == 0) {
                m_log->error("Connection closed while reading headers");
                co_return false;
            }

            headers.append(read_buf, bytes_read);
            if (headers.size() > max_headers_size) {
                m_log->error("HTTP headers exceed {} bytes", max_headers_size);
                co_return false;
            }
        }

        auto header_end = headers.find("\r\n\r\n");
        std::string response_body = headers.substr(header_end + 4);
        headers = headers.substr(0, header_end);

        m_log->debug("HTTP response headers:\n{}", headers);

        auto first_line_end = headers.find("\r\n");
        std::string status_line = headers.substr(0, first_line_end);
        int status_code = parse_status_code(status_line);
        if (status_code < 200 || status_code >= 300) {
            m_log->error("HTTP error: {}", status_line);
            co_return false;
        }

        m_chunked = has_chunked_transfer_encoding(headers);
        if (m_chunked) {
            m_chunk_raw_buffer.append(response_body);
            if (!decode_chunked_body()) {
                m_log->error("HTTP chunked body decode failed");
                co_return false;
            }
        } else {
            m_buffer = std::move(response_body);
        }

        m_log->info("HTTP connected: {}", status_line);
        co_return true;
    }

    asio::io_context& m_ioc;
    input_source_config m_config;
    std::shared_ptr<spdlog::logger> m_log;

    asio::ip::tcp::resolver m_resolver;
    asio::ssl::context m_ssl_ctx;

    std::unique_ptr<asio::ip::tcp::socket> m_socket;
    std::unique_ptr<asio::ssl::stream<asio::ip::tcp::socket>> m_ssl_socket;

    std::string m_host;
    std::string m_path;
    bool m_use_ssl = false;
    bool m_connected = false;
    std::string m_buffer;

    bool m_chunked = false;
    std::string m_chunk_raw_buffer;
    size_t m_chunk_bytes_remaining = 0;
    bool m_chunk_stream_complete = false;
};

} // namespace nats_asio
