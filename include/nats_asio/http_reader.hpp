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
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/write.hpp>
#include <memory>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>
#include <openssl/ssl.h>

namespace nats_asio {

// ============================================================================
// Generic input reader configuration - supports stdin, file, HTTP, and follow mode
// ============================================================================

struct input_source_config {
    std::string file_path;              // Empty = use stdin (unless http_url is set)
    bool follow = false;                // Continuously read new data (like tail -f)
    int poll_interval_ms = 100;         // Poll interval for follow mode

    // HTTP source options
    std::string http_url;               // HTTP URL to fetch from (empty = not HTTP)
    std::string http_method = "POST";   // HTTP method (GET, POST)
    std::string http_body;              // Request body for POST
    std::vector<std::pair<std::string, std::string>> http_headers;  // Custom headers
    int http_timeout_ms = 30000;        // Connection timeout
    bool http_insecure = false;         // Skip SSL verification
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

        try {
            // Resolve hostname
            auto endpoints = co_await m_resolver.async_resolve(host, port, asio::use_awaitable);

            if (m_use_ssl) {
                m_ssl_socket = std::make_unique<asio::ssl::stream<asio::ip::tcp::socket>>(m_ioc, m_ssl_ctx);

                // Set SNI hostname
                if (!SSL_set_tlsext_host_name(m_ssl_socket->native_handle(), host.c_str())) {
                    m_log->error("Failed to set SNI hostname");
                    co_return false;
                }

                // Connect
                co_await asio::async_connect(m_ssl_socket->lowest_layer(), endpoints, asio::use_awaitable);

                // SSL handshake
                co_await m_ssl_socket->async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
            } else {
                m_socket = std::make_unique<asio::ip::tcp::socket>(m_ioc);
                co_await asio::async_connect(*m_socket, endpoints, asio::use_awaitable);
            }

            // Send HTTP request
            co_await send_request();

            // Read and validate response headers
            if (!co_await read_response_headers()) {
                co_return false;
            }

            m_connected = true;
            co_return true;

        } catch (const std::exception& e) {
            m_log->error("HTTP connection failed: {}", e.what());
            co_return false;
        }
    }

    // Read next line from HTTP response body
    asio::awaitable<std::tuple<std::string, bool, bool>> read_line() {
        if (!m_connected) {
            co_return std::make_tuple("", true, true);
        }

        // Check if we have a complete line in buffer
        auto newline_pos = m_buffer.find('\n');
        if (newline_pos != std::string::npos) {
            std::string line = m_buffer.substr(0, newline_pos);
            m_buffer.erase(0, newline_pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            co_return std::make_tuple(std::move(line), false, false);
        }

        // Read more data
        char read_buf[8192];
        try {
            size_t bytes_read;
            if (m_use_ssl) {
                bytes_read = co_await m_ssl_socket->async_read_some(
                    asio::buffer(read_buf, sizeof(read_buf)), asio::use_awaitable);
            } else {
                bytes_read = co_await m_socket->async_read_some(
                    asio::buffer(read_buf, sizeof(read_buf)), asio::use_awaitable);
            }

            if (bytes_read == 0) {
                // Connection closed
                if (!m_buffer.empty()) {
                    std::string line = std::move(m_buffer);
                    m_buffer.clear();
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    co_return std::make_tuple(std::move(line), true, false);
                }
                co_return std::make_tuple("", true, false);
            }

            m_buffer.append(read_buf, bytes_read);

            // Check for complete line again
            newline_pos = m_buffer.find('\n');
            if (newline_pos != std::string::npos) {
                std::string line = m_buffer.substr(0, newline_pos);
                m_buffer.erase(0, newline_pos + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                co_return std::make_tuple(std::move(line), false, false);
            }

            // No complete line yet, continue reading
            co_return co_await read_line();

        } catch (const asio::system_error& e) {
            if (e.code() == asio::error::eof) {
                if (!m_buffer.empty()) {
                    std::string line = std::move(m_buffer);
                    m_buffer.clear();
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    co_return std::make_tuple(std::move(line), true, false);
                }
                co_return std::make_tuple("", true, false);
            }
            m_log->error("HTTP read error: {}", e.what());
            co_return std::make_tuple("", true, true);
        }
    }

    bool is_follow_mode() const { return true; }  // HTTP streaming is always follow-like

private:
    asio::awaitable<void> send_request() {
        std::ostringstream request;

        // Request line
        request << m_config.http_method << " " << m_path << " HTTP/1.1\r\n";

        // Required headers
        request << "Host: " << m_host << "\r\n";
        request << "Connection: keep-alive\r\n";
        request << "Accept: */*\r\n";

        // Custom headers
        for (const auto& [key, value] : m_config.http_headers) {
            request << key << ": " << value << "\r\n";
        }

        // Content-Length for POST with body
        if (!m_config.http_body.empty()) {
            request << "Content-Length: " << m_config.http_body.size() << "\r\n";
            if (std::find_if(m_config.http_headers.begin(), m_config.http_headers.end(),
                    [](const auto& h) { return h.first == "Content-Type"; }) == m_config.http_headers.end()) {
                request << "Content-Type: application/json\r\n";
            }
        }

        request << "\r\n";

        // Add body if present
        if (!m_config.http_body.empty()) {
            request << m_config.http_body;
        }

        std::string req_str = request.str();
        m_log->debug("HTTP request:\n{}", req_str);

        if (m_use_ssl) {
            co_await asio::async_write(*m_ssl_socket, asio::buffer(req_str), asio::use_awaitable);
        } else {
            co_await asio::async_write(*m_socket, asio::buffer(req_str), asio::use_awaitable);
        }
    }

    asio::awaitable<bool> read_response_headers() {
        // Read until we find \r\n\r\n (end of headers)
        char read_buf[4096];
        std::string headers;

        while (headers.find("\r\n\r\n") == std::string::npos) {
            size_t bytes_read;
            if (m_use_ssl) {
                bytes_read = co_await m_ssl_socket->async_read_some(
                    asio::buffer(read_buf, sizeof(read_buf)), asio::use_awaitable);
            } else {
                bytes_read = co_await m_socket->async_read_some(
                    asio::buffer(read_buf, sizeof(read_buf)), asio::use_awaitable);
            }

            if (bytes_read == 0) {
                m_log->error("Connection closed while reading headers");
                co_return false;
            }

            headers.append(read_buf, bytes_read);
        }

        // Split headers from body
        auto header_end = headers.find("\r\n\r\n");
        m_buffer = headers.substr(header_end + 4);  // Body starts after \r\n\r\n
        headers = headers.substr(0, header_end);

        m_log->debug("HTTP response headers:\n{}", headers);

        // Parse status line
        auto first_line_end = headers.find("\r\n");
        std::string status_line = headers.substr(0, first_line_end);

        // Check for 2xx status
        if (status_line.find(" 200 ") == std::string::npos &&
            status_line.find(" 201 ") == std::string::npos &&
            status_line.find(" 202 ") == std::string::npos) {
            m_log->error("HTTP error: {}", status_line);
            co_return false;
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
};

} // namespace nats_asio
