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

#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <tuple>
#include <unistd.h>
#include <zlib.h>
#include <zstd.h>

namespace nats_asio {

// ============================================================================
// Compression format detection and streaming decompression
// ============================================================================

enum class compression_format {
    none,
    gzip,
    zstd
};

// Detect compression format from magic bytes
inline compression_format detect_compression(const char* data, size_t size) {
    if (size < 4) return compression_format::none;

    // gzip magic: 0x1f 0x8b
    if (static_cast<unsigned char>(data[0]) == 0x1f &&
        static_cast<unsigned char>(data[1]) == 0x8b) {
        return compression_format::gzip;
    }

    // zstd magic: 0x28 0xb5 0x2f 0xfd
    if (static_cast<unsigned char>(data[0]) == 0x28 &&
        static_cast<unsigned char>(data[1]) == 0xb5 &&
        static_cast<unsigned char>(data[2]) == 0x2f &&
        static_cast<unsigned char>(data[3]) == 0xfd) {
        return compression_format::zstd;
    }

    return compression_format::none;
}

// Detect compression from file extension
inline compression_format detect_compression_from_filename(const std::string& path) {
    if (path.size() > 3 && path.substr(path.size() - 3) == ".gz") {
        return compression_format::gzip;
    }
    if (path.size() > 4 && path.substr(path.size() - 4) == ".zst") {
        return compression_format::zstd;
    }
    if (path.size() > 5 && path.substr(path.size() - 5) == ".zstd") {
        return compression_format::zstd;
    }
    return compression_format::none;
}

// ============================================================================
// Streaming decompression reader
// ============================================================================

class decompression_reader {
public:
    decompression_reader(int fd, compression_format format, std::shared_ptr<spdlog::logger> log)
        : m_fd(fd), m_format(format), m_log(std::move(log)), m_eof(false) {

        if (m_format == compression_format::gzip) {
            init_gzip();
        } else if (m_format == compression_format::zstd) {
            init_zstd();
        }
    }

    ~decompression_reader() {
        cleanup();
    }

    // Read decompressed data into buffer
    // Returns: {bytes_read, eof_reached, error_occurred}
    std::tuple<ssize_t, bool, bool> read(char* buffer, size_t buffer_size) {
        if (m_eof) {
            return {0, true, false};
        }

        if (m_format == compression_format::gzip) {
            return read_gzip(buffer, buffer_size);
        } else if (m_format == compression_format::zstd) {
            return read_zstd(buffer, buffer_size);
        }

        // No compression - direct read
        ssize_t bytes = ::read(m_fd, buffer, buffer_size);
        if (bytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return {0, false, false};  // No data available
            }
            m_log->error("Read error: {}", strerror(errno));
            return {0, true, true};
        }
        if (bytes == 0) {
            m_eof = true;
            return {0, true, false};
        }
        return {bytes, false, false};
    }

    bool is_eof() const { return m_eof; }
    compression_format format() const { return m_format; }

private:
    void init_gzip() {
        m_zstream = std::make_unique<z_stream>();
        m_zstream->zalloc = Z_NULL;
        m_zstream->zfree = Z_NULL;
        m_zstream->opaque = Z_NULL;
        m_zstream->avail_in = 0;
        m_zstream->next_in = Z_NULL;

        // Use inflateInit2 with window bits + 32 for gzip support
        int ret = inflateInit2(m_zstream.get(), 15 + 32);
        if (ret != Z_OK) {
            m_log->error("Failed to initialize gzip decompression: {}", ret);
            m_format = compression_format::none;
        }

        m_compressed_buffer.resize(8192);
    }

    void init_zstd() {
        m_zstd_dctx = ZSTD_createDStream();
        if (!m_zstd_dctx) {
            m_log->error("Failed to initialize zstd decompression");
            m_format = compression_format::none;
            return;
        }

        size_t ret = ZSTD_initDStream(m_zstd_dctx);
        if (ZSTD_isError(ret)) {
            m_log->error("Failed to initialize zstd stream: {}", ZSTD_getErrorName(ret));
            ZSTD_freeDStream(m_zstd_dctx);
            m_zstd_dctx = nullptr;
            m_format = compression_format::none;
            return;
        }

        m_compressed_buffer.resize(ZSTD_DStreamInSize());
        m_zstd_input.src = nullptr;
        m_zstd_input.size = 0;
        m_zstd_input.pos = 0;
    }

    void cleanup() {
        if (m_zstream) {
            inflateEnd(m_zstream.get());
        }
        if (m_zstd_dctx) {
            ZSTD_freeDStream(m_zstd_dctx);
            m_zstd_dctx = nullptr;
        }
    }

    std::tuple<ssize_t, bool, bool> read_gzip(char* buffer, size_t buffer_size) {
        if (!m_zstream) {
            return {0, true, true};
        }

        m_zstream->avail_out = buffer_size;
        m_zstream->next_out = reinterpret_cast<unsigned char*>(buffer);

        while (m_zstream->avail_out > 0) {
            // Need more input data
            if (m_zstream->avail_in == 0) {
                ssize_t bytes = ::read(m_fd, m_compressed_buffer.data(), m_compressed_buffer.size());
                if (bytes < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // No more data available right now
                        ssize_t decompressed = buffer_size - m_zstream->avail_out;
                        return {decompressed, false, false};
                    }
                    m_log->error("Read error: {}", strerror(errno));
                    return {0, true, true};
                }
                if (bytes == 0) {
                    // EOF on input
                    int ret = inflate(m_zstream.get(), Z_FINISH);
                    if (ret != Z_STREAM_END && ret != Z_OK) {
                        m_log->error("gzip decompression error: {}", ret);
                        return {0, true, true};
                    }
                    m_eof = true;
                    ssize_t decompressed = buffer_size - m_zstream->avail_out;
                    return {decompressed, true, false};
                }

                m_zstream->avail_in = bytes;
                m_zstream->next_in = reinterpret_cast<unsigned char*>(m_compressed_buffer.data());
            }

            int ret = inflate(m_zstream.get(), Z_NO_FLUSH);
            if (ret == Z_STREAM_END) {
                m_eof = true;
                ssize_t decompressed = buffer_size - m_zstream->avail_out;
                return {decompressed, true, false};
            }
            if (ret != Z_OK) {
                m_log->error("gzip decompression error: {}", ret);
                return {0, true, true};
            }

            // Check if we have decompressed data
            if (m_zstream->avail_out < buffer_size) {
                ssize_t decompressed = buffer_size - m_zstream->avail_out;
                return {decompressed, false, false};
            }
        }

        return {static_cast<ssize_t>(buffer_size), false, false};
    }

    std::tuple<ssize_t, bool, bool> read_zstd(char* buffer, size_t buffer_size) {
        if (!m_zstd_dctx) {
            return {0, true, true};
        }

        ZSTD_outBuffer output;
        output.dst = buffer;
        output.size = buffer_size;
        output.pos = 0;

        while (output.pos < output.size) {
            // Need more input data
            if (m_zstd_input.pos >= m_zstd_input.size) {
                ssize_t bytes = ::read(m_fd, m_compressed_buffer.data(), m_compressed_buffer.size());
                if (bytes < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // No more data available right now
                        return {static_cast<ssize_t>(output.pos), false, false};
                    }
                    m_log->error("Read error: {}", strerror(errno));
                    return {0, true, true};
                }
                if (bytes == 0) {
                    // EOF on input
                    m_eof = true;
                    return {static_cast<ssize_t>(output.pos), true, false};
                }

                m_zstd_input.src = m_compressed_buffer.data();
                m_zstd_input.size = bytes;
                m_zstd_input.pos = 0;
            }

            size_t ret = ZSTD_decompressStream(m_zstd_dctx, &output, &m_zstd_input);
            if (ZSTD_isError(ret)) {
                m_log->error("zstd decompression error: {}", ZSTD_getErrorName(ret));
                return {0, true, true};
            }

            if (ret == 0) {
                // Frame complete
                m_eof = true;
                return {static_cast<ssize_t>(output.pos), true, false};
            }

            // Check if we have decompressed data
            if (output.pos > 0) {
                return {static_cast<ssize_t>(output.pos), false, false};
            }
        }

        return {static_cast<ssize_t>(output.pos), false, false};
    }

    int m_fd;
    compression_format m_format;
    std::shared_ptr<spdlog::logger> m_log;
    bool m_eof;

    // gzip state
    std::unique_ptr<z_stream> m_zstream;

    // zstd state
    ZSTD_DStream* m_zstd_dctx = nullptr;
    ZSTD_inBuffer m_zstd_input{};

    // Shared compressed buffer
    std::vector<char> m_compressed_buffer;
};

} // namespace nats_asio
