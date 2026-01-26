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

#include <cstring>
#include <span>
#include <vector>
#include <zstd.h>

namespace nats_asio {

// ============================================================================
// Compression utilities using zstd
// ============================================================================

class zstd_compressor {
public:
    zstd_compressor(int level = 3) : m_level(level) {
        m_cctx = ZSTD_createCCtx();
        m_dctx = ZSTD_createDCtx();
    }

    ~zstd_compressor() {
        if (m_cctx) ZSTD_freeCCtx(m_cctx);
        if (m_dctx) ZSTD_freeDCtx(m_dctx);
    }

    // Compress data, returns compressed bytes (empty on error)
    std::vector<char> compress(std::span<const char> input) {
        size_t bound = ZSTD_compressBound(input.size());
        std::vector<char> output(bound);

        size_t compressed_size = ZSTD_compressCCtx(
            m_cctx, output.data(), output.size(),
            input.data(), input.size(), m_level);

        if (ZSTD_isError(compressed_size)) {
            return {};
        }

        output.resize(compressed_size);
        return output;
    }

    // Decompress data, returns decompressed bytes (empty on error)
    std::vector<char> decompress(std::span<const char> input) {
        unsigned long long decompressed_size = ZSTD_getFrameContentSize(input.data(), input.size());
        if (decompressed_size == ZSTD_CONTENTSIZE_ERROR ||
            decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            return {};
        }

        std::vector<char> output(decompressed_size);
        size_t result = ZSTD_decompressDCtx(
            m_dctx, output.data(), output.size(),
            input.data(), input.size());

        if (ZSTD_isError(result)) {
            return {};
        }

        output.resize(result);
        return output;
    }

    // Check if data appears to be zstd compressed
    static bool is_compressed(std::span<const char> data) {
        if (data.size() < 4) return false;
        // zstd magic number: 0xFD2FB528
        unsigned int magic = ZSTD_MAGICNUMBER;
        return std::memcmp(data.data(), &magic, 4) == 0;
    }

private:
    ZSTD_CCtx* m_cctx = nullptr;
    ZSTD_DCtx* m_dctx = nullptr;
    int m_level;
};

} // namespace nats_asio
