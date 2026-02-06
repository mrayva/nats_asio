/*
MIT License

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

#include <zerialize/zerialize.hpp>
#include <span>
#include <string>
#include <optional>
#include <spdlog/spdlog.h>

namespace nats_tool {

// Supported binary formats for deserialization
enum class binary_format {
    msgpack,
    cbor,
    flexbuffers,
    zera
};

// Parse format string to enum
inline std::optional<binary_format> parse_format(const std::string& fmt_str) {
    if (fmt_str == "msgpack") return binary_format::msgpack;
    if (fmt_str == "cbor") return binary_format::cbor;
    if (fmt_str == "flexbuffers") return binary_format::flexbuffers;
    if (fmt_str == "zera") return binary_format::zera;
    return std::nullopt;
}

// Deserialize binary payload to compact JSON string
// Returns nullopt on error (malformed data)
inline std::optional<std::string> deserialize_to_json(
    std::span<const char> payload,
    binary_format format,
    std::shared_ptr<spdlog::logger> log = nullptr) {

    try {
        // Convert span to vector for zerialize
        std::vector<std::byte> data(payload.size());
        std::memcpy(data.data(), payload.data(), payload.size());

        // Deserialize based on format
        switch (format) {
            case binary_format::msgpack: {
                auto doc = zerialize::from_msgpack(data);
                return zerialize::to_json_string(doc);
            }
            case binary_format::cbor: {
                auto doc = zerialize::from_cbor(data);
                return zerialize::to_json_string(doc);
            }
            case binary_format::flexbuffers: {
                auto doc = zerialize::from_flexbuffers(data);
                return zerialize::to_json_string(doc);
            }
            case binary_format::zera: {
                auto doc = zerialize::from_zera(data);
                return zerialize::to_json_string(doc);
            }
        }
    } catch (const std::exception& e) {
        if (log) {
            log->debug("Failed to deserialize payload: {}", e.what());
        }
        return std::nullopt;
    }

    return std::nullopt; // Should not reach here
}

// Serialize JSON string to binary format
// Returns nullopt on error (malformed JSON)
inline std::optional<std::vector<std::byte>> serialize_from_json(
    const std::string& json_str,
    binary_format format,
    std::shared_ptr<spdlog::logger> log = nullptr) {

    try {
        // Parse JSON string
        auto doc = zerialize::from_json_string(json_str);

        // Serialize to binary format
        switch (format) {
            case binary_format::msgpack:
                return zerialize::to_msgpack(doc);
            case binary_format::cbor:
                return zerialize::to_cbor(doc);
            case binary_format::flexbuffers:
                return zerialize::to_flexbuffers(doc);
            case binary_format::zera:
                return zerialize::to_zera(doc);
        }
    } catch (const std::exception& e) {
        if (log) {
            log->debug("Failed to serialize JSON: {}", e.what());
        }
        return std::nullopt;
    }

    return std::nullopt; // Should not reach here
}

// Error tracking for deserializer
class deserializer_stats {
public:
    deserializer_stats(std::size_t max_bad_messages = 0, double max_bad_percentage = 0.0)
        : m_max_bad_messages(max_bad_messages), m_max_bad_percentage(max_bad_percentage),
          m_total_messages(0), m_bad_messages(0) {}

    // Record a successful deserialization
    void record_success() {
        m_total_messages++;
    }

    // Record a failed deserialization
    // Returns true if error threshold exceeded (should exit)
    bool record_failure() {
        m_total_messages++;
        m_bad_messages++;

        // Check absolute threshold
        if (m_max_bad_messages > 0 && m_bad_messages >= m_max_bad_messages) {
            return true;
        }

        // Check percentage threshold
        if (m_max_bad_percentage > 0.0 && m_total_messages >= 100) {
            double bad_pct = (100.0 * m_bad_messages) / m_total_messages;
            if (bad_pct >= m_max_bad_percentage) {
                return true;
            }
        }

        return false;
    }

    std::size_t total_messages() const { return m_total_messages; }
    std::size_t bad_messages() const { return m_bad_messages; }

    double bad_percentage() const {
        if (m_total_messages == 0) return 0.0;
        return (100.0 * m_bad_messages) / m_total_messages;
    }

private:
    std::size_t m_max_bad_messages;
    double m_max_bad_percentage;
    std::size_t m_total_messages;
    std::size_t m_bad_messages;
};

} // namespace nats_tool
