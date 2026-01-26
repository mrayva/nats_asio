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

#include <simdjson.h>
#include <span>
#include <string>
#include <string_view>

namespace nats_tool {

// ============================================================================
// Fast JSON parsing using simdjson (for read-heavy workloads)
// ============================================================================

class fast_json_parser {
public:
    // Parse JSON string, returns true on success
    bool parse(const std::string& json_str) {
        m_padded = simdjson::padded_string(json_str);
        auto result = m_parser.parse(m_padded);
        if (result.error()) {
            return false;
        }
        m_doc = std::move(result.value());
        return true;
    }

    // Parse from char span (avoids string copy)
    bool parse(std::span<const char> data) {
        m_padded = simdjson::padded_string(data.data(), data.size());
        auto result = m_parser.parse(m_padded);
        if (result.error()) {
            return false;
        }
        m_doc = std::move(result.value());
        return true;
    }

    // Get string field (returns empty string_view on error)
    std::string_view get_string(const char* field) {
        auto result = m_doc[field].get_string();
        if (result.error()) return {};
        return result.value();
    }

    // Get int64 field (returns 0 on error)
    int64_t get_int(const char* field) {
        auto result = m_doc[field].get_int64();
        if (result.error()) return 0;
        return result.value();
    }

    // Get double field (returns 0.0 on error)
    double get_double(const char* field) {
        auto result = m_doc[field].get_double();
        if (result.error()) return 0.0;
        return result.value();
    }

    // Get bool field (returns false on error)
    bool get_bool(const char* field) {
        auto result = m_doc[field].get_bool();
        if (result.error()) return false;
        return result.value();
    }

    // Get the raw element for complex access patterns
    simdjson::dom::element& doc() { return m_doc; }

private:
    simdjson::dom::parser m_parser;
    simdjson::dom::element m_doc;
    simdjson::padded_string m_padded;
};

} // namespace nats_tool
