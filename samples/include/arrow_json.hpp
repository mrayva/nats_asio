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

// Only ever compiled in when NATS_TOOL_HAS_ARROW is defined (see
// CMakeLists.txt's NATS_ASIO_ENABLE_ARROW option) - zerialize_json.hpp only
// #includes this header inside the same #ifdef, so there's no need for an
// internal guard here.
//
// Arrow isn't a zerialize protocol (see pg_arrow's README for why - its
// physical layout doesn't fit zerialize's per-value Writer interface), so
// this decodes directly against the real libarrow C++ API rather than going
// through zerialize::translate<JSON>() the way the other 7 --format
// backends do. Decode-only, matching pg_arrow's own columnar-batch-only
// design: rows_to_arrow() has no single-row form, so there's no natural
// "pub --format arrow" encode path either.
//
// Column-walking logic mirrors pg_arrow.cpp's array_column_to_jsonb() (same
// project, same author) - built for a different target tree (nlohmann::json
// here vs. PostgreSQL's JsonbValue there) since nats_tool has no reason to
// depend on PostgreSQL headers, but the type dispatch is the same.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <nlohmann/json.hpp>
#include <span>
#include <string>
#include <optional>
#include <cstdint>
#include <cstdio>

namespace nats_tool {

// Same "~b"/"base64" blob tag convention zerialize's own JSON protocol
// writer uses for binary values (see zerialize/protocols/json.hpp's
// Serializer::binary()) - matches what every other --format's blob/bytea
// columns already look like in nats_tool's --json output.
inline nlohmann::json arrow_binary_to_json(std::string_view bytes)
{
    static constexpr char kBase64Chars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((bytes.size() + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 2 < bytes.size(); i += 3) {
        uint32_t n = (static_cast<uint8_t>(bytes[i]) << 16) |
                     (static_cast<uint8_t>(bytes[i + 1]) << 8) |
                     static_cast<uint8_t>(bytes[i + 2]);
        out.push_back(kBase64Chars[(n >> 18) & 0x3F]);
        out.push_back(kBase64Chars[(n >> 12) & 0x3F]);
        out.push_back(kBase64Chars[(n >> 6) & 0x3F]);
        out.push_back(kBase64Chars[n & 0x3F]);
    }
    size_t rem = bytes.size() - i;
    if (rem == 1) {
        uint32_t n = static_cast<uint8_t>(bytes[i]) << 16;
        out.push_back(kBase64Chars[(n >> 18) & 0x3F]);
        out.push_back(kBase64Chars[(n >> 12) & 0x3F]);
        out.push_back('=');
        out.push_back('=');
    } else if (rem == 2) {
        uint32_t n = (static_cast<uint8_t>(bytes[i]) << 16) | (static_cast<uint8_t>(bytes[i + 1]) << 8);
        out.push_back(kBase64Chars[(n >> 18) & 0x3F]);
        out.push_back(kBase64Chars[(n >> 12) & 0x3F]);
        out.push_back(kBase64Chars[(n >> 6) & 0x3F]);
        out.push_back('=');
    }
    return nlohmann::json::array({"~b", out, "base64"});
}

// ISO 8601 text for Date32 (days since Unix epoch) / Timestamp (micros
// since Unix epoch, optionally UTC) - unlike pg_arrow.cpp's arrow_to_jsonb
// (which deliberately mirrors pg_zerialize's raw-epoch-integer convention
// for exact byte-for-byte cross-format comparison against its own
// reference), nats_tool has no equivalent "must match an existing
// convention" constraint here - Arrow is the first format nats_tool
// decodes that has genuine date/timestamp *types* at all (every other
// format just carries whatever raw integer pg_zerialize chose to put
// there), so human-readable ISO text is the more useful choice for a
// --json inspection tool.
inline std::string arrow_date32_to_iso(int32_t unix_days)
{
    // Days -> (year, month, day) via civil_from_days (Howard Hinnant's
    // well-known proleptic-Gregorian algorithm) - avoids pulling in a
    // full date/calendar library for one conversion.
    int64_t z = unix_days + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    uint64_t doe = static_cast<uint64_t>(z - era * 146097);
    uint64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y = static_cast<int64_t>(yoe) + era * 400;
    uint64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    uint64_t mp = (5 * doy + 2) / 153;
    uint64_t d = doy - (153 * mp + 2) / 5 + 1;
    uint64_t m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);

    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u",
                  static_cast<long long>(y), static_cast<unsigned>(m), static_cast<unsigned>(d));
    return std::string(buf);
}

inline std::string arrow_timestamp_micros_to_iso(int64_t unix_micros, bool utc)
{
    int64_t days = unix_micros >= 0
        ? unix_micros / 86400000000LL
        : -((-unix_micros + 86399999999LL) / 86400000000LL);
    int64_t rem_micros = unix_micros - days * 86400000000LL;
    int64_t hh = rem_micros / 3600000000LL;
    rem_micros -= hh * 3600000000LL;
    int64_t mm = rem_micros / 60000000LL;
    rem_micros -= mm * 60000000LL;
    int64_t ss = rem_micros / 1000000LL;
    int64_t us = rem_micros - ss * 1000000LL;

    std::string date_part = arrow_date32_to_iso(static_cast<int32_t>(days));
    char buf[48];
    std::snprintf(buf, sizeof(buf), "T%02lld:%02lld:%02lld.%06lld",
                  static_cast<long long>(hh), static_cast<long long>(mm),
                  static_cast<long long>(ss), static_cast<long long>(us));
    return date_part + buf + (utc ? "Z" : "");
}

// One column -> a JSON array of its values (columnar shape) - the raw
// building block both the plain and --expand_columnar output paths below
// use.
inline nlohmann::json arrow_column_to_json_array(const std::shared_ptr<arrow::Array>& array)
{
    nlohmann::json out = nlohmann::json::array();
    for (int64_t i = 0; i < array->length(); i++) {
        if (array->IsNull(i)) {
            out.push_back(nullptr);
            continue;
        }
        switch (array->type_id()) {
            case arrow::Type::INT16:
                out.push_back(static_cast<const arrow::Int16Array&>(*array).Value(i));
                break;
            case arrow::Type::INT32:
                out.push_back(static_cast<const arrow::Int32Array&>(*array).Value(i));
                break;
            case arrow::Type::INT64:
                out.push_back(static_cast<const arrow::Int64Array&>(*array).Value(i));
                break;
            case arrow::Type::FLOAT:
                out.push_back(static_cast<const arrow::FloatArray&>(*array).Value(i));
                break;
            case arrow::Type::DOUBLE:
                out.push_back(static_cast<const arrow::DoubleArray&>(*array).Value(i));
                break;
            case arrow::Type::BOOL:
                out.push_back(static_cast<const arrow::BooleanArray&>(*array).Value(i));
                break;
            case arrow::Type::STRING: {
                auto sv = static_cast<const arrow::StringArray&>(*array).GetView(i);
                out.push_back(std::string(sv));
                break;
            }
            case arrow::Type::BINARY: {
                auto sv = static_cast<const arrow::BinaryArray&>(*array).GetView(i);
                out.push_back(arrow_binary_to_json(sv));
                break;
            }
            case arrow::Type::DECIMAL128: {
                // Exact decimal text, not a JSON number - avoids float
                // precision loss and matches what a jq/JSON consumer would
                // expect for a "money"-shaped value.
                out.push_back(static_cast<const arrow::Decimal128Array&>(*array).FormatValue(i));
                break;
            }
            case arrow::Type::DATE32:
                out.push_back(arrow_date32_to_iso(static_cast<const arrow::Date32Array&>(*array).Value(i)));
                break;
            case arrow::Type::TIMESTAMP: {
                auto& ts_type = static_cast<const arrow::TimestampType&>(*array->type());
                bool utc = !ts_type.timezone().empty();
                out.push_back(arrow_timestamp_micros_to_iso(
                    static_cast<const arrow::TimestampArray&>(*array).Value(i), utc));
                break;
            }
            default:
                out.push_back("<unsupported Arrow type>");
                break;
        }
    }
    return out;
}

// Decodes one Arrow IPC stream (one RecordBatch, as rows_to_arrow()
// produces) to JSON text.
//
// expand_columnar: mirrors deserialize_to_json()'s own flag of the same
// name for the other 6 --format backends (see zerialize_json.hpp) - a
// decoded RecordBatch is exactly the same {"col1":[...],"col2":[...]}
// columnar shape zerialize::expand_columnar() targets, so --expand_columnar
// applies here too: false gives the raw columnar object, true expands it
// into a JSON array of per-row objects.
//
// Returns nullopt on any decode failure (malformed/truncated Arrow bytes),
// same contract deserialize_to_json() has for the other formats.
inline std::optional<std::string> deserialize_arrow_to_json(
    std::span<const char> payload, bool expand_columnar)
{
    try {
        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(payload.data()), static_cast<int64_t>(payload.size()));
        auto reader_stream = std::make_shared<arrow::io::BufferReader>(buffer);

        auto stream_reader_result = arrow::ipc::RecordBatchStreamReader::Open(reader_stream);
        if (!stream_reader_result.ok()) {
            return std::nullopt;
        }
        auto stream_reader = stream_reader_result.ValueOrDie();

        std::shared_ptr<arrow::RecordBatch> batch;
        if (!stream_reader->ReadNext(&batch).ok()) {
            return std::nullopt;
        }

        if (!batch) {
            // Zero-field schema - see rows_to_arrow()'s own comment on why
            // this can happen (a genuinely columnless composite type).
            return expand_columnar ? nlohmann::json::array().dump() : nlohmann::json::object().dump();
        }

        if (!expand_columnar) {
            nlohmann::json obj = nlohmann::json::object();
            for (int i = 0; i < batch->num_columns(); i++) {
                obj[batch->column_name(i)] = arrow_column_to_json_array(batch->column(i));
            }
            return obj.dump();
        }

        // Expand: build each column once (column-major, cheap), then
        // transpose into row-major - same approach pg_arrow.cpp and
        // zerialize::expand_columnar() both use, for the same reason (an
        // Arrow column's own value accessors are naturally column-major;
        // re-fetching cell-by-cell in row-major order would mean redundant
        // per-cell dispatch overhead for no benefit here, unlike a
        // streaming Writer that can't hold a whole document in memory -
        // this already holds the whole decoded RecordBatch).
        std::vector<std::string> names;
        std::vector<nlohmann::json> columns;
        names.reserve(batch->num_columns());
        columns.reserve(batch->num_columns());
        for (int i = 0; i < batch->num_columns(); i++) {
            names.push_back(batch->column_name(i));
            columns.push_back(arrow_column_to_json_array(batch->column(i)));
        }

        nlohmann::json rows = nlohmann::json::array();
        int64_t n = batch->num_rows();
        for (int64_t r = 0; r < n; r++) {
            nlohmann::json row = nlohmann::json::object();
            for (size_t c = 0; c < names.size(); c++) {
                row[names[c]] = columns[c][static_cast<size_t>(r)];
            }
            rows.push_back(std::move(row));
        }
        return rows.dump();
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

} // namespace nats_tool
