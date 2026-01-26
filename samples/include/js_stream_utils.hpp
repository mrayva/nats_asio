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

#include <asio/awaitable.hpp>
#include <asio/use_awaitable.hpp>
#include <chrono>
#include <nats_asio/nats_asio.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

namespace nats_tool {

// ============================================================================
// JetStream Stream Creation and Management Utilities
// ============================================================================

// Create or update JetStream stream to ensure subject is included
inline asio::awaitable<bool> ensure_stream_for_subject(
    nats_asio::iconnection_sptr conn,
    const std::string& stream_name,
    const std::string& subject,
    std::shared_ptr<spdlog::logger> log,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {

    // First, try to get stream info
    std::string info_subject = "$JS.API.STREAM.INFO." + stream_name;

    nlohmann::json info_request = {{"name", stream_name}};
    std::string info_payload = info_request.dump();
    std::span<const char> info_span(info_payload.data(), info_payload.size());

    auto [info_reply, info_status] = co_await conn->request(
        info_subject, info_span, timeout);

    bool stream_exists = false;
    nlohmann::json stream_config;

    log->debug("Stream info status: {}, payload size: {}",
              info_status.ok() ? "OK" : info_status.error(),
              info_reply.payload.size());

    if (info_status.ok() && !info_reply.payload.empty()) {
        try {
            auto info_response = nlohmann::json::parse(
                std::string(info_reply.payload.data(), info_reply.payload.size()));

            log->debug("Stream info response: {}", info_response.dump());

            if (info_response.contains("config")) {
                stream_exists = true;
                stream_config = info_response["config"];
                log->info("Stream '{}' exists with config", stream_name);

                // Check if subject is already in the stream's subjects list
                if (stream_config.contains("subjects")) {
                    auto subjects = stream_config["subjects"].get<std::vector<std::string>>();
                    for (const auto& s : subjects) {
                        if (s == subject || s == (subject + ".>")) {
                            log->info("Stream '{}' already includes subject '{}'", stream_name, subject);
                            co_return true;
                        }
                    }
                    // Subject not found, add it
                    subjects.push_back(subject);
                    stream_config["subjects"] = subjects;
                    log->info("Adding subject '{}' to existing stream '{}'", subject, stream_name);
                } else {
                    // No subjects field, add it
                    stream_config["subjects"] = std::vector<std::string>{subject};
                }
            }
        } catch (const std::exception& e) {
            log->warn("Failed to parse stream info response: {}", e.what());
        }
    }

    // Create or update stream
    std::string create_subject = stream_exists
        ? "$JS.API.STREAM.UPDATE." + stream_name
        : "$JS.API.STREAM.CREATE." + stream_name;

    if (!stream_exists) {
        // Default stream configuration
        stream_config = {
            {"name", stream_name},
            {"subjects", {subject}},
            {"retention", "limits"},
            {"storage", "file"},
            {"max_msgs", -1},
            {"max_bytes", -1},
            {"max_age", 0},
            {"max_msg_size", -1},
            {"discard", "old"},
            {"num_replicas", 1}
        };
        log->info("Creating new stream '{}' for subject '{}'", stream_name, subject);
    }

    std::string create_payload = stream_config.dump();
    std::span<const char> create_span(create_payload.data(), create_payload.size());

    auto [create_reply, create_status] = co_await conn->request(
        create_subject, create_span, timeout);

    if (create_status.failed()) {
        log->error("Failed to {} stream '{}': {}",
                  stream_exists ? "update" : "create",
                  stream_name, create_status.error());
        co_return false;
    }

    if (create_reply.payload.empty()) {
        log->error("No reply received for stream {} request",
                  stream_exists ? "update" : "create");
        co_return false;
    }

    try {
        auto response = nlohmann::json::parse(
            std::string(create_reply.payload.data(), create_reply.payload.size()));

        if (response.contains("error")) {
            std::string desc = response["error"].contains("description")
                ? response["error"]["description"].get<std::string>()
                : "unknown";
            int code = response["error"].contains("code")
                ? response["error"]["code"].get<int>()
                : 0;
            log->error("Stream {} error: {} (code: {})",
                      stream_exists ? "update" : "creation", desc, code);
            co_return false;
        }

        log->info("Successfully {} stream '{}' for subject '{}'",
                 stream_exists ? "updated" : "created",
                 stream_name, subject);
        co_return true;

    } catch (const std::exception& e) {
        log->error("Failed to parse stream {} response: {}",
                  stream_exists ? "update" : "creation", e.what());
        co_return false;
    }
}

} // namespace nats_tool
