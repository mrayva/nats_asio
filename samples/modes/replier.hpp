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

#include "../include/worker.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <memory>
#include <unistd.h>

namespace nats_tool {

class replier : public worker {
public:
    replier(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console,
            nats_asio::iconnection_sptr conn, const std::string& topic,
            int stats_interval, const std::string& data, bool echo_mode,
            const std::string& translate_cmd, const std::string& queue_group,
            output_mode mode)
        : worker(ioc, console, stats_interval), m_conn(std::move(conn)),
          m_topic(topic), m_data(data), m_echo_mode(echo_mode),
          m_translate_cmd(translate_cmd), m_queue_group(queue_group),
          m_output_mode(mode) {}

    asio::awaitable<void> start() {
        // Wait for connection
        while (!m_conn->is_connected()) {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        nats_asio::subscribe_options sub_opts;
        if (!m_queue_group.empty()) {
            sub_opts.queue_group = m_queue_group;
        }

        auto [sub, status] = co_await m_conn->subscribe(
            m_topic,
            [this](nats_asio::string_view subject, nats_asio::optional<nats_asio::string_view> reply_to,
                   std::span<const char> payload) -> asio::awaitable<void> {
                nats_asio::message msg;
                msg.subject = std::string(subject);
                if (reply_to) {
                    msg.reply_to = std::string(*reply_to);
                }
                msg.payload.assign(payload.begin(), payload.end());
                co_await handle_message(msg);
            },
            sub_opts);

        if (status.failed()) {
            m_log->error("subscribe failed: {}", status.error());
            m_ioc.stop();
            co_return;
        }

        m_log->info("Replier listening on '{}'{}", m_topic,
                   m_queue_group.empty() ? "" : " (queue: " + m_queue_group + ")");
        co_return;
    }

private:
    asio::awaitable<void> handle_message(const nats_asio::message& msg) {
        m_counter++;

        // Print received message based on output mode
        switch (m_output_mode) {
            case output_mode::raw:
                std::cout.write(msg.payload.data(), msg.payload.size());
                std::cout << std::endl;
                break;
            case output_mode::json: {
                nlohmann::json j;
                j["subject"] = msg.subject;
                if (msg.reply_to) j["reply_to"] = *msg.reply_to;
                j["payload"] = std::string(msg.payload.begin(), msg.payload.end());
                std::cout << j.dump() << std::endl;
                break;
            }
            case output_mode::normal:
                std::cout << "[" << msg.subject << "] ";
                std::cout.write(msg.payload.data(), msg.payload.size());
                std::cout << std::endl;
                break;
            case output_mode::none:
                break;
        }

        // Only reply if there's a reply_to subject
        if (!msg.reply_to || msg.reply_to->empty()) {
            co_return;
        }

        std::string reply_payload;

        if (m_echo_mode) {
            // Echo mode - reply with received payload
            reply_payload = std::string(msg.payload.begin(), msg.payload.end());
        } else if (!m_translate_cmd.empty()) {
            // Transform through external command (runs on thread pool to avoid blocking)
            std::string cmd = m_translate_cmd;
            // Replace {{Subject}} placeholder if present
            std::string subject_placeholder = "{{Subject}}";
            size_t pos = cmd.find(subject_placeholder);
            if (pos != std::string::npos) {
                cmd.replace(pos, subject_placeholder.length(), msg.subject);
            }

            // Copy payload for thread-safe access
            std::vector<char> payload_copy(msg.payload.begin(), msg.payload.end());

            // Run blocking subprocess on background thread
            reply_payload = co_await async_run_blocking([this, cmd, payload_copy]() {
                return run_translate_command(cmd, std::span<const char>(payload_copy));
            });

            if (reply_payload.empty()) {
                m_log->warn("translate command returned empty output");
            }
        } else if (!m_data.empty()) {
            // Fixed reply data
            reply_payload = m_data;
        } else {
            // Default empty reply
            reply_payload = "";
        }

        std::span<const char> reply_span(reply_payload.data(), reply_payload.size());
        auto status = co_await m_conn->publish(*msg.reply_to, reply_span, std::nullopt);

        if (status.failed()) {
            m_log->error("reply failed: {}", status.error());
        }

        co_return;
    }

    std::string run_translate_command(const std::string& cmd, std::span<const char> input) {
        int pipe_in[2], pipe_out[2];
        if (pipe(pipe_in) < 0 || pipe(pipe_out) < 0) {
            m_log->error("pipe creation failed");
            return "";
        }

        pid_t pid = fork();
        if (pid < 0) {
            m_log->error("fork failed");
            close(pipe_in[0]); close(pipe_in[1]);
            close(pipe_out[0]); close(pipe_out[1]);
            return "";
        }

        if (pid == 0) {
            // Child process
            close(pipe_in[1]);
            close(pipe_out[0]);
            dup2(pipe_in[0], STDIN_FILENO);
            dup2(pipe_out[1], STDOUT_FILENO);
            close(pipe_in[0]);
            close(pipe_out[1]);
            execl("/bin/sh", "sh", "-c", cmd.c_str(), nullptr);
            _exit(1);
        }

        // Parent process
        close(pipe_in[0]);
        close(pipe_out[1]);

        // Write input to child
        if (!input.empty()) {
            ssize_t written = ::write(pipe_in[1], input.data(), input.size());
            if (written < 0 || static_cast<size_t>(written) != input.size()) {
                m_log->warn("translate command: incomplete write to stdin");
            }
        }
        close(pipe_in[1]);

        // Read output from child
        std::string result;
        char buf[4096];
        ssize_t n;
        while ((n = ::read(pipe_out[0], buf, sizeof(buf))) > 0) {
            result.append(buf, n);
        }
        close(pipe_out[0]);

        // Wait for child
        int status;
        waitpid(pid, &status, 0);

        // Trim trailing newline
        while (!result.empty() && (result.back() == '\n' || result.back() == '\r')) {
            result.pop_back();
        }

        return result;
    }

    nats_asio::iconnection_sptr m_conn;
    std::string m_topic;
    std::string m_data;
    bool m_echo_mode;
    std::string m_translate_cmd;
    std::string m_queue_group;
    output_mode m_output_mode;
};

} // namespace nats_tool
