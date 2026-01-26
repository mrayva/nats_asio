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

#include <inja/inja.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <string_view>
#include <stringzilla/stringzilla.hpp>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace nats_tool {

// Helper to split string by delimiter using StringZilla (SIMD-accelerated)
inline std::vector<std::string> split_string(const std::string& s, char delim) {
    namespace sz = ashvardanian::stringzilla;
    std::vector<std::string> result;

    sz::string_view sz_str(s.data(), s.size());
    char delim_str[2] = {delim, '\0'};
    sz::string_view sz_delim(delim_str, 1);

    // Use StringZilla's SIMD-accelerated split - returns iterable range
    for (auto part : sz_str.split(sz_delim)) {
        // Trim whitespace
        while (!part.empty() && (part.front() == ' ' || part.front() == '\t')) {
            part = part.substr(1);
        }
        while (!part.empty() && (part.back() == ' ' || part.back() == '\t')) {
            part = part.substr(0, part.size() - 1);
        }
        if (!part.empty()) {
            result.emplace_back(part.data(), part.size());
        }
    }

    return result;
}

// Apply template substitution: replace {{field}} with values from JSON object
// Uses inja template engine for Jinja2-style templating
inline std::string apply_template(const std::string& tpl, const nlohmann::json& obj) {
    try {
        return inja::render(tpl, obj);
    } catch (const std::exception&) {
        // On parse error, return template unchanged
        return tpl;
    }
}

// Build payload from selected fields
inline std::string build_payload(const nlohmann::json& obj, const std::vector<std::string>& fields) {
    if (fields.empty()) {
        return obj.dump();
    }

    nlohmann::json result;
    for (const auto& field : fields) {
        if (obj.contains(field)) {
            result[field] = obj[field];
        }
    }
    return result.dump();
}

// Parse CSV line into JSON object using headers (RFC 4180 compliant)
// Handles: quoted fields, commas in quotes, escaped quotes (doubled)
inline nlohmann::json parse_csv_line(const std::string& line, const std::vector<std::string>& headers) {
    nlohmann::json obj;
    std::vector<std::string> values;
    std::string field;
    bool in_quotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];

        if (in_quotes) {
            if (c == '"') {
                // Check for escaped quote (doubled quote)
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field += '"';
                    ++i;  // Skip next quote
                } else {
                    in_quotes = false;
                }
            } else {
                field += c;
            }
        } else {
            if (c == '"') {
                in_quotes = true;
            } else if (c == ',') {
                // Trim whitespace from unquoted fields
                while (!field.empty() && (field.front() == ' ' || field.front() == '\t')) field.erase(0, 1);
                while (!field.empty() && (field.back() == ' ' || field.back() == '\t')) field.pop_back();
                values.push_back(std::move(field));
                field.clear();
            } else {
                field += c;
            }
        }
    }

    // Don't forget the last field
    while (!field.empty() && (field.front() == ' ' || field.front() == '\t')) field.erase(0, 1);
    while (!field.empty() && (field.back() == ' ' || field.back() == '\t')) field.pop_back();
    values.push_back(std::move(field));

    for (size_t i = 0; i < headers.size() && i < values.size(); i++) {
        obj[headers[i]] = values[i];
    }

    return obj;
}

// Translate payload through external command
// Supports {{Subject}} placeholder in command string
inline std::string translate_payload(const std::string& cmd, std::string_view subject,
                              std::span<const char> payload,
                              const std::shared_ptr<spdlog::logger>& log) {
    // Replace {{Subject}} placeholder with actual subject
    std::string actual_cmd = cmd;
    const std::string placeholder = "{{Subject}}";
    std::size_t pos = 0;
    while ((pos = actual_cmd.find(placeholder, pos)) != std::string::npos) {
        actual_cmd.replace(pos, placeholder.length(), subject);
        pos += subject.length();
    }

    // Create pipes for stdin/stdout
    int stdin_pipe[2];
    int stdout_pipe[2];

    if (pipe(stdin_pipe) < 0 || pipe(stdout_pipe) < 0) {
        log->error("translate: failed to create pipes");
        return std::string(payload.data(), payload.size());
    }

    pid_t pid = fork();
    if (pid < 0) {
        log->error("translate: fork failed");
        close(stdin_pipe[0]); close(stdin_pipe[1]);
        close(stdout_pipe[0]); close(stdout_pipe[1]);
        return std::string(payload.data(), payload.size());
    }

    if (pid == 0) {
        // Child process
        close(stdin_pipe[1]);   // Close write end of stdin pipe
        close(stdout_pipe[0]);  // Close read end of stdout pipe

        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);

        close(stdin_pipe[0]);
        close(stdout_pipe[1]);

        // Execute command via shell
        execl("/bin/sh", "sh", "-c", actual_cmd.c_str(), nullptr);
        _exit(127);  // exec failed
    }

    // Parent process
    close(stdin_pipe[0]);   // Close read end of stdin pipe
    close(stdout_pipe[1]);  // Close write end of stdout pipe

    // Write payload to child's stdin
    ssize_t written = write(stdin_pipe[1], payload.data(), payload.size());
    close(stdin_pipe[1]);  // Signal EOF to child

    if (written < 0) {
        log->error("translate: write to child failed");
    }

    // Read output from child's stdout
    std::string result;
    char buf[4096];
    ssize_t n;
    while ((n = read(stdout_pipe[0], buf, sizeof(buf))) > 0) {
        result.append(buf, static_cast<std::size_t>(n));
    }
    close(stdout_pipe[0]);

    // Wait for child to finish
    int status;
    waitpid(pid, &status, 0);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        log->warn("translate: command exited with status {}", WEXITSTATUS(status));
    }

    return result;
}

} // namespace nats_tool
