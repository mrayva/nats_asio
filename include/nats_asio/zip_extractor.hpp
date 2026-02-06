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

#include <spdlog/spdlog.h>
#include <string>
#include <vector>
#include <filesystem>
#include <zip.h>
#include <fstream>
#include <memory>

namespace nats_asio {

// ============================================================================
// ZIP archive extraction utilities
// ============================================================================

// Check if file is a zip archive by extension
inline bool is_zip_file(const std::string& path) {
    if (path.size() > 4 && path.substr(path.size() - 4) == ".zip") {
        return true;
    }
    return false;
}

// Check if file is a zip archive by magic bytes
inline bool is_zip_file_magic(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return false;

    char magic[4];
    file.read(magic, 4);
    if (file.gcount() != 4) return false;

    // ZIP magic: PK\x03\x04
    return (static_cast<unsigned char>(magic[0]) == 0x50 &&
            static_cast<unsigned char>(magic[1]) == 0x4B &&
            static_cast<unsigned char>(magic[2]) == 0x03 &&
            static_cast<unsigned char>(magic[3]) == 0x04);
}

// Extract all files from a zip archive to a temporary directory
// Returns: vector of extracted file paths
inline std::vector<std::string> extract_zip_to_temp(
    const std::string& zip_path,
    std::shared_ptr<spdlog::logger> log) {

    std::vector<std::string> extracted_files;

    // Open zip archive
    int err = 0;
    zip_t* archive = zip_open(zip_path.c_str(), ZIP_RDONLY, &err);
    if (!archive) {
        zip_error_t zerr;
        zip_error_init_with_code(&zerr, err);
        log->error("Failed to open zip archive {}: {}", zip_path, zip_error_strerror(&zerr));
        zip_error_fini(&zerr);
        return extracted_files;
    }

    // Create temporary directory for extraction
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path() /
        ("nats_tool_zip_" + std::to_string(std::hash<std::string>{}(zip_path)));

    try {
        std::filesystem::create_directories(temp_dir);
    } catch (const std::exception& e) {
        log->error("Failed to create temp directory {}: {}", temp_dir.string(), e.what());
        zip_close(archive);
        return extracted_files;
    }

    // Get number of files in archive
    zip_int64_t num_entries = zip_get_num_entries(archive, 0);
    log->info("Extracting {} file(s) from zip archive: {}", num_entries, zip_path);

    // Extract each file
    for (zip_int64_t i = 0; i < num_entries; i++) {
        const char* name = zip_get_name(archive, i, 0);
        if (!name) {
            log->warn("Failed to get name for entry {} in {}", i, zip_path);
            continue;
        }

        // Skip directories
        if (name[strlen(name) - 1] == '/') {
            continue;
        }

        // Open file in archive
        zip_file_t* zf = zip_fopen_index(archive, i, 0);
        if (!zf) {
            log->warn("Failed to open entry {} ({}) in {}", i, name, zip_path);
            continue;
        }

        // Create output file path (flatten directory structure)
        std::string filename = std::filesystem::path(name).filename().string();
        if (filename.empty()) {
            // Use the full path as filename if no filename component
            filename = std::string(name);
            std::replace(filename.begin(), filename.end(), '/', '_');
            std::replace(filename.begin(), filename.end(), '\\', '_');
        }

        std::filesystem::path output_path = temp_dir / filename;

        // Read and write file
        std::ofstream out(output_path, std::ios::binary);
        if (!out) {
            log->warn("Failed to create output file: {}", output_path.string());
            zip_fclose(zf);
            continue;
        }

        char buffer[8192];
        zip_int64_t bytes_read;
        while ((bytes_read = zip_fread(zf, buffer, sizeof(buffer))) > 0) {
            out.write(buffer, bytes_read);
        }

        zip_fclose(zf);
        out.close();

        if (bytes_read < 0) {
            log->warn("Error reading entry {} ({}) from {}", i, name, zip_path);
            continue;
        }

        log->debug("Extracted: {} -> {}", name, output_path.string());
        extracted_files.push_back(output_path.string());
    }

    zip_close(archive);

    log->info("Successfully extracted {} file(s) from {}", extracted_files.size(), zip_path);
    return extracted_files;
}

// RAII wrapper for automatic cleanup of extracted temp files
class zip_temp_cleanup {
public:
    zip_temp_cleanup(const std::string& zip_path, std::shared_ptr<spdlog::logger> log)
        : m_log(std::move(log)) {
        m_temp_dir = std::filesystem::temp_directory_path() /
            ("nats_tool_zip_" + std::to_string(std::hash<std::string>{}(zip_path)));
    }

    ~zip_temp_cleanup() {
        if (std::filesystem::exists(m_temp_dir)) {
            try {
                std::filesystem::remove_all(m_temp_dir);
                m_log->debug("Cleaned up temp directory: {}", m_temp_dir.string());
            } catch (const std::exception& e) {
                m_log->warn("Failed to clean up temp directory {}: {}",
                           m_temp_dir.string(), e.what());
            }
        }
    }

    // Non-copyable, non-movable
    zip_temp_cleanup(const zip_temp_cleanup&) = delete;
    zip_temp_cleanup& operator=(const zip_temp_cleanup&) = delete;
    zip_temp_cleanup(zip_temp_cleanup&&) = delete;
    zip_temp_cleanup& operator=(zip_temp_cleanup&&) = delete;

private:
    std::filesystem::path m_temp_dir;
    std::shared_ptr<spdlog::logger> m_log;
};

} // namespace nats_asio
