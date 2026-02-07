// mimalloc: drop-in malloc replacement - must be included first
#include <mimalloc-new-delete.h>

#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <inja/inja.hpp>
#include <numeric>
#include <sstream>
#include <asio/as_tuple.hpp>
#include <asio/detached.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl.hpp>
#include <asio/ip/tcp.hpp>
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#include <fstream>
#include <future>
#include <iostream>
#include <nats_asio/nats_asio.hpp>
#include <nats_asio/compression.hpp>
#include <nats_asio/http_reader.hpp>
#include <nats_asio/input_reader.hpp>
#include <nats_asio/multi_file_reader.hpp>
#include <simdjson.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <poll.h>
#include <zstd.h>

#include "cxxopts.hpp"
#include "include/string_utils.hpp"
#include "include/fast_json_parser.hpp"
#include "include/worker.hpp"
#include "include/batch_publisher.hpp"
#include "include/js_sliding_window.hpp"
#include "include/js_stream_utils.hpp"
#include "include/zerialize_json.hpp"

using nats_asio::zstd_compressor;
using nats_asio::input_source_config;
using nats_asio::async_http_reader;
using nats_asio::async_input_reader;
using nats_asio::async_multi_file_reader;

using nats_tool::output_mode;
using nats_tool::input_format;
using nats_tool::input_config;
using nats_tool::worker;
using nats_tool::fast_json_parser;
using nats_tool::split_string;
using nats_tool::apply_template;
using nats_tool::build_payload;
using nats_tool::parse_csv_line;
using nats_tool::translate_payload;
using nats_tool::batch_item;
using nats_tool::batch_queue;
using nats_tool::batch_publisher;

const std::string grub_mode("grub");
const std::string gen_mode("gen");
const std::string pub_mode("pub");
const std::string req_mode("req");
const std::string reply_mode("reply");
const std::string bench_mode("bench");
const std::string js_grub_mode("js_grub");
const std::string js_fetch_mode("js_fetch");
const std::string pubkv_mode("pubkv");
const std::string kvwatch_mode("kvwatch");
const std::string kvcreate_mode("kvcreate");
const std::string kvupdate_mode("kvupdate");
const std::string kvkeys_mode("kvkeys");
const std::string kvhistory_mode("kvhistory");
const std::string kvpurge_mode("kvpurge");
const std::string kvrevert_mode("kvrevert");

enum class mode { grubber, generator, publisher, requester, replier, benchmarker, js_grubber, js_fetcher, kv_publisher, kv_watcher, kv_creator, kv_updater, kv_keys_lister, kv_history_viewer, kv_purger, kv_reverter };


// Mode class headers
#include "modes/common.hpp"
#include "modes/generator.hpp"
#include "modes/grubber.hpp"
#include "modes/js_grubber.hpp"
#include "modes/js_fetcher.hpp"
#include "modes/kv_publisher.hpp"
#include "modes/kv_watcher_handler.hpp"
#include "modes/publisher.hpp"
#include "modes/requester.hpp"
#include "modes/replier.hpp"
#include "modes/benchmarker.hpp"

using nats_tool::generator;
using nats_tool::grubber;
using nats_tool::js_grubber;
using nats_tool::js_fetcher;
using nats_tool::kv_publisher;
using nats_tool::kv_watcher_handler;
using nats_tool::publisher;
using nats_tool::requester;
using nats_tool::replier;
using nats_tool::benchmarker;

std::string read_file(const std::shared_ptr<spdlog::logger>& console, const std::string& path) {
    try {
        if (path.empty()) {
            return {};
        }

        std::ifstream t(path);
        std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
        return str;
    } catch (const std::exception& e) {
        console->error("failed to read file {}, with error: {}", path, e.what());
    }

    return {};
}

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options options(argv[0], " - filters command line options");
        nats_asio::connect_config conf;
        nats_asio::ssl_config ssl_conf;
        ssl_conf.verify = true;
        conf.address = "127.0.0.1";
        conf.port = 4222;
        std::string username;
        std::string password;
        std::string mode;
        std::string topic;
        int stats_interval = 1;
        int publish_interval = -1;
        int num_connections = 1;
        int max_in_flight = 1000;
        bool print_to_stdout = false;
        std::string ssl_key_file;
        std::string ssl_cert_file;
        std::string ssl_ca_file;
        /* clang-format off */
        options.add_options()
        ("h,help", "Print help")
        ("d,debug", "Enable debugging")
        ("address", "Address of NATS server", cxxopts::value<std::string>(conf.address))
        ("port", "Port of NATS server", cxxopts::value<uint16_t >(conf.port))
        ("user", "Username", cxxopts::value<std::string >(username))
        ("pass", "Password", cxxopts::value<std::string >(password))
        ("mode", "mode", cxxopts::value<std::string >(mode))
        ("topic", "topic", cxxopts::value<std::string >(topic))
        ("stats_interval", "stat interval seconds", cxxopts::value<int>(stats_interval))
        ("publish_interval", "publish interval seconds in ms", cxxopts::value<int>(publish_interval))
        ("n,connections", "Number of connections for pub mode (default: 1)", cxxopts::value<int>(num_connections))
        ("threads", "Number of threads to run io_context (default: 1, use 0 for hardware_concurrency)", cxxopts::value<int>())
        ("max_in_flight", "Max in-flight publishes for pub mode (default: 1000)", cxxopts::value<int>(max_in_flight))
        ("batch_pub", "Use high-performance batch publishing (pub mode)")
        ("batch_size", "Batch read size in bytes for batch_pub mode (default: 65536)", cxxopts::value<int>())
        ("max_queue", "Max batches in queue for batch_pub mode (default: 100)", cxxopts::value<int>())
        ("flush_timeout", "Flush timeout in ms for batch_pub mode - flush partial batch if no data arrives within timeout (default: 0 = disabled)", cxxopts::value<int>())
        ("js,jetstream", "Use JetStream publish (pub mode only)")
        ("no_ack", "Fire-and-forget JetStream publish, don't wait for ack (with --js)")
        ("js_timeout", "JetStream publish timeout in ms (default: 5000)", cxxopts::value<int>())
        ("js_window", "JetStream ACK sliding window size (default: 1000)", cxxopts::value<int>())
        ("js_max_retries", "Max retries for timed-out JetStream publishes (default: 3)", cxxopts::value<int>())
        ("stream", "JetStream stream name (pub/js_grub/js_fetch mode)", cxxopts::value<std::string>())
        ("create_stream", "Auto-create/update JetStream stream to include subject (pub mode with --js)")
        ("consumer", "JetStream consumer name (js_grub/js_fetch mode)", cxxopts::value<std::string>())
        ("durable", "Durable consumer name (js_grub mode)", cxxopts::value<std::string>())
        ("batch", "Batch size for js_fetch mode (default: 10)", cxxopts::value<int>())
        ("fetch_interval", "Interval between fetches in ms (default: 100)", cxxopts::value<int>())
        ("auto_ack", "Auto-acknowledge messages (js_grub mode)", cxxopts::value<bool>())
        ("bucket", "KV bucket name (pubkv/kvwatch/kvcreate/kvupdate mode)", cxxopts::value<std::string>())
        ("key", "KV key (required for kvcreate/kvupdate, optional filter for kvwatch)", cxxopts::value<std::string>())
        ("value", "KV value for kvcreate/kvupdate mode", cxxopts::value<std::string>())
        ("revision", "Expected revision for kvupdate mode", cxxopts::value<uint64_t>())
        ("separator", "Key-value separator for pubkv mode (default: |)", cxxopts::value<std::string>())
        ("kv_timeout", "KV operation timeout in ms (default: 5000)", cxxopts::value<int>())
        ("print", "print messages to stdout", cxxopts::value<bool>(print_to_stdout))
        ("queue", "Queue group for subscribe (grub/js_grub mode)", cxxopts::value<std::string>())
        ("max_msgs", "Auto-unsubscribe after N messages (grub mode)", cxxopts::value<uint32_t>())
        ("raw", "Output raw payload only (grub/js_grub mode)")
        ("dump", "Dump messages to file (grub/js_grub mode)", cxxopts::value<std::string>())
        ("json", "Output messages as JSON (grub/js_grub mode)")
        ("format", "Binary format: msgpack, cbor, flexbuffers, zera (grub/js_grub/js_fetch/pub mode)", cxxopts::value<std::string>())
        ("max_bad_messages", "Exit after N failed deserializations (default: 0 = disabled)", cxxopts::value<std::size_t>())
        ("max_bad_percentage", "Exit if bad message percentage exceeds threshold (default: 0 = disabled)", cxxopts::value<double>())
        ("translate", "Transform payload through external command (supports {{Subject}})", cxxopts::value<std::string>())
        ("data", "Payload data for req/reply mode (if not provided, reads from stdin)", cxxopts::value<std::string>())
        ("timeout", "Request timeout in ms for req mode (default: 5000)", cxxopts::value<int>())
        ("echo", "Echo mode for reply - reply with received payload")
        ("H,header", "Add header to message (format: Key:Value, repeatable)", cxxopts::value<std::vector<std::string>>())
        ("reply_to", "Custom reply-to subject for pub mode", cxxopts::value<std::string>())
        ("t,timestamp", "Show timestamps in subscribe output")
        ("count", "Number of messages to publish (pub mode, default: 1)", cxxopts::value<int>())
        ("sleep", "Sleep interval between publishes in ms (pub mode)", cxxopts::value<int>())
        ("pub_size", "Message size in bytes for bench mode (default: 128)", cxxopts::value<int>())
        ("bench_rtt", "Bench mode: measure round-trip latency using request-reply")
        ("bench_batch", "Bench mode: messages per batch for pipelined publishing (default: 1000)", cxxopts::value<int>())
        ("input_format", "Input format: line (default), json, csv", cxxopts::value<std::string>())
        ("subject_template", "Subject template with {{field}} placeholders (requires json/csv input)", cxxopts::value<std::string>())
        ("payload_fields", "Comma-separated fields to include in payload (default: all)", cxxopts::value<std::string>())
        ("csv_headers", "Comma-separated header names for CSV input", cxxopts::value<std::string>())
        ("compress", "Compress payloads with zstd (pub/batch_pub mode)")
        ("decompress", "Decompress zstd payloads (grub mode)")
        ("compress_level", "Compression level 1-22 (default: 3)", cxxopts::value<int>())
        ("file", "Read input from file(s)/glob pattern (repeatable, supports wildcards)", cxxopts::value<std::vector<std::string>>())
        ("follow,f", "Follow mode - continuously read new data (like tail -f)")
        ("poll_interval", "Poll interval in ms for follow mode (default: 100)", cxxopts::value<int>())
        ("http", "HTTP URL to read streaming data from (e.g., Feldera output)", cxxopts::value<std::string>())
        ("http_method", "HTTP method: GET or POST (default: POST)", cxxopts::value<std::string>())
        ("http_body", "HTTP request body for POST method", cxxopts::value<std::string>())
        ("http_header", "HTTP header (format: Key:Value, repeatable)", cxxopts::value<std::vector<std::string>>())
        ("http_insecure", "Skip SSL certificate verification for HTTPS")
        ("ssl", "Enable ssl")
        ("ssl_key", "ssl_key", cxxopts::value<std::string>(ssl_key_file))
        ("ssl_cert", "ssl_cert", cxxopts::value<std::string>(ssl_cert_file))
        ("ssl_ca", "ssl_ca", cxxopts::value<std::string>(ssl_ca_file))
        ;
        /* clang-format on */
        options.parse_positional({"mode"});
        auto result = options.parse(argc, argv);

        if (result.count("help")) {
            std::cout << options.help() << std::endl;
            return 0;
        }

        auto console = spdlog::stdout_color_mt("console");

        if (result.count("debug")) {
            console->set_level(spdlog::level::debug);
        }

        if (result.count("mode") == 0) {
            console->error("Please specify mode");
            return 1;
        }

        std::optional<nats_asio::ssl_config> opt_ssl_conf;
        if (result.count("ssl")) {
            ssl_conf.cert = read_file(console, ssl_cert_file);
            ssl_conf.ca = read_file(console, ssl_ca_file);
            ssl_conf.key = read_file(console, ssl_key_file);
            opt_ssl_conf = ssl_conf;
        } else {
            opt_ssl_conf = std::nullopt;
        }

        mode = result["mode"].as<std::string>();

        // Only require topic for modes that need it
        if (topic.empty() && result.count("topic")) {
            topic = result["topic"].as<std::string>();
        }

        if (mode != grub_mode && mode != gen_mode && mode != pub_mode &&
            mode != req_mode && mode != reply_mode && mode != bench_mode &&
            mode != js_grub_mode && mode != js_fetch_mode && mode != pubkv_mode &&
            mode != kvwatch_mode && mode != kvcreate_mode && mode != kvupdate_mode &&
            mode != kvkeys_mode && mode != kvhistory_mode && mode != kvpurge_mode &&
            mode != kvrevert_mode) {
            console->error("Invalid mode. Use --help to see available modes");
            return 1;
        }

        auto m = mode::generator;

        if (mode == grub_mode) {
            m = mode::grubber;
            publish_interval = -1;
        } else if (mode == pub_mode) {
            m = mode::publisher;
            if (num_connections < 1) {
                num_connections = 1;
            }
        } else if (mode == req_mode) {
            m = mode::requester;
            publish_interval = -1;
        } else if (mode == reply_mode) {
            m = mode::replier;
            publish_interval = -1;
        } else if (mode == bench_mode) {
            m = mode::benchmarker;
            publish_interval = -1;
        } else if (mode == js_grub_mode) {
            m = mode::js_grubber;
            publish_interval = -1;
        } else if (mode == js_fetch_mode) {
            m = mode::js_fetcher;
            publish_interval = -1;
        } else if (mode == pubkv_mode) {
            m = mode::kv_publisher;
            publish_interval = -1;
        } else if (mode == kvwatch_mode) {
            m = mode::kv_watcher;
            publish_interval = -1;
        } else if (mode == kvcreate_mode) {
            m = mode::kv_creator;
            publish_interval = -1;
        } else if (mode == kvupdate_mode) {
            m = mode::kv_updater;
            publish_interval = -1;
        } else if (mode == kvkeys_mode) {
            m = mode::kv_keys_lister;
            publish_interval = -1;
        } else if (mode == kvhistory_mode) {
            m = mode::kv_history_viewer;
            publish_interval = -1;
        } else if (mode == kvpurge_mode) {
            m = mode::kv_purger;
            publish_interval = -1;
        } else if (mode == kvrevert_mode) {
            m = mode::kv_reverter;
            publish_interval = -1;
        } else {
            if (publish_interval < 0) {
                publish_interval = 1000;
            }
        }

        // Subscribe options for grub mode
        std::string queue_group;
        uint32_t max_msgs = 0;
        output_mode out_mode = print_to_stdout ? output_mode::normal : output_mode::none;
        std::string dump_file;

        if (result.count("queue")) {
            queue_group = result["queue"].as<std::string>();
        }
        if (result.count("max_msgs")) {
            max_msgs = result["max_msgs"].as<uint32_t>();
        }
        if (result.count("raw")) {
            out_mode = output_mode::raw;
        }
        if (result.count("json")) {
            out_mode = output_mode::json;
        }
        if (result.count("dump")) {
            dump_file = result["dump"].as<std::string>();
            // If dump is specified but no output mode, default to normal
            if (out_mode == output_mode::none) {
                out_mode = output_mode::normal;
            }
        }

        std::string translate_cmd;
        if (result.count("translate")) {
            translate_cmd = result["translate"].as<std::string>();
            // If translate is specified but no output mode, default to raw
            if (out_mode == output_mode::none) {
                out_mode = output_mode::raw;
            }
        }

        // Validate JetStream mode requirements
        std::string js_stream, js_consumer, js_durable;
        int batch_size = 10;
        int fetch_interval_ms = 100;
        bool auto_ack = result.count("auto_ack") > 0;

        if (m == mode::js_grubber || m == mode::js_fetcher) {
            if (!result.count("stream")) {
                console->error("JetStream mode requires --stream");
                return 1;
            }
            js_stream = result["stream"].as<std::string>();

            if (m == mode::js_fetcher) {
                if (!result.count("consumer")) {
                    console->error("js_fetch mode requires --consumer");
                    return 1;
                }
                js_consumer = result["consumer"].as<std::string>();
            }

            if (result.count("durable")) {
                js_durable = result["durable"].as<std::string>();
            }
            if (result.count("batch")) {
                batch_size = result["batch"].as<int>();
            }
            if (result.count("fetch_interval")) {
                fetch_interval_ms = result["fetch_interval"].as<int>();
            }
        }

        // Validate KV mode requirements
        std::string kv_bucket;
        std::string kv_key;
        std::string kv_value;
        uint64_t kv_revision = 0;
        std::string kv_separator = "|";
        int kv_timeout_ms = 5000;

        if (m == mode::kv_publisher || m == mode::kv_watcher ||
            m == mode::kv_creator || m == mode::kv_updater || m == mode::kv_keys_lister ||
            m == mode::kv_history_viewer || m == mode::kv_purger || m == mode::kv_reverter) {
            if (!result.count("bucket")) {
                console->error("{} mode requires --bucket", mode);
                return 1;
            }
            kv_bucket = result["bucket"].as<std::string>();

            if (result.count("separator")) {
                kv_separator = result["separator"].as<std::string>();
            }
            if (result.count("kv_timeout")) {
                kv_timeout_ms = result["kv_timeout"].as<int>();
            }
            if (result.count("key")) {
                kv_key = result["key"].as<std::string>();
            }
            if (result.count("value")) {
                kv_value = result["value"].as<std::string>();
            }
            if (result.count("revision")) {
                kv_revision = result["revision"].as<uint64_t>();
            }

            // kvcreate and kvupdate require key and value
            if (m == mode::kv_creator || m == mode::kv_updater) {
                if (kv_key.empty()) {
                    console->error("{} mode requires --key", mode);
                    return 1;
                }
                if (kv_value.empty()) {
                    console->error("{} mode requires --value", mode);
                    return 1;
                }
            }

            // kvhistory, kvpurge, and kvrevert require key
            if (m == mode::kv_history_viewer || m == mode::kv_purger || m == mode::kv_reverter) {
                if (kv_key.empty()) {
                    console->error("{} mode requires --key", mode);
                    return 1;
                }
            }

            // kvrevert requires revision
            if (m == mode::kv_reverter) {
                if (kv_revision == 0) {
                    console->error("kvrevert mode requires --revision > 0");
                    return 1;
                }
            }

            // kvupdate requires revision
            if (m == mode::kv_updater) {
                if (kv_revision == 0) {
                    console->error("kvupdate mode requires --revision > 0");
                    return 1;
                }
            }
        }

        asio::io_context ioc;
        auto strand = asio::make_strand(ioc);  // Serialize operations for thread safety
        std::shared_ptr<grubber> grub_ptr;
        std::shared_ptr<generator> gen_ptr;
        std::shared_ptr<publisher> pub_ptr;
        std::shared_ptr<requester> req_ptr;
        std::shared_ptr<replier> reply_ptr;
        std::shared_ptr<benchmarker> bench_ptr;
        std::shared_ptr<batch_publisher> batch_pub_ptr;
        std::shared_ptr<js_grubber> js_grub_ptr;
        std::shared_ptr<js_fetcher> js_fetch_ptr;
        std::shared_ptr<kv_publisher> kv_pub_ptr;
        std::shared_ptr<kv_watcher_handler> kv_watch_ptr;
        nats_asio::iconnection_sptr conn;
        std::vector<nats_asio::iconnection_sptr> pub_connections;

        // Parse timestamp option
        bool show_timestamp = result.count("timestamp") > 0;

        // Parse headers (format: Key:Value)
        nats_asio::headers_t headers;
        if (result.count("header")) {
            auto header_strs = result["header"].as<std::vector<std::string>>();
            for (const auto& h : header_strs) {
                auto colon_pos = h.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = h.substr(0, colon_pos);
                    std::string value = h.substr(colon_pos + 1);
                    // Trim leading whitespace from value
                    while (!value.empty() && (value[0] == ' ' || value[0] == '\t')) {
                        value.erase(0, 1);
                    }
                    headers.emplace_back(key, value);
                } else {
                    console->warn("Invalid header format (expected Key:Value): {}", h);
                }
            }
        }

        // Parse reply_to, count, sleep options
        std::string custom_reply_to;
        int pub_count = 0;
        int pub_sleep_ms = 0;
        std::string pub_data;
        if (result.count("reply_to")) {
            custom_reply_to = result["reply_to"].as<std::string>();
        }
        if (result.count("count")) {
            pub_count = result["count"].as<int>();
        }
        if (result.count("sleep")) {
            pub_sleep_ms = result["sleep"].as<int>();
        }
        if (result.count("data")) {
            pub_data = result["data"].as<std::string>();
        }

        // Parse input format options for publisher
        input_config in_cfg;
        if (result.count("input_format")) {
            std::string fmt = result["input_format"].as<std::string>();
            if (fmt == "json") {
                in_cfg.format = input_format::json;
            } else if (fmt == "csv") {
                in_cfg.format = input_format::csv;
            } else if (fmt != "line") {
                console->warn("Unknown input format '{}', using 'line'", fmt);
            }
        }
        if (result.count("subject_template")) {
            in_cfg.subject_template = result["subject_template"].as<std::string>();
        }
        if (result.count("payload_fields")) {
            in_cfg.payload_fields = split_string(result["payload_fields"].as<std::string>(), ',');
        }
        if (result.count("csv_headers")) {
            in_cfg.csv_headers = split_string(result["csv_headers"].as<std::string>(), ',');
        }

        // Parse input source options (file vs stdin, follow mode, HTTP)
        input_source_config src_cfg;
        if (result.count("file")) {
            src_cfg.file_patterns = result["file"].as<std::vector<std::string>>();
            // For backward compatibility, also set file_path if single pattern
            if (src_cfg.file_patterns.size() == 1) {
                src_cfg.file_path = src_cfg.file_patterns[0];
            }
        }
        src_cfg.follow = result.count("follow") > 0;
        if (result.count("poll_interval")) {
            src_cfg.poll_interval_ms = result["poll_interval"].as<int>();
        }

        // HTTP source options
        if (result.count("http")) {
            src_cfg.http_url = result["http"].as<std::string>();
        }
        if (result.count("http_method")) {
            src_cfg.http_method = result["http_method"].as<std::string>();
        }
        if (result.count("http_body")) {
            src_cfg.http_body = result["http_body"].as<std::string>();
        }
        src_cfg.http_insecure = result.count("http_insecure") > 0;
        if (result.count("http_header")) {
            auto http_headers = result["http_header"].as<std::vector<std::string>>();
            for (const auto& h : http_headers) {
                auto colon_pos = h.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = h.substr(0, colon_pos);
                    std::string value = h.substr(colon_pos + 1);
                    // Trim leading whitespace from value
                    while (!value.empty() && (value[0] == ' ' || value[0] == '\t')) {
                        value.erase(0, 1);
                    }
                    src_cfg.http_headers.emplace_back(key, value);
                } else {
                    console->warn("Invalid HTTP header format (expected Key:Value): {}", h);
                }
            }
        }

        // Parse binary format options
        std::optional<nats_tool::binary_format> binary_fmt;
        std::size_t max_bad_messages = 0;
        double max_bad_percentage = 0.0;

        if (result.count("format")) {
            std::string fmt_str = result["format"].as<std::string>();
            binary_fmt = nats_tool::parse_format(fmt_str);
            if (!binary_fmt) {
                console->error("Invalid format '{}'. Valid formats: msgpack, cbor, flexbuffers, zera", fmt_str);
                return 1;
            }
        }

        if (result.count("max_bad_messages")) {
            max_bad_messages = result["max_bad_messages"].as<std::size_t>();
        }

        if (result.count("max_bad_percentage")) {
            max_bad_percentage = result["max_bad_percentage"].as<double>();
            if (max_bad_percentage < 0.0 || max_bad_percentage > 100.0) {
                console->error("max_bad_percentage must be between 0 and 100");
                return 1;
            }
        }

        if (m == mode::grubber) {
            grub_ptr = std::make_shared<grubber>(ioc, console, stats_interval, out_mode, dump_file, translate_cmd, show_timestamp, binary_fmt, max_bad_messages, max_bad_percentage);
        } else if (m == mode::js_grubber) {
            js_grub_ptr = std::make_shared<js_grubber>(ioc, console, stats_interval, out_mode, auto_ack, dump_file, translate_cmd, binary_fmt, max_bad_messages, max_bad_percentage);
        } else if (m == mode::kv_watcher) {
            kv_watch_ptr = std::make_shared<kv_watcher_handler>(ioc, console, stats_interval, print_to_stdout);
        }

        // Helper to create a connection
        auto make_connection = [&](int conn_id = -1) {
            return nats_asio::create_connection(
                ioc,
                [&, conn_id](nats_asio::iconnection& /*c*/) -> asio::awaitable<void> {
                    if (conn_id >= 0) {
                        console->info("connection {} connected", conn_id);
                    } else {
                        console->info("on connected");
                    }

                    if (m == mode::grubber) {
                        nats_asio::subscribe_options sub_opts;
                        if (!queue_group.empty()) {
                            sub_opts.queue_group = queue_group;
                        }
                        sub_opts.max_messages = max_msgs;

                        auto r = co_await conn->subscribe(
                            topic,
                            [grub_ptr](auto v1, auto v2, auto v3) -> asio::awaitable<void> {
                                return grub_ptr->on_message(v1, v2, v3);
                            },
                            sub_opts);

                        if (r.second.failed()) {
                            console->error("failed to subscribe with error: {}", r.second.error());
                        } else if (max_msgs > 0) {
                            console->info("subscribed to {} (will auto-unsubscribe after {} messages)", topic, max_msgs);
                        }
                    } else if (m == mode::js_grubber) {
                        // Setup JetStream push consumer
                        nats_asio::js_consumer_config config;
                        config.stream = js_stream;
                        config.filter_subject = topic.empty() ? std::nullopt : std::optional<std::string>(topic);
                        if (!js_durable.empty()) {
                            config.durable_name = js_durable;
                        }
                        config.ack = nats_asio::js_ack_policy::explicit_;

                        auto [sub, s] = co_await conn->js_subscribe(
                            config,
                            [js_grub_ptr](nats_asio::ijs_subscription& sub,
                                         const nats_asio::js_message& msg) -> asio::awaitable<void> {
                                return js_grub_ptr->on_js_message(sub, msg);
                            });

                        if (s.failed()) {
                            console->error("js_subscribe failed: {}", s.error());
                        } else {
                            console->info("JetStream subscription active: stream={} consumer={}",
                                        sub->info().stream, sub->info().name);
                        }
                    } else if (m == mode::kv_watcher) {
                        // Setup KV watcher
                        auto [watcher, s] = co_await conn->kv_watch(
                            kv_bucket,
                            [kv_watch_ptr](const nats_asio::kv_entry& entry) -> asio::awaitable<void> {
                                return kv_watch_ptr->on_kv_entry(entry);
                            },
                            kv_key);

                        if (s.failed()) {
                            console->error("kv_watch failed: {}", s.error());
                        } else {
                            if (kv_key.empty()) {
                                console->info("Watching KV bucket: {}", kv_bucket);
                            } else {
                                console->info("Watching KV bucket: {} key: {}", kv_bucket, kv_key);
                            }
                        }
                    } else if (m == mode::kv_creator) {
                        // Create key (only if doesn't exist)
                        std::span<const char> value_span(kv_value.data(), kv_value.size());
                        auto [rev, s] = co_await conn->kv_create(
                            kv_bucket, kv_key, value_span,
                            std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_create failed: {}", s.error());
                        } else {
                            console->info("Created {}/{} revision={}", kv_bucket, kv_key, rev);
                        }
                        ioc.stop();
                    } else if (m == mode::kv_updater) {
                        // Update key (only if revision matches)
                        std::span<const char> value_span(kv_value.data(), kv_value.size());
                        auto [rev, s] = co_await conn->kv_update(
                            kv_bucket, kv_key, value_span, kv_revision,
                            std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_update failed: {}", s.error());
                        } else {
                            console->info("Updated {}/{} revision={} (was {})", kv_bucket, kv_key, rev, kv_revision);
                        }
                        ioc.stop();
                    } else if (m == mode::kv_keys_lister) {
                        // List all keys in bucket
                        auto [keys, s] = co_await conn->kv_keys(
                            kv_bucket, std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_keys failed: {}", s.error());
                        } else {
                            console->info("Keys in bucket '{}' ({} keys):", kv_bucket, keys.size());
                            for (const auto& key : keys) {
                                std::cout << key << std::endl;
                            }
                        }
                        ioc.stop();
                    } else if (m == mode::kv_history_viewer) {
                        // Show full history for a key
                        auto [history, s] = co_await conn->kv_history(
                            kv_bucket, kv_key, std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_history failed: {}", s.error());
                        } else {
                            console->info("History for {}/{} ({} revisions):", kv_bucket, kv_key, history.size());
                            for (const auto& entry : history) {
                                std::string op_str;
                                switch (entry.op) {
                                    case nats_asio::kv_entry::operation::put: op_str = "PUT"; break;
                                    case nats_asio::kv_entry::operation::del: op_str = "DEL"; break;
                                    case nats_asio::kv_entry::operation::purge: op_str = "PURGE"; break;
                                }
                                std::cout << "rev=" << entry.revision << " [" << op_str << "]";
                                if (entry.op == nats_asio::kv_entry::operation::put && !entry.value.empty()) {
                                    std::cout << " value=";
                                    std::cout.write(entry.value.data(), entry.value.size());
                                }
                                std::cout << std::endl;
                            }
                        }
                        ioc.stop();
                    } else if (m == mode::kv_purger) {
                        // Purge key (delete and clear history)
                        auto [rev, s] = co_await conn->kv_purge(
                            kv_bucket, kv_key, std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_purge failed: {}", s.error());
                        } else {
                            console->info("Purged {}/{} revision={}", kv_bucket, kv_key, rev);
                        }
                        ioc.stop();
                    } else if (m == mode::kv_reverter) {
                        // Revert key to a previous revision
                        auto [rev, s] = co_await conn->kv_revert(
                            kv_bucket, kv_key, kv_revision,
                            std::chrono::milliseconds(kv_timeout_ms));

                        if (s.failed()) {
                            console->error("kv_revert failed: {}", s.error());
                        } else {
                            console->info("Reverted {}/{} to revision {} -> new revision={}",
                                         kv_bucket, kv_key, kv_revision, rev);
                        }
                        ioc.stop();
                    }
                    co_return;
                },
                [&console, conn_id](nats_asio::iconnection&) -> asio::awaitable<void> {
                    if (conn_id >= 0) {
                        console->info("connection {} disconnected", conn_id);
                    } else {
                        console->info("on disconnected");
                    }
                    co_return;
                },
                [&console, conn_id](nats_asio::iconnection&, nats_asio::string_view err) -> asio::awaitable<void> {
                    if (conn_id >= 0) {
                        console->error("connection {} error: {}", conn_id, err);
                    } else {
                        console->error("on error: {}", err);
                    }
                    co_return;
                },
                opt_ssl_conf);
        };

        if (m == mode::publisher) {
            bool use_batch_pub = result.count("batch_pub") > 0;
            bool use_jetstream = result.count("jetstream") > 0;

            if (use_batch_pub && use_jetstream && result.count("no_ack") == 0) {
                console->error("--batch_pub with --jetstream requires --no_ack (fire-and-forget mode)");
                return 1;
            }

            if (use_batch_pub) {
                // Multi-threaded batch publishing mode
                int batch_size_val = result.count("batch_size") ? result["batch_size"].as<int>() : 65536;
                int max_queue_val = result.count("max_queue") ? result["max_queue"].as<int>() : 100;
                int flush_timeout_val = result.count("flush_timeout") ? result["flush_timeout"].as<int>() : 0;
                console->info("Using multi-threaded batch publisher: {} writers, batch_size={}, max_queue={}",
                             num_connections, batch_size_val, max_queue_val);
                batch_pub_ptr = std::make_shared<batch_publisher>(console, conf, opt_ssl_conf, topic,
                                                                   num_connections, stats_interval,
                                                                   static_cast<std::size_t>(batch_size_val),
                                                                   static_cast<std::size_t>(max_queue_val),
                                                                   flush_timeout_val,
                                                                   src_cfg.file_path);
                // Run batch publisher (blocks until done)
                batch_pub_ptr->run();
                return 0;
            } else {
                // Standard publisher mode - multiple connections supported
                console->info("Creating {} connections for pub mode", num_connections);
                for (int i = 0; i < num_connections; i++) {
                    auto c = make_connection(i);
                    c->start(conf);
                    pub_connections.push_back(c);
                }
                int js_timeout_ms = result.count("js_timeout") ? result["js_timeout"].as<int>() : 5000;
                bool wait_for_ack = result.count("no_ack") == 0;  // default: wait for ack
                int js_window_size = result.count("js_window") ? result["js_window"].as<int>() : 1000;
                int js_max_retries = result.count("js_max_retries") ? result["js_max_retries"].as<int>() : 3;
                std::string js_stream = result.count("stream") ? result["stream"].as<std::string>() : "default";
                bool js_create_stream = result.count("create_stream") > 0;
                pub_ptr = std::make_shared<publisher>(ioc, console, pub_connections, topic, stats_interval,
                                                       max_in_flight, use_jetstream, js_timeout_ms, wait_for_ack,
                                                       headers, custom_reply_to, pub_count, pub_sleep_ms, pub_data,
                                                       in_cfg, src_cfg, binary_fmt, js_window_size, js_stream,
                                                       js_create_stream, js_max_retries);
            }
        } else if (m == mode::benchmarker) {
            int bench_count = result.count("count") ? result["count"].as<int>() : 100000;
            int bench_size = result.count("pub_size") ? result["pub_size"].as<int>() : 128;
            bool use_js = result.count("jetstream") > 0;
            bool bench_rtt = result.count("bench_rtt") > 0;
            int bench_timeout = result.count("timeout") ? result["timeout"].as<int>() : 5000;
            int bench_batch = result.count("bench_batch") ? result["bench_batch"].as<int>() : 1000;

            // Create connections for benchmark (use --connections option)
            std::vector<nats_asio::iconnection_sptr> bench_connections;
            for (int i = 0; i < num_connections; i++) {
                auto c = make_connection(i);
                c->start(conf);
                bench_connections.push_back(c);
            }
            console->info("Benchmark using {} connection(s)", num_connections);

            bench_ptr = std::make_shared<benchmarker>(ioc, console, bench_connections, topic, stats_interval,
                                                       bench_count, bench_size, use_js, bench_rtt, bench_timeout, bench_batch);
            asio::co_spawn(ioc, bench_ptr->run(), asio::detached);
        } else {
            conn = make_connection();
            conn->start(conf);

            if (m == mode::generator) {
                gen_ptr = std::make_shared<generator>(ioc, console, conn, topic, stats_interval,
                                                      publish_interval);
            } else if (m == mode::js_fetcher) {
                js_fetch_ptr = std::make_shared<js_fetcher>(ioc, console, conn, js_stream,
                                                            js_consumer, stats_interval, print_to_stdout,
                                                            batch_size, fetch_interval_ms,
                                                            out_mode, binary_fmt, max_bad_messages,
                                                            max_bad_percentage, dump_file, translate_cmd);
            } else if (m == mode::kv_publisher) {
                kv_pub_ptr = std::make_shared<kv_publisher>(ioc, console, conn, kv_bucket,
                                                            stats_interval, max_in_flight,
                                                            kv_separator, kv_timeout_ms);
            } else if (m == mode::requester) {
                int req_timeout = result.count("timeout") ? result["timeout"].as<int>() : 5000;
                std::string req_data = result.count("data") ? result["data"].as<std::string>() : "";
                req_ptr = std::make_shared<requester>(ioc, console, conn, topic, stats_interval,
                                                      req_timeout, req_data, out_mode, headers);
            } else if (m == mode::replier) {
                std::string reply_data = result.count("data") ? result["data"].as<std::string>() : "";
                bool echo_mode = result.count("echo") > 0;
                reply_ptr = std::make_shared<replier>(ioc, console, conn, topic, stats_interval,
                                                      reply_data, echo_mode, translate_cmd, queue_group, out_mode);
                asio::co_spawn(ioc, reply_ptr->start(), asio::detached);
            }
        }

        // Multi-threaded io_context support
        int num_threads = 1;
        if (result.count("threads")) {
            num_threads = result["threads"].as<int>();
            if (num_threads == 0) {
                num_threads = std::thread::hardware_concurrency();
            }
            if (num_threads < 1) {
                num_threads = 1;
            }

            // Check if current mode supports multi-threading
            if (num_threads > 1) {
                bool thread_safe = (m == mode::benchmarker || m == mode::generator || m == mode::publisher);
                if (!thread_safe && (m == mode::grubber ||
                                     m == mode::js_grubber || m == mode::js_fetcher)) {
                    console->warn("--threads > 1 not yet supported for this mode (requires strand support), using single thread");
                    num_threads = 1;
                }
            }
        }

        if (num_threads > 1) {
            console->info("Running io_context on {} threads", num_threads);
            std::vector<std::thread> thread_pool;
            thread_pool.reserve(num_threads - 1);

            // Start worker threads
            for (int i = 0; i < num_threads - 1; ++i) {
                thread_pool.emplace_back([&ioc]() {
                    ioc.run();
                });
            }

            // Run on main thread too
            ioc.run();

            // Wait for all threads to finish
            for (auto& t : thread_pool) {
                if (t.joinable()) {
                    t.join();
                }
            }
        } else {
            ioc.run();
        }

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "unknown exception" << std::endl;
        return 1;
    }

    return 0;
}
