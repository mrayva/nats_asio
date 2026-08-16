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

// One mode_runner subclass per single-connection CLI mode. Each of these is a
// direct port of what used to be one branch of main()'s two dispatch chains -
// same log messages, same error handling, same control flow, just moved into
// a method so dispatch can be a single virtual call instead of a 14-way
// if/else-if repeated at both the on-connect and post-connect injection
// points.

#include "mode_runner.hpp"
#include "generator.hpp"
#include "grubber.hpp"
#include "js_fetcher.hpp"
#include "js_grubber.hpp"
#include "kv_publisher.hpp"
#include "kv_watcher_handler.hpp"
#include "replier.hpp"
#include "requester.hpp"
#include <nats_asio/nats_asio.hpp>
#include <iostream>
#include <memory>
#include <span>
#include <string>

namespace nats_tool {

// --- grub --------------------------------------------------------------

class grubber_runner : public mode_runner {
public:
    grubber_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                   std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_topic(ctx.topic),
          m_queue_group(ctx.queue_group), m_max_msgs(ctx.max_msgs),
          m_grub(std::make_shared<grubber>(ioc, m_console, ctx.stats_interval, ctx.out_mode,
                                            ctx.dump_file, ctx.translate_cmd, ctx.show_timestamp,
                                            ctx.binary_fmt, ctx.max_bad_messages,
                                            ctx.max_bad_percentage,
                                            ctx.expand_columnar_records)) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        nats_asio::subscribe_options sub_opts;
        if (!m_queue_group.empty()) {
            sub_opts.queue_group = m_queue_group;
        }
        sub_opts.max_messages = m_max_msgs;

        auto grub = m_grub;
        auto r = co_await conn->subscribe(
            m_topic,
            [grub](const nats_asio::message_view& msg) -> asio::awaitable<void> {
                return grub->on_message(msg);
            },
            sub_opts);

        if (r.second.failed()) {
            fail(fmt::format("failed to subscribe with error: {}", r.second.error()));
        } else if (m_max_msgs > 0) {
            m_console->info("subscribed to {} (will auto-unsubscribe after {} messages)", m_topic,
                            m_max_msgs);
        }
    }

private:
    std::string m_topic;
    std::string m_queue_group;
    uint32_t m_max_msgs;
    std::shared_ptr<grubber> m_grub;
};

// --- js_grub -------------------------------------------------------------

class js_grubber_runner : public mode_runner {
public:
    js_grubber_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                      std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_topic(ctx.topic),
          m_js_stream(ctx.js_stream), m_js_durable(ctx.js_durable),
          m_js_grub(std::make_shared<js_grubber>(ioc, m_console, ctx.stats_interval, ctx.out_mode,
                                                  ctx.auto_ack, ctx.dump_file, ctx.translate_cmd,
                                                  ctx.binary_fmt, ctx.max_bad_messages,
                                                  ctx.max_bad_percentage,
                                                  ctx.expand_columnar_records)) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        nats_asio::js_consumer_config config;
        config.stream = m_js_stream;
        config.filter_subject = m_topic.empty() ? std::nullopt : std::optional<std::string>(m_topic);
        if (!m_js_durable.empty()) {
            config.durable_name = m_js_durable;
        }
        config.ack = nats_asio::js_ack_policy::explicit_;

        auto js_grub = m_js_grub;
        auto [sub, s] = co_await conn->js_subscribe(
            config,
            [js_grub](nats_asio::ijs_subscription& sub,
                      const nats_asio::js_message& msg) -> asio::awaitable<void> {
                return js_grub->on_js_message(sub, msg);
            });

        if (s.failed()) {
            fail(fmt::format("js_subscribe failed: {}", s.error()));
        } else {
            m_console->info("JetStream subscription active: stream={} consumer={}",
                            sub->info().stream, sub->info().name);
        }
    }

private:
    std::string m_topic;
    std::string m_js_stream;
    std::string m_js_durable;
    std::shared_ptr<js_grubber> m_js_grub;
};

// --- kvwatch ---------------------------------------------------------------

class kv_watcher_runner : public mode_runner {
public:
    kv_watcher_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                      std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_key(ctx.kv_key),
          m_watcher(std::make_shared<kv_watcher_handler>(ioc, m_console, ctx.stats_interval,
                                                          ctx.print_to_stdout)) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        auto watcher = m_watcher;
        auto [w, s] = co_await conn->kv_watch(
            m_kv_bucket,
            [watcher](const nats_asio::kv_entry& entry) -> asio::awaitable<void> {
                return watcher->on_kv_entry(entry);
            },
            m_kv_key);

        if (s.failed()) {
            fail(fmt::format("kv_watch failed: {}", s.error()));
        } else if (m_kv_key.empty()) {
            m_console->info("Watching KV bucket: {}", m_kv_bucket);
        } else {
            m_console->info("Watching KV bucket: {} key: {}", m_kv_bucket, m_kv_key);
        }
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_key;
    std::shared_ptr<kv_watcher_handler> m_watcher;
};

// --- one-shot KV operations ------------------------------------------------
// kvcreate/kvupdate/kvkeys/kvhistory/kvpurge/kvrevert all stop the event loop
// unconditionally once their single request completes, success or failure -
// unlike fail()'s "stop only on error" (grub/js_grub/kvwatch run forever, so
// only an error path needs to interrupt them).

class kv_create_runner : public mode_runner {
public:
    kv_create_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                     std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_key(ctx.kv_key), m_kv_value(ctx.kv_value), m_kv_timeout_ms(ctx.kv_timeout_ms) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        std::span<const char> value_span(m_kv_value.data(), m_kv_value.size());
        auto [rev, s] = co_await conn->kv_create(m_kv_bucket, m_kv_key, value_span,
                                                  std::chrono::milliseconds(m_kv_timeout_ms));
        if (s.failed()) {
            m_console->error("kv_create failed: {}", s.error());
            m_operation_failed.store(true, std::memory_order_release);
        } else {
            m_console->info("Created {}/{} revision={}", m_kv_bucket, m_kv_key, rev);
        }
        m_ioc.stop();
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_key;
    std::string m_kv_value;
    int m_kv_timeout_ms;
};

class kv_update_runner : public mode_runner {
public:
    kv_update_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                     std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_key(ctx.kv_key), m_kv_value(ctx.kv_value), m_kv_revision(ctx.kv_revision),
          m_kv_timeout_ms(ctx.kv_timeout_ms) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        std::span<const char> value_span(m_kv_value.data(), m_kv_value.size());
        auto [rev, s] = co_await conn->kv_update(m_kv_bucket, m_kv_key, value_span, m_kv_revision,
                                                  std::chrono::milliseconds(m_kv_timeout_ms));
        if (s.failed()) {
            m_console->error("kv_update failed: {}", s.error());
            m_operation_failed.store(true, std::memory_order_release);
        } else {
            m_console->info("Updated {}/{} revision={} (was {})", m_kv_bucket, m_kv_key, rev,
                            m_kv_revision);
        }
        m_ioc.stop();
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_key;
    std::string m_kv_value;
    uint64_t m_kv_revision;
    int m_kv_timeout_ms;
};

class kv_keys_runner : public mode_runner {
public:
    kv_keys_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                   std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_pattern(ctx.kv_key), m_kv_timeout_ms(ctx.kv_timeout_ms) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        // --key doubles as an optional wildcard pattern here (e.g. "test.*",
        // "alice.>") -- same as `nats kv ls BUCKET "pattern"`; omitted means
        // every key in the bucket.
        auto [keys, s] = m_kv_pattern.empty()
            ? co_await conn->kv_keys(m_kv_bucket, std::chrono::milliseconds(m_kv_timeout_ms))
            : co_await conn->kv_keys(m_kv_bucket, m_kv_pattern, std::chrono::milliseconds(m_kv_timeout_ms));
        if (s.failed()) {
            m_console->error("kv_keys failed: {}", s.error());
            m_operation_failed.store(true, std::memory_order_release);
        } else {
            m_console->info("Keys in bucket '{}' ({} keys):", m_kv_bucket, keys.size());
            for (const auto& key : keys) {
                std::cout << key << std::endl;
            }
        }
        m_ioc.stop();
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_pattern;
    int m_kv_timeout_ms;
};

class kv_history_runner : public mode_runner {
public:
    kv_history_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                      std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_key(ctx.kv_key), m_kv_timeout_ms(ctx.kv_timeout_ms) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        auto [history, s] = co_await conn->kv_history(m_kv_bucket, m_kv_key,
                                                       std::chrono::milliseconds(m_kv_timeout_ms));
        if (s.failed()) {
            m_console->error("kv_history failed: {}", s.error());
            m_operation_failed.store(true, std::memory_order_release);
        } else {
            m_console->info("History for {}/{} ({} revisions):", m_kv_bucket, m_kv_key,
                            history.size());
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
        m_ioc.stop();
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_key;
    int m_kv_timeout_ms;
};

class kv_purge_runner : public mode_runner {
public:
    kv_purge_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                    std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_key(ctx.kv_key), m_kv_timeout_ms(ctx.kv_timeout_ms) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        auto [rev, s] = co_await conn->kv_purge(m_kv_bucket, m_kv_key,
                                                std::chrono::milliseconds(m_kv_timeout_ms));
        if (s.failed()) {
            m_console->error("kv_purge failed: {}", s.error());
            m_operation_failed.store(true, std::memory_order_release);
        } else {
            m_console->info("Purged {}/{} revision={}", m_kv_bucket, m_kv_key, rev);
        }
        m_ioc.stop();
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_key;
    int m_kv_timeout_ms;
};

class kv_revert_runner : public mode_runner {
public:
    kv_revert_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                     std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_kv_bucket(ctx.kv_bucket),
          m_kv_key(ctx.kv_key), m_kv_revision(ctx.kv_revision),
          m_kv_timeout_ms(ctx.kv_timeout_ms) {}

    asio::awaitable<void> on_connected(nats_asio::iconnection_sptr conn) override {
        auto [rev, s] = co_await conn->kv_revert(m_kv_bucket, m_kv_key, m_kv_revision,
                                                 std::chrono::milliseconds(m_kv_timeout_ms));
        if (s.failed()) {
            m_console->error("kv_revert failed: {}", s.error());
            m_operation_failed.store(true, std::memory_order_release);
        } else {
            m_console->info("Reverted {}/{} to revision {} -> new revision={}", m_kv_bucket,
                            m_kv_key, m_kv_revision, rev);
        }
        m_ioc.stop();
    }

private:
    std::string m_kv_bucket;
    std::string m_kv_key;
    uint64_t m_kv_revision;
    int m_kv_timeout_ms;
};

// --- post-connect setup modes ----------------------------------------------
// generator/js_fetcher/kv_publisher/requester/replier each construct a worker
// whose own constructor spawns its independent coroutine, rather than doing
// anything inside the on-connected callback itself.

class generator_runner : public mode_runner {
public:
    generator_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                     std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_ctx(ctx) {}

    void setup(nats_asio::iconnection_sptr conn) override {
        m_gen = std::make_shared<generator>(m_ioc, m_console, conn, m_ctx.topic,
                                            m_ctx.stats_interval, m_ctx.publish_interval_ms);
    }

private:
    mode_context m_ctx;
    std::shared_ptr<generator> m_gen;
};

class js_fetcher_runner : public mode_runner {
public:
    js_fetcher_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                      std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_ctx(ctx) {}

    void setup(nats_asio::iconnection_sptr conn) override {
        m_fetch = std::make_shared<js_fetcher>(
            m_ioc, m_console, conn, m_ctx.js_stream, m_ctx.js_consumer, m_ctx.stats_interval,
            m_ctx.print_to_stdout, m_ctx.batch_size, m_ctx.fetch_interval_ms, m_ctx.out_mode,
            m_ctx.binary_fmt, m_ctx.max_bad_messages, m_ctx.max_bad_percentage, m_ctx.dump_file,
            m_ctx.translate_cmd, m_ctx.expand_columnar_records);
    }

private:
    mode_context m_ctx;
    std::shared_ptr<js_fetcher> m_fetch;
};

class kv_publisher_runner : public mode_runner {
public:
    kv_publisher_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                        std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_ctx(ctx) {}

    void setup(nats_asio::iconnection_sptr conn) override {
        m_pub = std::make_shared<kv_publisher>(m_ioc, m_console, conn, m_ctx.kv_bucket,
                                               m_ctx.stats_interval, m_ctx.max_in_flight,
                                               m_ctx.kv_separator, m_ctx.kv_timeout_ms);
    }

private:
    mode_context m_ctx;
    std::shared_ptr<kv_publisher> m_pub;
};

class requester_runner : public mode_runner {
public:
    requester_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                     std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_ctx(ctx) {}

    void setup(nats_asio::iconnection_sptr conn) override {
        m_req = std::make_shared<requester>(m_ioc, m_console, conn, m_ctx.topic,
                                            m_ctx.stats_interval, m_ctx.timeout_ms,
                                            m_ctx.data_or_stdin, m_ctx.out_mode, m_ctx.headers);
    }

private:
    mode_context m_ctx;
    std::shared_ptr<requester> m_req;
};

class replier_runner : public mode_runner {
public:
    replier_runner(asio::io_context& ioc, std::shared_ptr<spdlog::logger> console,
                   std::atomic<bool>& operation_failed, const mode_context& ctx)
        : mode_runner(ioc, console, operation_failed), m_ctx(ctx) {}

    void setup(nats_asio::iconnection_sptr conn) override {
        m_reply = std::make_shared<replier>(m_ioc, m_console, conn, m_ctx.topic,
                                            m_ctx.stats_interval, m_ctx.data_or_stdin,
                                            m_ctx.echo_mode, m_ctx.translate_cmd,
                                            m_ctx.queue_group, m_ctx.out_mode);
        asio::co_spawn(m_ioc, m_reply->start(), asio::detached);
    }

private:
    mode_context m_ctx;
    std::shared_ptr<replier> m_reply;
};

} // namespace nats_tool
