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
#include "../include/zerialize_json.hpp"
#include "message_output.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <span>
#include <string>
#include <memory>
#include <chrono>
#include <ctime>
#include <thread>

namespace nats_tool {

class grubber : public worker {
public:
    grubber(asio::io_context& ioc, std::shared_ptr<spdlog::logger>& console, int stats_interval,
            output_mode mode, const std::string& dump_file = {}, const std::string& translate_cmd = {},
            bool show_timestamp = false, std::optional<binary_format> format = std::nullopt,
            std::size_t max_bad_messages = 0, double max_bad_percentage = 0.0,
            bool expand_columnar_records = false)
        : worker(ioc, console, stats_interval), m_output_mode(mode), m_translate_cmd(translate_cmd),
          m_show_timestamp(show_timestamp), m_format(format),
          m_deserializer_stats(max_bad_messages, max_bad_percentage),
          m_dump_writer(dump_file, console), m_expand_columnar_records(expand_columnar_records),
          m_writer_thread([this] { writer_loop(); }) {}

    // Stops and joins the writer thread, draining anything still queued
    // first - see writer_loop()'s comment for why this can't just discard
    // a backlog on shutdown. Safe to run a blocking join here: by the time
    // a grubber (owned via shared_ptr, ultimately from the connection's
    // subscription callback and grubber_runner::m_grub) is actually
    // destroyed, nats_tool.cpp's shutdown sequence has already made sure
    // ioc.run() returned first (see the comment there about pub_ptr for
    // the same reasoning applied to a different background thread).
    ~grubber() {
        m_stop.store(true, std::memory_order_release);
        if (m_writer_thread.joinable()) {
            m_writer_thread.join();
        }
    }

    grubber(const grubber&) = delete;
    grubber& operator=(const grubber&) = delete;

    // Takes the zero-copy message_view (rather than separate subject/
    // reply_to/payload args) specifically to get at msg.headers -- the
    // legacy on_message_cb subscribe() overload this used to bind to has
    // no headers at all. subject/reply_to/payload/headers here alias the
    // connection's own read buffer and are only valid until this
    // coroutine's next real suspension point (see message_view's own
    // warning) -- matches the existing reply_to usage below, which reads
    // it after the same co_await for the same reason: apply_translate_if_
    // configured() only actually suspends when --translate is given, and
    // copies subject/payload to owned storage itself before it does.
    //
    // Deliberately does the *minimum* work here before handing off to the
    // writer thread: decoding the payload (zerialize::translate<JSON>,
    // effectively a full parse-binary -> build-a-JSON-DOM -> serialize-
    // DOM-to-string round trip) and the dump-file write are real CPU cost,
    // and this coroutine runs on the same io_context thread that's also
    // responsible for reading the *next* incoming message off the socket.
    // Doing that decode+format+write work inline here means a burst of
    // messages arriving faster than one thread can decode+format+write
    // them blocks this thread from draining the socket in time - and NATS
    // has no backpressure for a plain (non-JetStream) subscription: once a
    // slow subscriber's server-side pending queue backs up far enough, the
    // server just silently drops further messages for it ("slow
    // consumer"), rather than the publisher ever being told to slow down.
    // Confirmed directly against a real NATS server (`slow_consumers` in
    // /varz incrementing, and dropped rows showing up as real content
    // mismatches) publishing ~1M messages at ~40-50k msgs/sec: this
    // coroutine returning as fast as possible - so the io_context can go
    // straight back to reading the socket - is what keeps up with that.
    asio::awaitable<void> on_message(const nats_asio::message_view& msg) {
        m_counter++;

        std::string translated_storage;
        auto output_payload = co_await apply_translate_if_configured(
            m_translate_cmd, msg.subject, msg.payload, m_log, translated_storage);

        queued_message qm;
        qm.subject = std::string(msg.subject);
        qm.payload.assign(output_payload.begin(), output_payload.end());

        // Timestamp, if requested, prefixes raw/normal lines and prepends the
        // JSON envelope's field list; reply_to/headers (if present) append to it.
        if (m_show_timestamp && m_output_mode != output_mode::none) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) % 1000;
            std::tm tm_now{};
            localtime_r(&time_t_now, &tm_now);
            std::string timestamp_str = fmt::format(
                "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
                tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday,
                tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec, static_cast<int>(ms.count()));
            qm.line_prefix = fmt::format("[{}] ", timestamp_str);
            qm.json_prefix_fields = fmt::format("\"timestamp\":\"{}\",", timestamp_str);
        }
        if (msg.reply_to) {
            qm.json_suffix_fields = fmt::format(",\"reply_to\":\"{}\"", *msg.reply_to);
        }
        if (msg.headers.has_data()) {
            qm.json_suffix_fields += format_headers_json_suffix(msg.headers.get());
        }

        m_queue.enqueue(std::move(qm));

        co_return;
    }

private:
    // Everything emit_message() needs, owned rather than viewing the
    // connection's read buffer - the writer thread processes this well
    // after on_message()'s coroutine (and the message_view it was given)
    // has returned.
    struct queued_message {
        std::string subject;
        std::string payload;
        std::string json_prefix_fields;
        std::string json_suffix_fields;
        std::string line_prefix;
    };

    // Runs on its own thread (started in the constructor, stopped/joined
    // in the destructor) so the io_context thread's on_message() never
    // blocks on decode+format+write - see on_message()'s comment for why
    // that matters. wait_dequeue_timed() blocks efficiently (no busy-spin)
    // when idle and wakes promptly when a message arrives, unlike a plain
    // try_dequeue()-in-a-loop; the timeout just bounds how long shutdown
    // can take to notice m_stop.
    //
    // m_dump_writer/m_deserializer_stats are, after this change, only ever
    // touched from this one thread (previously only ever touched from the
    // io_context thread) - never both at once, so no new synchronization
    // is needed for them specifically. m_ioc.stop() (called by
    // emit_message() if the bad-message threshold is exceeded) is
    // documented safe to call from any thread.
    void writer_loop() {
        queued_message qm;
        for (;;) {
            if (m_queue.wait_dequeue_timed(qm, std::chrono::milliseconds(50))) {
                process(qm);
                continue;
            }
            if (m_stop.load(std::memory_order_acquire)) {
                // A message could have been enqueued after the timed-out
                // wait above gave up but before this stop check - drain
                // whatever's left rather than discarding it.
                while (m_queue.try_dequeue(qm)) {
                    process(qm);
                }
                return;
            }
        }
    }

    void process(const queued_message& qm) {
        std::ostream& out = m_dump_writer.stream();
        emit_message(out, m_output_mode, qm.subject, std::span<const char>(qm.payload), m_format,
                    m_deserializer_stats, m_log, m_ioc, qm.json_prefix_fields, qm.json_suffix_fields,
                    qm.line_prefix, m_expand_columnar_records);
        m_dump_writer.on_message_written();
    }

    output_mode m_output_mode;
    std::string m_translate_cmd;
    bool m_show_timestamp;
    std::optional<binary_format> m_format;
    deserializer_stats m_deserializer_stats;
    dump_file_writer m_dump_writer;
    bool m_expand_columnar_records;

    moodycamel::BlockingConcurrentQueue<queued_message> m_queue;
    std::atomic<bool> m_stop{false};
    // Must be initialized last: the constructor starts it running
    // immediately, and it reads every other member above.
    std::thread m_writer_thread;
};

} // namespace nats_tool
