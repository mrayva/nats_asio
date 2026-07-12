#!/usr/bin/env bash
set -euo pipefail

nats_tool=$1

expect_invalid() {
    if "$nats_tool" pub --topic validation.test "$@" >/dev/null 2>&1; then
        echo "expected invalid options to fail: $*" >&2
        exit 1
    fi
}

expect_invalid --jetstream --js_timeout 0
expect_invalid --jetstream --js_window 0
expect_invalid --jetstream --js_window -1
expect_invalid --jetstream --js_max_retries -1
expect_invalid --connections 0
expect_invalid --max_in_flight 0
expect_invalid --batch_pub --batch_size 0
expect_invalid --batch_pub --batch_size -1
expect_invalid --batch_pub --max_queue -1
expect_invalid --batch_pub --flush_timeout -1
expect_invalid --batch_pub --file /tmp/nats_asio_cli_test_missing_input --stats_interval 0
expect_invalid --max_line_size 0
expect_invalid --poll_interval 0
expect_invalid --poll_interval -1
expect_invalid --zip_max_entries 0
expect_invalid --zip_max_entry_bytes 0
expect_invalid --zip_max_total_bytes 0
expect_invalid --zip_max_entry_bytes 10 --zip_max_total_bytes 9
