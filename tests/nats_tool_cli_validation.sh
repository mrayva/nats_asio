#!/usr/bin/env bash
set -euo pipefail

nats_tool=$1

# Every check in this file is expected to fail during argument validation,
# strictly before any connection is attempted - so it's supposed to return
# in well under a second, no live nats-server required. A `timeout` wrapper
# is still worth it: if a validation check ever regresses into silently
# accepting a required flag (e.g. a missing --value defaulting to empty
# instead of being rejected), the command can fall through into mode logic
# that actually connects and blocks waiting on a server response that will
# never come - turning a should-be-obvious test failure into the whole
# suite hanging indefinitely instead. Bounding it means that regression
# still fails loudly and quickly rather than stalling CI.
readonly TIMEOUT_SECS=10

# Shorthand for the numeric-range validation checks below: all of them are
# pub-mode flags, so the mode/--topic boilerplate is folded in here.
expect_invalid() {
    if timeout "$TIMEOUT_SECS" "$nats_tool" pub --topic validation.test "$@" >/dev/null 2>&1; then
        echo "expected invalid options to fail: $*" >&2
        exit 1
    fi
}

# Full command line, mode included - for validation checks that are
# mode-specific (e.g. --stream required for js_grub) rather than pub-only.
expect_invalid_cmd() {
    if timeout "$TIMEOUT_SECS" "$nats_tool" "$@" >/dev/null 2>&1; then
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
expect_invalid --format bogus
expect_invalid --max_bad_percentage -1
expect_invalid --max_bad_percentage 101
expect_invalid --io_shards -1

# All of these fail during argument validation, strictly before any
# connection is attempted, so - like everything else in this file - none of
# them need a live nats-server.

# Missing --mode / unrecognized --mode.
expect_invalid_cmd
expect_invalid_cmd bogus_mode

# --batch_pub with --jetstream requires --no_ack (fire-and-forget); without
# it, JetStream ack-waiting and the batch publisher's own fire-and-forget
# write path would conflict.
expect_invalid_cmd pub --topic validation.test --batch_pub --jetstream

# js_grub/js_fetch require --stream; js_fetch additionally requires
# --consumer.
expect_invalid_cmd js_grub --topic validation.test
expect_invalid_cmd js_fetch --topic validation.test --stream s

# Every KV mode requires --bucket (checked once, shared across all of them -
# pubkv exercises the shared check; the rest exercise their own additional
# per-mode requirements below).
expect_invalid_cmd pubkv
expect_invalid_cmd kvcreate --bucket b
expect_invalid_cmd kvcreate --bucket b --key k
expect_invalid_cmd kvupdate --bucket b --key k --value v
expect_invalid_cmd kvhistory --bucket b
expect_invalid_cmd kvpurge --bucket b
expect_invalid_cmd kvrevert --bucket b --key k
