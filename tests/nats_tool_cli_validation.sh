#!/usr/bin/env bash
set -euo pipefail

nats_tool=$1

expect_invalid() {
    if "$nats_tool" pub --topic validation.test --jetstream "$@" >/dev/null 2>&1; then
        echo "expected invalid options to fail: $*" >&2
        exit 1
    fi
}

expect_invalid --js_timeout 0
expect_invalid --js_window 0
expect_invalid --js_window -1
expect_invalid --js_max_retries -1
