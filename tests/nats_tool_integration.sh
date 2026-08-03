#!/usr/bin/env bash
# Integration smoke test: drives the built nats_tool binary through the happy
# path of each of its 16 CLI modes against a real NATS server with JetStream
# enabled. Unlike the unit tests (which run against a hand-rolled fake NATS
# protocol server), the JetStream/KV modes need real server-side behavior
# that isn't worth faking, so this test requires an actual nats-server.
#
# If no server is reachable on NATS_HOST:NATS_PORT, the test SKIPs (exit 125,
# recognized by CTest's SKIP_RETURN_CODE) rather than failing, so it doesn't
# break `ctest` for anyone without a local server running.
#
# Usage: nats_tool_integration.sh <path-to-nats_tool-binary>

set -uo pipefail

SKIP_CODE=125

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <path-to-nats_tool-binary>" >&2
    exit 1
fi

NATS_TOOL="$1"
NATS_HOST="${NATS_HOST:-127.0.0.1}"
NATS_PORT="${NATS_PORT:-4222}"
RUN_ID="itest_$$_$(date +%s)"

if ! timeout 2 bash -c "cat < /dev/null > /dev/tcp/${NATS_HOST}/${NATS_PORT}" 2>/dev/null; then
    echo "SKIP: no NATS server reachable on ${NATS_HOST}:${NATS_PORT}"
    exit "$SKIP_CODE"
fi

WORKDIR="$(mktemp -d)"
declare -a BG_PIDS=()
declare -a STREAMS_TO_CLEAN=()
PASS_COUNT=0
FAIL_COUNT=0
declare -a FAILED_TESTS=()

cleanup() {
    for pid in "${BG_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${BG_PIDS[@]:-}"; do
        wait "$pid" 2>/dev/null || true
    done
    for stream in "${STREAMS_TO_CLEAN[@]:-}"; do
        delete_stream "$stream"
    done
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# --- Helpers ---------------------------------------------------------------

# Runs nats_tool in the background, remembers its PID for cleanup, redirects
# output to $1 (a file path).
start_bg() {
    local out_file="$1"
    shift
    "$NATS_TOOL" "$@" > "$out_file" 2>&1 &
    local pid=$!
    BG_PIDS+=("$pid")
    echo "$pid"
}

stop_bg() {
    local pid="$1"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

delete_stream() {
    local stream="$1"
    "$NATS_TOOL" req --topic "\$JS.API.STREAM.DELETE.${stream}" --data '{}' --timeout 3000 \
        > /dev/null 2>&1 || true
}

create_js_stream() {
    local stream="$1" subject="$2"
    STREAMS_TO_CLEAN+=("$stream")
    "$NATS_TOOL" req --topic "\$JS.API.STREAM.CREATE.${stream}" --timeout 5000 \
        --data "{\"name\":\"${stream}\",\"subjects\":[\"${subject}\"],\"retention\":\"limits\",\"storage\":\"file\",\"num_replicas\":1}" \
        > /dev/null 2>&1
}

create_kv_bucket() {
    local bucket="$1"
    local stream="KV_${bucket}"
    STREAMS_TO_CLEAN+=("$stream")
    "$NATS_TOOL" req --topic "\$JS.API.STREAM.CREATE.${stream}" --timeout 5000 \
        --data "{\"name\":\"${stream}\",\"subjects\":[\"\$KV.${bucket}.>\"],\"retention\":\"limits\",\"max_msgs_per_subject\":10,\"discard\":\"new\",\"storage\":\"file\",\"num_replicas\":1,\"allow_direct\":true,\"allow_rollup_hdrs\":true}" \
        > /dev/null 2>&1
}

create_durable_pull_consumer() {
    local stream="$1" consumer="$2"
    "$NATS_TOOL" req --topic "\$JS.API.CONSUMER.DURABLE.CREATE.${stream}.${consumer}" --timeout 5000 \
        --data "{\"stream_name\":\"${stream}\",\"config\":{\"durable_name\":\"${consumer}\",\"ack_policy\":\"explicit\"}}" \
        > /dev/null 2>&1
}

run_test() {
    local name="$1"
    shift
    echo "--- ${name} ---"
    if "$@"; then
        echo "PASS: ${name}"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo "FAIL: ${name}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_TESTS+=("$name")
    fi
}

# --- Mode tests --------------------------------------------------------------

test_grub_pub() {
    local subj="${RUN_ID}.grub_pub"
    local out="${WORKDIR}/grub_pub.log"
    local pid
    pid=$(start_bg "$out" grub --topic "$subj" --print)
    sleep 1
    "$NATS_TOOL" pub --topic "$subj" --data "hello-plain" --count 1 > /dev/null 2>&1
    sleep 1
    stop_bg "$pid"
    grep -q "hello-plain" "$out"
}

test_pub_headers() {
    local subj="${RUN_ID}.pub_headers"
    local out="${WORKDIR}/pub_headers.log"
    local pid
    pid=$(start_bg "$out" grub --topic "$subj" --print)
    sleep 1
    "$NATS_TOOL" pub --topic "$subj" --data "hello-headers" --count 1 -H "X-Test:1" > /dev/null 2>&1
    sleep 1
    stop_bg "$pid"
    grep -q "hello-headers" "$out"
}

test_pub_input_format_json() {
    # Regression test: --input_format json without --payload_fields used to
    # rebuild the payload via nlohmann::json's dump(), which sorts object
    # keys alphabetically - so every publish silently reordered the input's
    # keys even when no transformation was actually requested. Also covers
    # --subject_template (needs the same parsed object) and --payload_fields
    # (field selection), which must keep working since they share the parse.
    local subj="${RUN_ID}.pub_json"
    local out="${WORKDIR}/pub_json.log"
    local pid
    pid=$(start_bg "$out" grub --topic "${subj}.>" --print)
    sleep 1

    # No --payload_fields/--subject_template: payload must come through with
    # its original key order intact, not alphabetically resorted. (--topic
    # gets a ".plain" suffix so it still falls under the "${subj}.>"
    # wildcard grub subscribes to below - a bare "$subj" publish wouldn't.)
    printf '%s\n' '{"zebra":1,"apple":2,"mango":3}' \
        | "$NATS_TOOL" pub --topic "${subj}.plain" --input_format json > /dev/null 2>&1
    sleep 1

    # --subject_template + --payload_fields together.
    printf '%s\n' '{"kind":"orders","zebra":1,"apple":2}' \
        | "$NATS_TOOL" pub --topic "${subj}.plain" --input_format json \
            --subject_template "${subj}.{{kind}}" --payload_fields "apple" > /dev/null 2>&1
    sleep 1

    # Malformed JSON on the fast (no template/fields) path: only a syntax
    # check runs there (simdjson, not nlohmann), so this exercises a
    # different parser than the two publishes above - must still be
    # rejected and never delivered, not crash or publish garbage.
    printf '%s\n' '{"broken":' \
        | "$NATS_TOOL" pub --topic "${subj}.plain" --input_format json > /dev/null 2>&1
    sleep 1

    # Malformed JSON on the templated path (nlohmann) - same requirement.
    printf '%s\n' '{"broken":' \
        | "$NATS_TOOL" pub --topic "${subj}.plain" --input_format json \
            --subject_template "${subj}.{{kind}}" --payload_fields "apple" > /dev/null 2>&1
    sleep 1

    # One more valid message so we can tell "malformed lines were skipped"
    # apart from "grub/pub stopped working after the malformed lines".
    printf '%s\n' '{"trailer":true}' \
        | "$NATS_TOOL" pub --topic "${subj}.plain" --input_format json > /dev/null 2>&1
    sleep 1

    stop_bg "$pid"

    grep -qF '{"zebra":1,"apple":2,"mango":3}' "$out" || {
        echo "payload key order was not preserved: $(cat "$out")"
        return 1
    }
    grep -qF "[${subj}.orders]" "$out" || {
        echo "subject_template did not resolve to ${subj}.orders: $(cat "$out")"
        return 1
    }
    grep -qF '{"apple":2}' "$out" || {
        echo "payload_fields did not filter to just 'apple': $(cat "$out")"
        return 1
    }
    grep -qF '"broken"' "$out" && {
        echo "malformed JSON was delivered instead of being rejected: $(cat "$out")"
        return 1
    }
    grep -qF '{"trailer":true}' "$out" || {
        echo "publishing did not recover after malformed JSON lines: $(cat "$out")"
        return 1
    }
}

test_req_reply() {
    local subj="${RUN_ID}.rpc"
    local out="${WORKDIR}/reply.log"
    local pid
    pid=$(start_bg "$out" reply --topic "$subj" --echo)
    sleep 1
    local resp
    resp=$("$NATS_TOOL" req --topic "$subj" --data "ping" --timeout 3000 2>&1)
    stop_bg "$pid"
    echo "$resp" | grep -q "ping"
}

test_translate_shell_injection() {
    # Regression test for a real shell-injection bug: translate_payload()
    # (shared by grub/js_grub/js_fetch, and reply via its own call site into
    # the same function) used to splice the NATS message subject - which is
    # attacker-controlled, since anyone who can publish controls it - verbatim
    # into a command string passed to `sh -c`. A subject containing shell
    # metacharacters could execute arbitrary commands on the subscriber's
    # host. Fixed by shell-quoting the subject before substitution. Uses
    # $IFS instead of a literal space since NATS subjects can't contain
    # whitespace (the wire protocol's own PUB line parsing uses it as a
    # delimiter), but $IFS expands to whitespace inside the injected sh -c
    # command, so it's an equivalent injection vector without needing one.
    local marker="${WORKDIR}/pwned_marker"
    rm -f "$marker"
    local subj="${RUN_ID}.inject_test;touch\$IFS${marker};echo"

    local grub_out="${WORKDIR}/inject_grub.log"
    local grub_pid
    grub_pid=$(start_bg "$grub_out" grub --topic "$subj" --translate "echo GOT:{{Subject}}")
    sleep 1
    "$NATS_TOOL" pub --topic "$subj" --data "hi" --count 1 > /dev/null 2>&1
    sleep 1
    stop_bg "$grub_pid"

    if [[ -f "$marker" ]]; then
        echo "SECURITY REGRESSION: grub --translate executed an injected command"
        return 1
    fi
    grep -qF "GOT:${subj}" "$grub_out" || { echo "grub --translate did not run as expected"; return 1; }

    # Also check reply mode's separate call site into the same shared helper.
    rm -f "$marker"
    local reply_out="${WORKDIR}/inject_reply.log"
    local reply_pid
    reply_pid=$(start_bg "$reply_out" reply --topic "$subj" --translate "echo GOT:{{Subject}}")
    sleep 1
    local resp
    resp=$("$NATS_TOOL" req --topic "$subj" --data "hi" --timeout 3000 2>&1)
    stop_bg "$reply_pid"

    if [[ -f "$marker" ]]; then
        echo "SECURITY REGRESSION: reply --translate executed an injected command"
        return 1
    fi
    echo "$resp" | grep -qF "GOT:${subj}"
}

test_dump_file_flushes_on_sigint() {
    # Regression test for a data-loss risk introduced alongside the
    # dump_file_writer perf fix: grub --dump only flushes its output file
    # every flush_every (100) messages instead of every one, for
    # throughput. grub/js_grub/js_fetch have no natural completion, so the
    # only way to stop them is Ctrl-C/kill - previously safe (every message
    # was already durable), but with periodic flushing, an abrupt process
    # kill could lose up to 99 buffered-but-unflushed messages, since
    # nats_tool had no signal handling at all. Fixed by catching
    # SIGINT/SIGTERM and routing through ioc.stop() so the normal (already
    # correct) teardown path runs and flushes on the way out. This
    # publishes well under the flush_every threshold, so the dump file can
    # only end up complete if the signal-triggered graceful shutdown
    # actually ran the flush - a periodic-only flush would leave it empty.
    local subj="${RUN_ID}.dump_sigint"
    local dump_file="${WORKDIR}/dump_sigint.bin"
    rm -f "$dump_file"

    "$NATS_TOOL" grub --topic "$subj" --dump "$dump_file" --raw > "${WORKDIR}/dump_sigint_grub.log" 2>&1 &
    local grub_pid=$!
    sleep 1

    for i in 1 2 3 4 5; do
        "$NATS_TOOL" pub --topic "$subj" --data "dump-msg-${i}" --count 1 > /dev/null 2>&1
    done
    sleep 1

    kill -SIGINT "$grub_pid"
    wait "$grub_pid"
    local exit_status=$?

    if [[ $exit_status -ne 0 ]]; then
        echo "grub did not exit cleanly after SIGINT (status ${exit_status})"
        return 1
    fi

    for i in 1 2 3 4 5; do
        grep -qF "dump-msg-${i}" "$dump_file" || {
            echo "dump file is missing message ${i} after graceful SIGINT shutdown: $(cat "$dump_file" 2>&1)"
            return 1
        }
    done
}

test_generator() {
    local subj="${RUN_ID}.gen"
    local grub_out="${WORKDIR}/gen_grub.log"
    local gen_out="${WORKDIR}/gen.log"
    local grub_pid gen_pid
    grub_pid=$(start_bg "$grub_out" grub --topic "$subj" --print)
    sleep 1
    gen_pid=$(start_bg "$gen_out" gen --topic "$subj" --publish_interval 100)
    sleep 1.5
    stop_bg "$gen_pid"
    stop_bg "$grub_pid"
    grep -q "value" "$grub_out"
}

test_bench() {
    local subj="${RUN_ID}.bench"
    timeout 15 "$NATS_TOOL" bench --topic "$subj" --connections 1 --count 200 --pub_size 64 \
        > "${WORKDIR}/bench.log" 2>&1
    grep -qi "throughput\|msgs/sec\|complete\|finished" "${WORKDIR}/bench.log" || \
        [[ -s "${WORKDIR}/bench.log" ]]
}

test_batch_pub() {
    local subj="${RUN_ID}.batch"
    local out="${WORKDIR}/batch_grub.log"
    local pid
    pid=$(start_bg "$out" grub --topic "$subj" --print)
    sleep 1
    printf 'line1\nline2\nline3\n' | timeout 10 "$NATS_TOOL" pub --topic "$subj" --batch_pub \
        > "${WORKDIR}/batch_pub.log" 2>&1
    sleep 1
    stop_bg "$pid"
    grep -q "line1" "$out" && grep -q "line3" "$out"
}

test_batch_pub_unreachable_server_does_not_hang() {
    # Regression test for a real hang bug: a writer thread's connect-wait
    # loop was `while (!conn->is_connected() && m_running)`, but m_running
    # only flips false after run() joins the writer threads - which itself
    # blocks on this very loop. Against an unreachable server, every writer
    # spun forever waiting for a flag that could never change until the
    # writer itself exited. Fixed by also checking m_failed (set by the
    # connection's error callback) in that loop, plus giving the batch
    # queue an abort flag so a reader_thread blocked on push() (queue full,
    # nothing consuming) doesn't hang either once all writers have died.
    #
    # Port 1 has nothing listening, giving an immediate ECONNREFUSED rather
    # than a slow connect timeout, so a working fix returns in well under a
    # second and a regression (hang) is caught decisively by the 15s bound
    # without the test itself being slow.
    local subj="${RUN_ID}.batch_unreachable"
    printf 'line1\nline2\n' | timeout 15 "$NATS_TOOL" pub --topic "$subj" --batch_pub \
        --address 127.0.0.1 --port 1 > "${WORKDIR}/batch_unreachable.log" 2>&1
    local status=$?

    if [[ $status -eq 124 ]]; then
        echo "hung: killed by the 15s safety timeout (regression of the batch_publisher hang bug)"
        cat "${WORKDIR}/batch_unreachable.log"
        return 1
    fi
    # Publishing against an unreachable server should be reported as a
    # failure, not silently succeed.
    if [[ $status -eq 0 ]]; then
        echo "expected non-zero exit for an unreachable server, got 0"
        cat "${WORKDIR}/batch_unreachable.log"
        return 1
    fi
    return 0
}

test_js_publish() {
    local stream="${RUN_ID}_JS_PUB"
    local subj="${RUN_ID}.js_pub"
    create_js_stream "$stream" "$subj"
    timeout 10 "$NATS_TOOL" pub --js --stream "$stream" --topic "$subj" --data "js-hello" --count 1 \
        > "${WORKDIR}/js_pub.log" 2>&1
    grep -q "acked=1" "${WORKDIR}/js_pub.log"
}

test_js_grub() {
    local stream="${RUN_ID}_JS_GRUB"
    local subj="${RUN_ID}.js_grub"
    create_js_stream "$stream" "$subj"
    local out="${WORKDIR}/js_grub.log"
    local pid
    pid=$(start_bg "$out" js_grub --stream "$stream" --topic "$subj" --auto_ack --print)
    sleep 1
    "$NATS_TOOL" pub --js --stream "$stream" --topic "$subj" --data "js-grub-hello" --count 1 \
        > /dev/null 2>&1
    sleep 1.5
    stop_bg "$pid"
    grep -q "js-grub-hello" "$out"
}

test_js_fetch() {
    local stream="${RUN_ID}_JS_FETCH"
    local subj="${RUN_ID}.js_fetch"
    local consumer="${RUN_ID}_fetch_consumer"
    create_js_stream "$stream" "$subj"
    "$NATS_TOOL" pub --js --stream "$stream" --topic "$subj" --data "js-fetch-hello" --count 1 \
        > /dev/null 2>&1
    create_durable_pull_consumer "$stream" "$consumer"
    local out="${WORKDIR}/js_fetch.log"
    local pid
    # js_fetch's internal fetch expiry is ~5s; give it enough runway.
    pid=$(start_bg "$out" js_fetch --stream "$stream" --consumer "$consumer" --batch 5 --print)
    sleep 8
    stop_bg "$pid"
    grep -q "js-fetch-hello" "$out"
}

test_pubkv() {
    local bucket="${RUN_ID}_pubkv"
    create_kv_bucket "$bucket"
    printf 'k1|v1\nk2|v2\n' | timeout 5 "$NATS_TOOL" pubkv --bucket "$bucket" \
        > "${WORKDIR}/pubkv.log" 2>&1
    local out
    out=$(timeout 5 "$NATS_TOOL" kvkeys --bucket "$bucket" 2>&1)
    echo "$out" | grep -q "k1" && echo "$out" | grep -q "k2"
}

test_kv_create_update_keys() {
    local bucket="${RUN_ID}_kvcru"
    create_kv_bucket "$bucket"
    timeout 5 "$NATS_TOOL" kvcreate --bucket "$bucket" --key mykey --value v1 \
        > "${WORKDIR}/kvcreate.log" 2>&1 || return 1
    grep -q "revision=1" "${WORKDIR}/kvcreate.log" || return 1
    timeout 5 "$NATS_TOOL" kvupdate --bucket "$bucket" --key mykey --value v2 --revision 1 \
        > "${WORKDIR}/kvupdate.log" 2>&1 || return 1
    grep -q "revision=2" "${WORKDIR}/kvupdate.log" || return 1
    local keys
    keys=$(timeout 5 "$NATS_TOOL" kvkeys --bucket "$bucket" 2>&1)
    echo "$keys" | grep -q "mykey"
}

test_kv_history_revert_purge() {
    local bucket="${RUN_ID}_kvhrp"
    create_kv_bucket "$bucket"
    timeout 5 "$NATS_TOOL" kvcreate --bucket "$bucket" --key k --value v1 > /dev/null 2>&1 || return 1
    timeout 5 "$NATS_TOOL" kvupdate --bucket "$bucket" --key k --value v2 --revision 1 \
        > /dev/null 2>&1 || return 1

    local hist
    hist=$(timeout 10 "$NATS_TOOL" kvhistory --bucket "$bucket" --key k 2>&1)
    echo "$hist" | grep -q "rev=1" || { echo "kvhistory missing rev=1: $hist"; return 1; }
    echo "$hist" | grep -q "rev=2" || { echo "kvhistory missing rev=2: $hist"; return 1; }

    timeout 5 "$NATS_TOOL" kvrevert --bucket "$bucket" --key k --revision 1 \
        > "${WORKDIR}/kvrevert.log" 2>&1 || return 1
    grep -q "Reverted" "${WORKDIR}/kvrevert.log" || return 1

    timeout 5 "$NATS_TOOL" kvpurge --bucket "$bucket" --key k \
        > "${WORKDIR}/kvpurge.log" 2>&1 || return 1
    grep -q "Purged" "${WORKDIR}/kvpurge.log"
}

test_kvwatch() {
    local bucket="${RUN_ID}_kvwatch"
    create_kv_bucket "$bucket"
    local out="${WORKDIR}/kvwatch.log"
    local pid
    pid=$(start_bg "$out" kvwatch --bucket "$bucket" --print)
    sleep 1
    timeout 5 "$NATS_TOOL" kvcreate --bucket "$bucket" --key wk --value wv > /dev/null 2>&1
    sleep 1.5
    stop_bg "$pid"
    grep -q "wk" "$out"
}

# --- Concurrency stress -----------------------------------------------------
# nats_tool has a real history of threading bugs: a segfault from concurrent
# access to the connection's subscription map under --threads, and (fixed
# this session) the plain-publish path being spawned onto the raw io_context
# instead of the connection's strand when --io_shards was in play, letting
# conn->publish() run from a thread that didn't own the connection. Neither
# is reachable by any single-threaded/single-connection test above, so
# correctness here only means anything when this whole suite runs under
# ThreadSanitizer (see ci.yml) - that's what actually catches a reintroduced
# race; a clean exit here alone only proves nothing crashed outright.

test_threaded_plain_publish() {
    # Exercises the exact combination fixed this session: multiple io_context
    # shards + multiple threads for a non-JetStream (fire-and-forget) publish.
    local subj="${RUN_ID}.threaded_plain"
    local out="${WORKDIR}/threaded_grub.log"
    local pid
    pid=$(start_bg "$out" grub --topic "$subj" --print)
    sleep 1

    local count=500
    timeout 30 "$NATS_TOOL" pub --topic "$subj" --data "stress-msg" --count "$count" \
        --connections 4 --io_shards 4 --threads 4 > "${WORKDIR}/threaded_pub.log" 2>&1
    local pub_status=$?
    sleep 2
    stop_bg "$pid"

    if [[ $pub_status -ne 0 ]]; then
        echo "pub exited $pub_status"
        cat "${WORKDIR}/threaded_pub.log"
        return 1
    fi
    grep -q "All publishes complete" "${WORKDIR}/threaded_pub.log" || return 1

    # Core NATS pub/sub isn't guaranteed delivery, so tolerate a small gap
    # rather than requiring an exact count - the point is catching a race
    # that drops/corrupts most or all messages, not micro-counting.
    local received
    received=$(grep -c "stress-msg" "$out")
    local min_expected=$((count * 90 / 100))
    if [[ "$received" -lt "$min_expected" ]]; then
        echo "received $received/$count messages (expected >= $min_expected)"
        return 1
    fi
    return 0
}

test_threaded_js_publish() {
    # Exercises the original segfault-era path: multiple threads and
    # connections publishing to JetStream concurrently, which touches the
    # connection's subscription map and ack-tracking state from more than
    # one thread if the strand guards regress.
    local stream="${RUN_ID}_JS_STRESS"
    local subj="${RUN_ID}.threaded_js"
    create_js_stream "$stream" "$subj"

    local count=200
    timeout 30 "$NATS_TOOL" pub --js --stream "$stream" --topic "$subj" --data "stress-msg" \
        --count "$count" --connections 4 --threads 4 > "${WORKDIR}/threaded_js_pub.log" 2>&1
    local pub_status=$?

    if [[ $pub_status -ne 0 ]]; then
        echo "pub exited $pub_status"
        cat "${WORKDIR}/threaded_js_pub.log"
        return 1
    fi
    # JetStream acks are reliable (unlike core pub/sub), so this is an exact
    # check: every message must have been acked and none failed.
    grep -q "acked=${count}, failed=0" "${WORKDIR}/threaded_js_pub.log"
}

# --- Run ---------------------------------------------------------------------

run_test "grub+pub plain round trip" test_grub_pub
run_test "pub with headers" test_pub_headers
run_test "pub --input_format json preserves key order and resolves templates/fields" test_pub_input_format_json
run_test "req/reply round trip" test_req_reply
run_test "--translate shell injection safety" test_translate_shell_injection
run_test "grub --dump flushes buffered messages on SIGINT" test_dump_file_flushes_on_sigint
run_test "generator mode" test_generator
run_test "benchmarker mode" test_bench
run_test "batch_pub mode" test_batch_pub
run_test "batch_pub does not hang on unreachable server" test_batch_pub_unreachable_server_does_not_hang
run_test "JetStream publish" test_js_publish
run_test "js_grub push consumer" test_js_grub
run_test "js_fetch pull consumer" test_js_fetch
run_test "pubkv mode" test_pubkv
run_test "kvcreate/kvupdate/kvkeys" test_kv_create_update_keys
run_test "kvhistory/kvrevert/kvpurge" test_kv_history_revert_purge
run_test "kvwatch mode" test_kvwatch
run_test "threaded plain publish (--io_shards/--threads)" test_threaded_plain_publish
run_test "threaded JetStream publish (--connections/--threads)" test_threaded_js_publish

echo ""
echo "=== ${PASS_COUNT} passed, ${FAIL_COUNT} failed ==="
if [[ $FAIL_COUNT -gt 0 ]]; then
    echo "Failed: ${FAILED_TESTS[*]}"
    exit 1
fi
exit 0
