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

# --- Run ---------------------------------------------------------------------

run_test "grub+pub plain round trip" test_grub_pub
run_test "pub with headers" test_pub_headers
run_test "req/reply round trip" test_req_reply
run_test "generator mode" test_generator
run_test "benchmarker mode" test_bench
run_test "batch_pub mode" test_batch_pub
run_test "JetStream publish" test_js_publish
run_test "js_grub push consumer" test_js_grub
run_test "js_fetch pull consumer" test_js_fetch
run_test "pubkv mode" test_pubkv
run_test "kvcreate/kvupdate/kvkeys" test_kv_create_update_keys
run_test "kvhistory/kvrevert/kvpurge" test_kv_history_revert_purge
run_test "kvwatch mode" test_kvwatch

echo ""
echo "=== ${PASS_COUNT} passed, ${FAIL_COUNT} failed ==="
if [[ $FAIL_COUNT -gt 0 ]]; then
    echo "Failed: ${FAILED_TESTS[*]}"
    exit 1
fi
exit 0
