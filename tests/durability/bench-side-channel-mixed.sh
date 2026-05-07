#!/bin/bash
#
# Performance benchmark: Side Channel tail latency under main-thread contention
#
# This test demonstrates the side channel's advantage when the primary's main
# thread is busy with expensive commands. The side channel continues processing
# beacons on a separate thread, so durability progress isn't blocked.
#
# Approach:
#   1. Pre-populate a large list (100K elements) on the primary
#   2. Run a background client that periodically issues SORT on the large list
#      (expensive O(N*log(N)) command that blocks the main thread for ~ms)
#   3. Simultaneously run the SET benchmark
#   4. Compare p99/p99.9 tail latency between baseline and side channel
#
# In baseline: SORT blocks the main thread → REPLCONF ACK processing is delayed
#   → committed offset stalls → write responses are delayed → high tail latency
#
# In side channel: SORT blocks the main thread → but beacons still arrive at the
#   durability thread → committed offset advances → when main thread resumes,
#   clients are unblocked immediately → lower tail latency
#
# Usage: ./tests/durability/bench-side-channel-constrained.sh [requests] [clients]

set -e

REQUESTS=${1:-100000}
CLIENTS=${2:-50}

VALKEY_SERVER=./src/valkey-server
VALKEY_CLI=./src/valkey-cli
VALKEY_BENCHMARK=./src/valkey-benchmark

PRIMARY_PORT=7380
REPLICA1_PORT=7381
REPLICA2_PORT=7382

# --- Tunable config values ---
HZ=100
IO_THREADS=1                       # Single-threaded primary to maximize contention
BASELINE_REPL_ACK_PERIOD=1         # ms — fast ACK for baseline
SIDECHAN_REPL_ACK_PERIOD=1000      # ms — ACK offloaded to side channel
BEACON_INTERVAL_MS=1               # ms
SORT_LIST_SIZE=1000              # Elements in the list used for SORT
SORT_INTERVAL_MS=1000                # How often to issue SORT (ms)

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR; kill 0 2>/dev/null" EXIT

# --- CPU pinning ---
PRIMARY_CPUS="0-1"                 # Core 0 for main thread, core 1 for DT
REPLICA1_CPUS="2-3"
REPLICA2_CPUS="4-5"
BENCH_CPUS="6-15"

echo "============================================================"
echo "Side Channel: Tail Latency Under Main-Thread Contention"
echo "============================================================"
echo "Requests: $REQUESTS | Clients: $CLIENTS"
echo "Primary CPUs: $PRIMARY_CPUS (io-threads=$IO_THREADS)"
echo "SORT list size: $SORT_LIST_SIZE | SORT interval: ${SORT_INTERVAL_MS}ms"
echo ""

# Kill lingering servers
pkill -f "valkey-server.*--port $PRIMARY_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA1_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA2_PORT" 2>/dev/null || true
sleep 0.5

# --- Helper functions ---

start_primary() {
    local label=$1
    shift
    echo "  Starting primary ($label)..."
    taskset -c $PRIMARY_CPUS $VALKEY_SERVER \
        --port $PRIMARY_PORT \
        --cluster-enabled yes \
        --cluster-config-file "$TMPDIR/primary-nodes.conf" \
        --appendonly no \
        --sync-replication-enabled yes \
        --min-sync-replicas 2 \
        --io-threads $IO_THREADS \
        --hz $HZ \
        --save "" \
        --daemonize yes \
        --pidfile "$TMPDIR/primary.pid" \
        --logfile "$TMPDIR/primary.log" \
        --dir "$TMPDIR" \
        --dbfilename "primary.rdb" \
        "$@"
}

start_replica() {
    local num=$1
    local port=$2
    local cpus=$3
    local label=$4
    shift 4
    echo "  Starting replica$num ($label)..."
    taskset -c $cpus $VALKEY_SERVER \
        --port $port \
        --cluster-enabled yes \
        --cluster-config-file "$TMPDIR/replica${num}-nodes.conf" \
        --appendonly no \
        --sync-replication-enabled yes \
        --sync-eligible yes \
        --io-threads 1 \
        --hz $HZ \
        --save "" \
        --daemonize yes \
        --pidfile "$TMPDIR/replica${num}.pid" \
        --logfile "$TMPDIR/replica${num}.log" \
        --dir "$TMPDIR" \
        --dbfilename "replica${num}.rdb" \
        "$@"
}

wait_for_server() {
    local port=$1
    for i in $(seq 1 30); do
        if $VALKEY_CLI -p $port PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: Server on port $port did not start"
    exit 1
}

setup_cluster_replication() {
    echo "  Setting up cluster replication..."
    $VALKEY_CLI -p $PRIMARY_PORT CLUSTER MEET 127.0.0.1 $REPLICA1_PORT > /dev/null
    $VALKEY_CLI -p $PRIMARY_PORT CLUSTER MEET 127.0.0.1 $REPLICA2_PORT > /dev/null

    for i in $(seq 1 30); do
        local n1=$($VALKEY_CLI -p $REPLICA1_PORT CLUSTER NODES 2>/dev/null | wc -l)
        local n2=$($VALKEY_CLI -p $REPLICA2_PORT CLUSTER NODES 2>/dev/null | wc -l)
        if [ "$n1" -ge 3 ] && [ "$n2" -ge 3 ]; then break; fi
        sleep 0.2
    done

    $VALKEY_CLI -p $PRIMARY_PORT CLUSTER ADDSLOTSRANGE 0 16383 > /dev/null

    for i in $(seq 1 30); do
        if $VALKEY_CLI -p $PRIMARY_PORT CLUSTER INFO 2>/dev/null | grep -q "cluster_state:ok"; then break; fi
        sleep 0.2
    done

    local primary_id=$($VALKEY_CLI -p $PRIMARY_PORT CLUSTER MYID)
    $VALKEY_CLI -p $REPLICA1_PORT CLUSTER REPLICATE $primary_id > /dev/null
    $VALKEY_CLI -p $REPLICA2_PORT CLUSTER REPLICATE $primary_id > /dev/null

    for i in $(seq 1 50); do
        local online=$($VALKEY_CLI -p $PRIMARY_PORT INFO replication 2>/dev/null | grep -c "state=online" || true)
        if [ "$online" -ge 2 ]; then break; fi
        sleep 0.2
    done

    for i in $(seq 1 100); do
        local isr=$($VALKEY_CLI -p $PRIMARY_PORT INFO durability 2>/dev/null | grep durability_sync_replicas | tr -d '\r' | cut -d: -f2)
        if [ "$isr" -ge 2 ] 2>/dev/null; then break; fi
        sleep 0.2
    done
    echo "  Cluster ready (ISR count: $isr)"
}

populate_sort_list() {
    echo "  Populating sort list (${SORT_LIST_SIZE} elements)..."
    # Use pipeline to populate quickly
    local batch=1000
    for ((i=0; i<SORT_LIST_SIZE; i+=batch)); do
        local cmds=""
        local end=$((i + batch))
        if [ $end -gt $SORT_LIST_SIZE ]; then end=$SORT_LIST_SIZE; fi
        for ((j=i; j<end; j++)); do
            cmds="${cmds}RPUSH sortlist element_${j}\r\n"
        done
        printf "$cmds" | $VALKEY_CLI -p $PRIMARY_PORT --pipe > /dev/null 2>&1
    done
    local len=$($VALKEY_CLI -p $PRIMARY_PORT LLEN sortlist 2>/dev/null)
    echo "  Sort list populated: $len elements"
}

# Background SORT stress — issues SORT on the large list periodically
start_sort_stress() {
    echo "  Starting SORT stress (every ${SORT_INTERVAL_MS}ms)..."
    (
        while true; do
            $VALKEY_CLI -p $PRIMARY_PORT SORT sortlist LIMIT 0 10 > /dev/null 2>&1
            sleep $(echo "scale=3; $SORT_INTERVAL_MS/1000" | bc)
        done
    ) &
    SORT_PID=$!
}

stop_sort_stress() {
    if [ -n "$SORT_PID" ]; then
        kill $SORT_PID 2>/dev/null || true
        wait $SORT_PID 2>/dev/null || true
        SORT_PID=""
    fi
}

stop_servers() {
    stop_sort_stress
    echo "  Logs: $TMPDIR/primary.log"
    $VALKEY_CLI -p $PRIMARY_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    $VALKEY_CLI -p $REPLICA1_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    $VALKEY_CLI -p $REPLICA2_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 0.5
    pkill -f "valkey-server.*--port $PRIMARY_PORT" 2>/dev/null || true
    pkill -f "valkey-server.*--port $REPLICA1_PORT" 2>/dev/null || true
    pkill -f "valkey-server.*--port $REPLICA2_PORT" 2>/dev/null || true
    sleep 0.3
    rm -f "$TMPDIR"/*-nodes.conf "$TMPDIR"/*.rdb
    rm -rf "$TMPDIR"/appendonlydir
}

run_benchmark() {
    local label=$1
    local outfile=$2
    echo ""
    echo "  Running benchmark with SORT stress: $label"
    echo "  ---"
    taskset -c $BENCH_CPUS $VALKEY_BENCHMARK -p $PRIMARY_PORT \
        -c $CLIENTS -n $REQUESTS -r $REQUESTS -d 512 -t set --threads 8 | tee "$outfile"
    echo ""
    echo "  INFO CPU (primary):"
    $VALKEY_CLI -p $PRIMARY_PORT INFO cpu 2>/dev/null | grep -E "used_cpu|active_time"
    echo ""
    echo "  Commandstats:"
    $VALKEY_CLI -p $PRIMARY_PORT INFO commandstats 2>/dev/null | grep -E "replconf|sort" || true
    echo ""
}

# ============================================================
# BASELINE
# ============================================================

echo "------------------------------------------------------------"
echo "BASELINE: repl-ack-period=${BASELINE_REPL_ACK_PERIOD}ms + SORT stress"
echo "------------------------------------------------------------"

start_primary "baseline" --repl-ack-period $BASELINE_REPL_ACK_PERIOD
start_replica 1 $REPLICA1_PORT "$REPLICA1_CPUS" "baseline" --repl-ack-period $BASELINE_REPL_ACK_PERIOD
start_replica 2 $REPLICA2_PORT "$REPLICA2_CPUS" "baseline" --repl-ack-period $BASELINE_REPL_ACK_PERIOD

wait_for_server $PRIMARY_PORT
wait_for_server $REPLICA1_PORT
wait_for_server $REPLICA2_PORT

setup_cluster_replication
populate_sort_list
sleep 1

$VALKEY_CLI -p $PRIMARY_PORT CONFIG RESETSTAT > /dev/null

start_sort_stress
run_benchmark "Baseline (${BASELINE_REPL_ACK_PERIOD}ms ACK + SORT)" "$TMPDIR/baseline.txt"
stop_sort_stress

stop_servers

# ============================================================
# SIDE CHANNEL
# ============================================================

echo ""
echo "------------------------------------------------------------"
echo "SIDE CHANNEL: beacon=${BEACON_INTERVAL_MS}ms + SORT stress"
echo "  (ACK offloaded — main thread contention doesn't block durability)"
echo "------------------------------------------------------------"

start_primary "side-channel" \
    --durability-side-channel yes \
    --durability-beacon-interval-ms $BEACON_INTERVAL_MS \
    --repl-ack-period $SIDECHAN_REPL_ACK_PERIOD

start_replica 1 $REPLICA1_PORT "$REPLICA1_CPUS" "side-channel" \
    --durability-side-channel yes \
    --durability-beacon-interval-ms $BEACON_INTERVAL_MS \
    --repl-ack-period $SIDECHAN_REPL_ACK_PERIOD

start_replica 2 $REPLICA2_PORT "$REPLICA2_CPUS" "side-channel" \
    --durability-side-channel yes \
    --durability-beacon-interval-ms $BEACON_INTERVAL_MS \
    --repl-ack-period $SIDECHAN_REPL_ACK_PERIOD

wait_for_server $PRIMARY_PORT
wait_for_server $REPLICA1_PORT
wait_for_server $REPLICA2_PORT

setup_cluster_replication
populate_sort_list
sleep 2

$VALKEY_CLI -p $PRIMARY_PORT CONFIG RESETSTAT > /dev/null

start_sort_stress
run_benchmark "Side Channel (${BEACON_INTERVAL_MS}ms beacon + SORT)" "$TMPDIR/sidechan.txt"
stop_sort_stress

stop_servers

# ============================================================
# SUMMARY
# ============================================================

echo ""
echo "============================================================"
echo "RESULTS SUMMARY — Focus on p99 and p99.9 tail latency"
echo "============================================================"
echo ""
echo "--- Baseline (SORT blocks main thread → ACK delayed → writes stall) ---"
cat "$TMPDIR/baseline.txt"
echo ""
echo "--- Side Channel (SORT blocks main thread → but DT still advances offset) ---"
cat "$TMPDIR/sidechan.txt"
echo ""
echo "Expected: similar p50, but side channel has LOWER p99/p99.9"
echo "because durability progress continues even when main thread is busy."
echo "============================================================"
