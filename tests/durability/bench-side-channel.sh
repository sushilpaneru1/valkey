#!/bin/bash
#
# Performance benchmark: Durability Side Channel vs REPLCONF ACK
#
# Compares two configurations:
#   Baseline: sync-replication with 20ms repl-ack-period (main-thread ACK path)
#   Side Channel: sync-replication with 1s repl-ack-period + durability-side-channel + 20ms beacon interval
#
# Both use: 1 primary (min-sync-replicas=2) + 2 sync replicas, cluster-enabled, 2 IO threads
# Processes are pinned to specific CPU cores to avoid interference.
#
# Usage: ./tests/durability/bench-side-channel.sh [requests] [clients]
#   requests: number of requests per benchmark (default: 100000)
#   clients:  number of concurrent clients (default: 50)

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
HZ=10
IO_THREADS=1
BASELINE_REPL_ACK_PERIOD=0        # ms — baseline ACK frequency
SIDECHAN_REPL_ACK_PERIOD=1000      # ms — side channel test (ACK path effectively disabled)
BEACON_INTERVAL_MS=1             # ms — durability beacon send interval

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR; kill 0 2>/dev/null" EXIT

# --- CPU pinning setup ---
# Detect available cores and assign them to avoid interference.
# Layout with 32 cores:
#   Core 0-3:   Primary (main + 2 IO threads + durability thread)
#   Core 4-7:   Replica 1 (main + 2 IO threads)
#   Core 8-11:  Replica 2 (main + 2 IO threads)
#   Core 12-19: Benchmark client

NUM_CORES=$(nproc)
echo "Detected $NUM_CORES CPU cores"

if [ "$NUM_CORES" -ge 20 ]; then
    PRIMARY_CPUS="0-7"
    REPLICA1_CPUS="8-11"
    REPLICA2_CPUS="12-15"
    BENCH_CPUS="16-32"
elif [ "$NUM_CORES" -ge 12 ]; then
    PRIMARY_CPUS="0-5"
    REPLICA1_CPUS="6-8"
    REPLICA2_CPUS="9-11"
    BENCH_CPUS="0-3"
elif [ "$NUM_CORES" -ge 8 ]; then
    PRIMARY_CPUS="0-3"
    REPLICA1_CPUS="4-5"
    REPLICA2_CPUS="6-7"
    BENCH_CPUS="0-3"
else
    PRIMARY_CPUS=""
    REPLICA1_CPUS=""
    REPLICA2_CPUS=""
    BENCH_CPUS=""
fi

# Helper to prefix command with taskset if pinning is available
pin_primary() {
    if [ -n "$PRIMARY_CPUS" ]; then
        taskset -c $PRIMARY_CPUS "$@"
    else
        "$@"
    fi
}

pin_replica1() {
    if [ -n "$REPLICA1_CPUS" ]; then
        taskset -c $REPLICA1_CPUS "$@"
    else
        "$@"
    fi
}

pin_replica2() {
    if [ -n "$REPLICA2_CPUS" ]; then
        taskset -c $REPLICA2_CPUS "$@"
    else
        "$@"
    fi
}

pin_bench() {
    if [ -n "$BENCH_CPUS" ]; then
        taskset -c $BENCH_CPUS "$@"
    else
        "$@"
    fi
}

echo "============================================================"
echo "Durability Side Channel Performance Benchmark"
echo "============================================================"
echo "Requests: $REQUESTS | Clients: $CLIENTS | IO threads: 2"
echo "Primary port: $PRIMARY_PORT (CPUs: ${PRIMARY_CPUS:-any})"
echo "Replica1 port: $REPLICA1_PORT (CPUs: ${REPLICA1_CPUS:-any})"
echo "Replica2 port: $REPLICA2_PORT (CPUs: ${REPLICA2_CPUS:-any})"
echo "Benchmark CPUs: ${BENCH_CPUS:-any}"
echo "min-sync-replicas: 2"
echo ""

# Kill any lingering servers on our ports before starting.
pkill -f "valkey-server.*--port $PRIMARY_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA1_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA2_PORT" 2>/dev/null || true
sleep 0.5

# --- Helper functions ---

start_primary() {
    local label=$1
    shift
    echo "  Starting primary ($label)..."
    pin_primary $VALKEY_SERVER \
        --port $PRIMARY_PORT \
        --cluster-enabled yes \
        --cluster-config-file "$TMPDIR/primary-nodes.conf" \
        --appendonly no \
        --sync-replication-enabled yes \
        --min-sync-replicas 2 \
        --io-threads $IO_THREADS \
        --save "" \
        --daemonize yes \
        --pidfile "$TMPDIR/primary.pid" \
        --logfile "$TMPDIR/primary.log" \
        --dir "$TMPDIR" \
        --dbfilename "primary.rdb" \
        "$@"
}

start_replica1() {
    local label=$1
    shift
    echo "  Starting replica1 ($label)..."
    pin_replica1 $VALKEY_SERVER \
        --port $REPLICA1_PORT \
        --cluster-enabled yes \
        --cluster-config-file "$TMPDIR/replica1-nodes.conf" \
        --appendonly no \
        --sync-replication-enabled yes \
        --sync-eligible yes \
        --io-threads $IO_THREADS \
        --save "" \
        --daemonize yes \
        --pidfile "$TMPDIR/replica1.pid" \
        --logfile "$TMPDIR/replica1.log" \
        --dir "$TMPDIR" \
        --dbfilename "replica1.rdb" \
        "$@"
}

start_replica2() {
    local label=$1
    shift
    echo "  Starting replica2 ($label)..."
    pin_replica2 $VALKEY_SERVER \
        --port $REPLICA2_PORT \
        --cluster-enabled yes \
        --cluster-config-file "$TMPDIR/replica2-nodes.conf" \
        --appendonly no \
        --sync-replication-enabled yes \
        --sync-eligible yes \
        --io-threads $IO_THREADS \
        --save "" \
        --daemonize yes \
        --pidfile "$TMPDIR/replica2.pid" \
        --logfile "$TMPDIR/replica2.log" \
        --dir "$TMPDIR" \
        --dbfilename "replica2.rdb" \
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
    # Meet all nodes
    $VALKEY_CLI -p $PRIMARY_PORT CLUSTER MEET 127.0.0.1 $REPLICA1_PORT > /dev/null
    $VALKEY_CLI -p $PRIMARY_PORT CLUSTER MEET 127.0.0.1 $REPLICA2_PORT > /dev/null

    # Wait for all nodes to see each other
    for i in $(seq 1 30); do
        local nodes=$($VALKEY_CLI -p $REPLICA1_PORT CLUSTER NODES 2>/dev/null | wc -l)
        local nodes2=$($VALKEY_CLI -p $REPLICA2_PORT CLUSTER NODES 2>/dev/null | wc -l)
        if [ "$nodes" -ge 3 ] && [ "$nodes2" -ge 3 ]; then
            break
        fi
        sleep 0.2
    done

    # Allocate all slots to primary
    $VALKEY_CLI -p $PRIMARY_PORT CLUSTER ADDSLOTSRANGE 0 16383 > /dev/null

    # Wait for cluster ok
    for i in $(seq 1 30); do
        if $VALKEY_CLI -p $PRIMARY_PORT CLUSTER INFO 2>/dev/null | grep -q "cluster_state:ok"; then
            break
        fi
        sleep 0.2
    done

    # Set up replication
    local primary_id=$($VALKEY_CLI -p $PRIMARY_PORT CLUSTER MYID)
    $VALKEY_CLI -p $REPLICA1_PORT CLUSTER REPLICATE $primary_id > /dev/null
    $VALKEY_CLI -p $REPLICA2_PORT CLUSTER REPLICATE $primary_id > /dev/null

    # Wait for both replicas to come online
    for i in $(seq 1 50); do
        local online=$($VALKEY_CLI -p $PRIMARY_PORT INFO replication 2>/dev/null | grep -c "state=online" || true)
        if [ "$online" -ge 2 ]; then
            break
        fi
        sleep 0.2
    done

    # Wait for ISR count >= 2
    for i in $(seq 1 100); do
        local isr=$($VALKEY_CLI -p $PRIMARY_PORT INFO durability 2>/dev/null | grep durability_sync_replicas | tr -d '\r' | cut -d: -f2)
        if [ "$isr" -ge 2 ] 2>/dev/null; then
            break
        fi
        sleep 0.2
    done
    echo "  Cluster ready (ISR count: $isr)"
}

stop_servers() {
    echo "  Logs: $TMPDIR/primary.log, $TMPDIR/replica1.log, $TMPDIR/replica2.log"
    echo "  Stopping servers..."
    $VALKEY_CLI -p $PRIMARY_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    $VALKEY_CLI -p $REPLICA1_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    $VALKEY_CLI -p $REPLICA2_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 2
    # Force kill if shutdown didn't work
    pkill -f "valkey-server.*--port $PRIMARY_PORT" 2>/dev/null || true
    pkill -f "valkey-server.*--port $REPLICA1_PORT" 2>/dev/null || true
    pkill -f "valkey-server.*--port $REPLICA2_PORT" 2>/dev/null || true
    sleep 2
    rm -f "$TMPDIR"/*-nodes.conf "$TMPDIR"/*.rdb
    rm -rf "$TMPDIR"/appendonlydir
}

run_benchmark() {
    local label=$1
    local outfile=$2
    echo ""
    echo "  Running benchmark: $label"
    echo "  ---"
    pin_bench $VALKEY_BENCHMARK -p $PRIMARY_PORT -c $CLIENTS -n $REQUESTS -r 100000000 -P 1 -d 512 -t set --threads 16 | tee "$outfile"
    echo ""
}

# --- Baseline: sync replication with 20ms repl-ack-period ---

echo "------------------------------------------------------------"
echo "BASELINE: sync-replication + repl-ack-period ${BASELINE_REPL_ACK_PERIOD}ms"
echo "------------------------------------------------------------"

start_primary "baseline" --repl-ack-period $BASELINE_REPL_ACK_PERIOD --hz $HZ
start_replica1 "baseline" --repl-ack-period $BASELINE_REPL_ACK_PERIOD --hz $HZ
start_replica2 "baseline" --repl-ack-period $BASELINE_REPL_ACK_PERIOD --hz $HZ

wait_for_server $PRIMARY_PORT
wait_for_server $REPLICA1_PORT
wait_for_server $REPLICA2_PORT

setup_cluster_replication
echo "  Primary port: $PRIMARY_PORT"

# Give the system a moment to stabilize
sleep 1

run_benchmark "Baseline (${BASELINE_REPL_ACK_PERIOD}ms repl-ack-period)" "$TMPDIR/baseline_bench.txt"
# read -p "Pause....."
stop_servers

echo ""
echo "------------------------------------------------------------"
echo "SIDE CHANNEL: durability-side-channel + ${BEACON_INTERVAL_MS}ms beacon interval"
echo "             (repl-ack-period = ${SIDECHAN_REPL_ACK_PERIOD}ms, ACK path is slow)"
echo "------------------------------------------------------------"

start_primary "side-channel" \
    --durability-side-channel yes \
    --durability-beacon-interval-ms $BEACON_INTERVAL_MS \
    --repl-ack-period $SIDECHAN_REPL_ACK_PERIOD \
    --hz $HZ

start_replica1 "side-channel" \
    --durability-side-channel yes \
    --durability-beacon-interval-ms $BEACON_INTERVAL_MS \
    --repl-ack-period $SIDECHAN_REPL_ACK_PERIOD \
    --hz $HZ

start_replica2 "side-channel" \
    --durability-side-channel yes \
    --durability-beacon-interval-ms $BEACON_INTERVAL_MS \
    --repl-ack-period $SIDECHAN_REPL_ACK_PERIOD \
    --hz $HZ

wait_for_server $PRIMARY_PORT
wait_for_server $REPLICA1_PORT
wait_for_server $REPLICA2_PORT

setup_cluster_replication

# Wait for side channel to establish
sleep 2

run_benchmark "Side Channel (${BEACON_INTERVAL_MS}ms beacon, ${SIDECHAN_REPL_ACK_PERIOD}ms repl-ack-period)" "$TMPDIR/sidechan_bench.txt"

read -p "Pause....."

# Verify data consistency: all nodes should have the same number of keys
sleep 1  # Let replication catch up
echo ""
echo "  Data consistency check:"
echo "    Primary DBSIZE:  $($VALKEY_CLI -p $PRIMARY_PORT DBSIZE 2>/dev/null)"
echo "    Replica1 DBSIZE: $($VALKEY_CLI -p $REPLICA1_PORT DBSIZE 2>/dev/null)"
echo "    Replica2 DBSIZE: $($VALKEY_CLI -p $REPLICA2_PORT DBSIZE 2>/dev/null)"
echo "    Expected keys:   $REQUESTS"
echo ""
echo "    Primary KEYS *:"
$VALKEY_CLI -p $PRIMARY_PORT KEYS '*' 2>/dev/null | head -20
echo "    ..."
echo ""
echo "    INFO durability (primary):"
$VALKEY_CLI -p $PRIMARY_PORT INFO durability 2>/dev/null
echo ""

stop_servers

# --- Summary ---

echo ""
echo "============================================================"
echo "RESULTS SUMMARY"
echo "============================================================"
echo ""
echo "--- Baseline (${BASELINE_REPL_ACK_PERIOD}ms repl-ack-period) ---"
cat "$TMPDIR/baseline_bench.txt"
echo "$TMPDIR/baseline_bench.txt"
echo ""
echo "--- Side Channel (${BEACON_INTERVAL_MS}ms beacon, ${SIDECHAN_REPL_ACK_PERIOD}ms ACK) ---"
cat "$TMPDIR/sidechan_bench.txt"
echo ""
echo "============================================================"
