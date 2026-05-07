#!/bin/bash
#
# Performance benchmark: 1 Primary + 2 Replicas (no sync-replication)
#
# This establishes the throughput ceiling for a Valkey cluster shard with
# standard async replication (sync-replication disabled). Same CPU pinning,
# IO threads, and config style as bench-side-channel.sh.
#
# Usage: ./tests/durability/bench-single-node.sh [requests] [clients]
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

IO_THREADS=1
HZ=100
SORT_LIST_SIZE=1000
SORT_INTERVAL_MS=1000

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR; kill 0 2>/dev/null" EXIT

# --- CPU pinning (matches bench-side-channel-constrained.sh) ---
PRIMARY_CPUS="0-1"
REPLICA1_CPUS="2-3"
REPLICA2_CPUS="4-5"
BENCH_CPUS="6-15"

echo "============================================================"
echo "1 Primary + 2 Replicas Benchmark (async replication, no sync-replication)"
echo "============================================================"
echo "Requests: $REQUESTS | Clients: $CLIENTS | IO threads: $IO_THREADS"
echo "Primary port: $PRIMARY_PORT (CPUs: ${PRIMARY_CPUS:-any})"
echo "Replica1 port: $REPLICA1_PORT (CPUs: ${REPLICA1_CPUS:-any})"
echo "Replica2 port: $REPLICA2_PORT (CPUs: ${REPLICA2_CPUS:-any})"
echo "Benchmark CPUs: ${BENCH_CPUS:-any}"
echo "SORT list size: $SORT_LIST_SIZE | SORT interval: ${SORT_INTERVAL_MS}ms"
echo ""

# Kill any lingering servers on our ports.
pkill -f "valkey-server.*--port $PRIMARY_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA1_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA2_PORT" 2>/dev/null || true
sleep 0.5

# --- Start servers ---

echo "  Starting primary..."
taskset -c $PRIMARY_CPUS $VALKEY_SERVER \
    --port $PRIMARY_PORT \
    --cluster-enabled yes \
    --cluster-config-file "$TMPDIR/primary-nodes.conf" \
    --appendonly no \
    --io-threads $IO_THREADS \
    --hz $HZ \
    --save "" \
    --daemonize yes \
    --pidfile "$TMPDIR/primary.pid" \
    --logfile "$TMPDIR/primary.log" \
    --dir "$TMPDIR" \
    --dbfilename "primary.rdb"

echo "  Starting replica1..."
taskset -c $REPLICA1_CPUS $VALKEY_SERVER \
    --port $REPLICA1_PORT \
    --cluster-enabled yes \
    --cluster-config-file "$TMPDIR/replica1-nodes.conf" \
    --appendonly no \
    --io-threads 1 \
    --hz $HZ \
    --save "" \
    --daemonize yes \
    --pidfile "$TMPDIR/replica1.pid" \
    --logfile "$TMPDIR/replica1.log" \
    --dir "$TMPDIR" \
    --dbfilename "replica1.rdb"

echo "  Starting replica2..."
taskset -c $REPLICA2_CPUS $VALKEY_SERVER \
    --port $REPLICA2_PORT \
    --cluster-enabled yes \
    --cluster-config-file "$TMPDIR/replica2-nodes.conf" \
    --appendonly no \
    --io-threads 1 \
    --hz $HZ \
    --save "" \
    --daemonize yes \
    --pidfile "$TMPDIR/replica2.pid" \
    --logfile "$TMPDIR/replica2.log" \
    --dir "$TMPDIR" \
    --dbfilename "replica2.rdb"

# Wait for all servers to start
for port in $PRIMARY_PORT $REPLICA1_PORT $REPLICA2_PORT; do
    for i in $(seq 1 30); do
        if $VALKEY_CLI -p $port PING 2>/dev/null | grep -q PONG; then
            break
        fi
        sleep 0.1
    done
done

# --- Set up cluster replication ---

echo "  Setting up cluster replication..."

# Meet all nodes
$VALKEY_CLI -p $PRIMARY_PORT CLUSTER MEET 127.0.0.1 $REPLICA1_PORT > /dev/null
$VALKEY_CLI -p $PRIMARY_PORT CLUSTER MEET 127.0.0.1 $REPLICA2_PORT > /dev/null

# Wait for all nodes to see each other
for i in $(seq 1 30); do
    nodes=$($VALKEY_CLI -p $REPLICA1_PORT CLUSTER NODES 2>/dev/null | wc -l)
    nodes2=$($VALKEY_CLI -p $REPLICA2_PORT CLUSTER NODES 2>/dev/null | wc -l)
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
PRIMARY_ID=$($VALKEY_CLI -p $PRIMARY_PORT CLUSTER MYID)
$VALKEY_CLI -p $REPLICA1_PORT CLUSTER REPLICATE $PRIMARY_ID > /dev/null
$VALKEY_CLI -p $REPLICA2_PORT CLUSTER REPLICATE $PRIMARY_ID > /dev/null

# Wait for both replicas to come online
for i in $(seq 1 50); do
    online=$($VALKEY_CLI -p $PRIMARY_PORT INFO replication 2>/dev/null | grep -c "state=online" || true)
    if [ "$online" -ge 2 ]; then
        break
    fi
    sleep 0.2
done

echo "  Cluster ready (primary + 2 replicas, async replication)"

# --- Populate sort list ---

populate_sort_list() {
    echo "  Populating sort list (${SORT_LIST_SIZE} elements)..."
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

populate_sort_list
sleep 1

$VALKEY_CLI -p $PRIMARY_PORT CONFIG RESETSTAT > /dev/null

# --- Run benchmark with SORT stress ---

start_sort_stress

echo ""
echo "  Running benchmark: SET with SORT stress (1 primary + 2 replicas, no sync-replication)"
echo "  ---"
taskset -c $BENCH_CPUS $VALKEY_BENCHMARK -p $PRIMARY_PORT -c $CLIENTS -n $REQUESTS -r $REQUESTS -d 512 -t set --threads 8 | tee "$TMPDIR/bench.txt"
echo ""

stop_sort_stress

# Data consistency check
sleep 1
echo "  Data consistency check:"
echo "    Primary DBSIZE:  $($VALKEY_CLI -p $PRIMARY_PORT DBSIZE 2>/dev/null)"
echo "    Replica1 DBSIZE: $($VALKEY_CLI -p $REPLICA1_PORT DBSIZE 2>/dev/null)"
echo "    Replica2 DBSIZE: $($VALKEY_CLI -p $REPLICA2_PORT DBSIZE 2>/dev/null)"
echo ""
echo "  INFO CPU (primary):"
$VALKEY_CLI -p $PRIMARY_PORT INFO cpu 2>/dev/null | grep -E "used_cpu|active_time"
echo ""
echo "  Commandstats:"
$VALKEY_CLI -p $PRIMARY_PORT INFO commandstats 2>/dev/null | grep -E "replconf|sort" || true
echo ""

# --- Cleanup ---

echo "  Logs: $TMPDIR/primary.log, $TMPDIR/replica1.log, $TMPDIR/replica2.log"
echo "  Stopping servers..."
$VALKEY_CLI -p $PRIMARY_PORT SHUTDOWN NOSAVE 2>/dev/null || true
$VALKEY_CLI -p $REPLICA1_PORT SHUTDOWN NOSAVE 2>/dev/null || true
$VALKEY_CLI -p $REPLICA2_PORT SHUTDOWN NOSAVE 2>/dev/null || true
sleep 0.5
pkill -f "valkey-server.*--port $PRIMARY_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA1_PORT" 2>/dev/null || true
pkill -f "valkey-server.*--port $REPLICA2_PORT" 2>/dev/null || true

# --- Summary ---

echo ""
echo "============================================================"
echo "RESULT"
echo "============================================================"
echo ""
cat "$TMPDIR/bench.txt"
echo ""
echo "This is the throughput ceiling with replication overhead"
echo "but no durability blocking (sync-replication disabled)."
echo "============================================================"
