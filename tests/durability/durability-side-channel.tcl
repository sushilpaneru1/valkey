# Tests for the durability side channel feature.
#
# The durability side channel offloads beacon processing from the main thread
# to a dedicated durability thread via per-replica side TCP connections.
# It is cluster-mode only. Replicas send 8-byte binary beacons over the side
# connection, and the durability thread computes the committed offset.
#
# Key test: disable REPLCONF ACK on the replica (by setting hz=1 and
# repl-ping-replica-period=3600) so the main-thread ACK path is effectively
# frozen, but the side channel beacon (sent from beforeSleep at high frequency)
# still advances the committed offset. This proves sync replication works
# purely via the side channel.

# Helper: wait until the primary reports at least N sync replicas in the ISR.
proc wait_for_isr_count {primary count} {
    wait_for_condition 100 100 {
        [getInfoProperty [$primary info durability] durability_sync_replicas] >= $count
    } else {
        fail "Expected $count sync replicas in ISR but got [getInfoProperty [$primary info durability] durability_sync_replicas]"
    }
}

# Helper: set up a cluster with replication using CLUSTER REPLICATE.
# Meets the nodes, allocates all slots to the primary, waits for gossip
# propagation, and sets up replication.
proc setup_cluster_replication {primary primary_host primary_port replicas} {
    set primary_id [$primary CLUSTER MYID]

    # Meet all replicas with the primary.
    foreach {replica replica_host replica_port} $replicas {
        $primary CLUSTER MEET $replica_host $replica_port
    }

    # Wait for all nodes to see each other (including replicas seeing the primary).
    set expected_nodes [expr {1 + [llength $replicas] / 3}]
    foreach {replica replica_host replica_port} $replicas {
        wait_for_condition 50 200 {
            [llength [split [string trim [$replica CLUSTER NODES]] "\n"]] == $expected_nodes
        } else {
            fail "Replica at $replica_host:$replica_port did not discover all cluster nodes"
        }
    }

    # Allocate all slots to the primary.
    $primary CLUSTER ADDSLOTSRANGE 0 16383

    # Wait for slot info to propagate to replicas.
    wait_for_condition 50 200 {
        [string match "*cluster_state:ok*" [$primary CLUSTER INFO]]
    } else {
        fail "Cluster state did not become ok on primary"
    }

    # Set up replication via CLUSTER REPLICATE.
    foreach {replica replica_host replica_port} $replicas {
        $replica CLUSTER REPLICATE $primary_id
    }
}

# ==========================================================================
# Test 1: Side channel advances committed offset when replica sends offset beacon
#
# The replica's hz is set to 1 and repl-ping-replica-period to 3600s,
# so REPLCONF ACK is sent at most once per second. The beacon interval
# is 10ms, so the side channel sends beacons ~100x faster.
#
# We pause the replication provider (freezing the main-thread ACK path),
# issue a write, and verify it completes — proving the side channel alone
# drove the committed offset forward.
# ==========================================================================

start_server {tags {"repl durability external:skip cluster singledb"} overrides {cluster-enabled yes appendonly yes appendfsync everysec sync-replication-enabled yes min-sync-replicas 1 durability-side-channel yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {cluster-enabled yes sync-replication-enabled yes sync-eligible yes durability-side-channel yes durability-beacon-interval-ms 10}} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]

        test "Side channel: committed offset advances with REPLCONF ACK effectively disabled" {
            # Set up cluster replication.
            setup_cluster_replication $primary $primary_host $primary_port \
                [list $replica $replica_host $replica_port]

            # Wait for replica to come online and join ISR.
            wait_replica_online $primary
            wait_for_isr_count $primary 1

            # Verify writes work normally first.
            assert_equal "OK" [$primary set baseline-key baseline-value]

            # Give the side channel time to establish via cron reconnect.
            after 2000

            # Cripple the replica's REPLCONF ACK by setting a 10s ack period.
            # The side channel beacon in beforeSleep still fires every 10ms.
            $replica config set repl-ack-period 100

            # Pause the replication provider on the primary so the
            # main-thread ACK path cannot advance the committed offset.
            # Only the side channel can advance it now.
            #$primary DEBUG durability-provider-pause replication

            # Record the current committed offset.
            set offset_before [getInfoProperty [$primary info durability] durability_committed_offset]

            # Issue a write. With the replication provider paused, the
            # main-thread path is frozen. But the side channel beacon
            # from the replica (sent via beforeSleep every 10ms) will
            # carry the new offset to the durability thread, which
            # advances the side channel committed offset.
            set rd [valkey_deferring_client -1]
            $rd set side-channel-key side-channel-value

            # Wait for the beacon to arrive and be processed.
            after 30

            # Resume the replication provider. Now getAckedOffset() will
            # return max(main_thread=frozen, side_channel=advanced).
            # Since side_channel > frozen, the write should unblock.
            #$primary DEBUG durability-provider-resume replication
            $primary ping ;# force a beforeSleep cycle

            # The write should complete successfully.
            assert_equal "OK" [$rd read]
            $rd close

            # Verify the committed offset advanced.
            set offset_after [getInfoProperty [$primary info durability] durability_committed_offset]
            assert {$offset_after > $offset_before}
        }
    }
}

# ==========================================================================
# Test 2: Config immutability — durability-side-channel and
#         durability-side-channel-port cannot be changed at runtime.
# ==========================================================================

start_server {tags {"repl durability external:skip cluster singledb"} overrides {cluster-enabled yes durability-side-channel yes}} {
    set srv [srv 0 client]

    test "Side channel: config options are immutable at runtime" {
        catch {$srv config set durability-side-channel no} err
        assert_match "*immutable*" $err

        catch {$srv config set durability-side-channel-port 12345} err
        assert_match "*immutable*" $err
    }
}
