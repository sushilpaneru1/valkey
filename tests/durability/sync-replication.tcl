# Tests for replication-based durability provider (sync replication).
#
# These tests validate the sync replication feature where writes are only
# considered committed once acknowledged by a configured number of sync
# replicas (ISR members).
#
# All tests run in cluster mode (single-shard). We use appendfsync=everysec
# so the AOF durability provider is DISABLED, isolating the replication
# provider behavior. The replication provider is enabled when
# min-sync-replicas > 0.
#
# Each test is run twice: once without the durability side channel (plain
# REPLCONF ACK path) and once with the side channel enabled (beacon-based
# offset reporting via a dedicated thread).

# Helper: wait until the primary reports at least N sync replicas in the ISR
# by polling INFO durability for durability_sync_replicas.
proc wait_for_isr_count {primary count} {
    wait_for_condition 100 100 {
        [getInfoProperty [$primary info durability] durability_sync_replicas] >= $count
    } else {
        fail "Expected $count sync replicas in ISR but got [getInfoProperty [$primary info durability] durability_sync_replicas]"
    }
}

# Helper: return the current write_blocked_count from INFO durability.
proc get_write_blocked_count {primary} {
    getInfoProperty [$primary info durability] durability_write_blocked_count
}

# Helper: set up cluster replication between a primary and one or more replicas.
# Uses CLUSTER MEET + CLUSTER ADDSLOTSRANGE + CLUSTER REPLICATE.
# replicas is a flat list: {client host port client host port ...}
proc setup_replication {primary primary_host primary_port replicas} {
    set primary_id [$primary CLUSTER MYID]

    # Meet all replicas with the primary.
    foreach {replica rhost rport} $replicas {
        $primary CLUSTER MEET $rhost $rport
    }

    # Wait for all nodes to see each other.
    set expected_nodes [expr {1 + [llength $replicas] / 3}]
    foreach {replica rhost rport} $replicas {
        wait_for_condition 50 200 {
            [llength [split [string trim [$replica CLUSTER NODES]] "\n"]] == $expected_nodes
        } else {
            fail "Replica at $rhost:$rport did not discover all cluster nodes"
        }
    }

    # Allocate all slots to the primary.
    $primary CLUSTER ADDSLOTSRANGE 0 16383

    # Wait for cluster state to become ok.
    wait_for_condition 50 200 {
        [string match "*cluster_state:ok*" [$primary CLUSTER INFO]]
    } else {
        fail "Cluster state did not become ok on primary"
    }

    # Set up replication via CLUSTER REPLICATE.
    foreach {replica rhost rport} $replicas {
        $replica CLUSTER REPLICATE $primary_id
    }
}

# Helper: tear down replication for replicas.
# In cluster mode, REPLICAOF is not allowed. We use CLUSTER RESET to
# remove the replication relationship.
proc teardown_replication {replicas} {
    foreach {replica rhost rport} $replicas {
        catch {$replica CLUSTER RESET HARD}
    }
}

# Run all tests with and without the durability side channel.
foreach side_channel {0 1} {

if {$side_channel} {
    set sc_label "side-channel"
    set sc_primary_overrides {appendonly yes appendfsync everysec sync-replication-enabled yes cluster-enabled yes cluster-databases 16 durability-side-channel yes}
    set sc_sync_replica_overrides {sync-replication-enabled yes sync-eligible yes cluster-enabled yes cluster-databases 16 durability-side-channel yes durability-beacon-interval-ms 10}
    set sc_nonsync_replica_overrides {sync-replication-enabled yes sync-eligible no cluster-enabled yes cluster-databases 16 durability-side-channel yes}
} else {
    set sc_label "main-thread"
    set sc_primary_overrides {appendonly yes appendfsync everysec sync-replication-enabled yes cluster-enabled yes cluster-databases 16}
    set sc_sync_replica_overrides {sync-replication-enabled yes sync-eligible yes cluster-enabled yes cluster-databases 16}
    set sc_nonsync_replica_overrides {sync-replication-enabled yes sync-eligible no cluster-enabled yes cluster-databases 16}
}

set sc_tags "repl durability external:skip cluster singledb"

# ==========================================================================
# Test 1: If number of sync replicas < min-sync-replicas, primary rejects
#         writes with CLUSTERDOWN.
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 2}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server [list overrides $sc_sync_replica_overrides] {
        set replica1 [srv 0 client]
        set replica1_host [srv 0 host]
        set replica1_port [srv 0 port]

        start_server [list overrides $sc_sync_replica_overrides] {
            set replica2 [srv 0 client]
            set replica2_host [srv 0 host]
            set replica2_port [srv 0 port]

            test "Sync replication ($sc_label): write rejected when ISR count < min-sync-replicas" {
                # Connect only replica1 — ISR will have 1 member
                setup_replication $primary $primary_host $primary_port \
                    [list $replica1 $replica1_host $replica1_port]
                wait_replica_online $primary
                wait_for_isr_count $primary 1

                # Write must be rejected — only 1 of 2 required sync replicas
                catch {$primary set mykey myvalue} err
                assert_match "*CLUSTERDOWN*" $err

                # Connect replica2 so ISR reaches 2
                set primary_id [$primary CLUSTER MYID]
                $primary CLUSTER MEET $replica2_host $replica2_port
                wait_for_condition 50 200 {
                    [llength [split [string trim [$replica2 CLUSTER NODES]] "\n"]] == 3
                } else {
                    fail "Replica2 did not discover all cluster nodes"
                }
                $replica2 CLUSTER REPLICATE $primary_id
                wait_replica_online $primary
                wait_for_isr_count $primary 2

                # Write should succeed now
                assert_equal "OK" [$primary set mykey myvalue]

                # Cleanup
                teardown_replication [list $replica1 $replica1_host $replica1_port \
                    $replica2 $replica2_host $replica2_port]
            }
        }
    }
}

# ==========================================================================
# Test 2: Primary connected to 2 sync replicas. Write is released to client
#         only when both sync replicas ack back.
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 2}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server [list overrides $sc_sync_replica_overrides] {
        set replica1 [srv 0 client]
        set replica1_host [srv 0 host]
        set replica1_port [srv 0 port]

        start_server [list overrides $sc_sync_replica_overrides] {
            set replica2 [srv 0 client]
            set replica2_host [srv 0 host]
            set replica2_port [srv 0 port]

            test "Sync replication ($sc_label): write released only after both sync replicas ack" {
                # Connect both replicas and let them sync
                setup_replication $primary $primary_host $primary_port \
                    [list $replica1 $replica1_host $replica1_port \
                          $replica2 $replica2_host $replica2_port]
                wait_replica_online $primary
                wait_for_isr_count $primary 2

                # Pause the replication provider so acks don't advance consensus
                $primary DEBUG durability-provider-pause replication

                set blocked_before [get_write_blocked_count $primary]

                # Issue a write via deferring client — reply should be held
                set rd [valkey_deferring_client -2]
                $rd set mykey myvalue

                # Wait for the write to be blocked
                wait_for_condition 50 100 {
                    [get_write_blocked_count $primary] > $blocked_before
                } else {
                    fail "Write was not blocked by durability provider"
                }

                # Resume the replication provider — replicas have already acked,
                # so consensus advances and the reply is released
                $primary DEBUG durability-provider-resume replication
                $primary ping ;# force a beforeSleep cycle

                assert_equal "OK" [$rd read]
                $rd close

                # Cleanup
                teardown_replication [list $replica1 $replica1_host $replica1_port \
                    $replica2 $replica2_host $replica2_port]
            }
        }
    }
}

# ==========================================================================
# Test 3: Primary connected to 2 sync replicas. Write is blocked if one
#         replica lags behind (paused with SIGSTOP).
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 2}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server [list overrides $sc_sync_replica_overrides] {
        set replica1 [srv 0 client]
        set replica1_host [srv 0 host]
        set replica1_port [srv 0 port]

        start_server [list overrides $sc_sync_replica_overrides] {
            set replica2 [srv 0 client]
            set replica2_host [srv 0 host]
            set replica2_port [srv 0 port]

            test "Sync replication ($sc_label): write blocked when one replica lags behind" {
                # Connect both replicas and let them sync
                setup_replication $primary $primary_host $primary_port \
                    [list $replica1 $replica1_host $replica1_port \
                          $replica2 $replica2_host $replica2_port]
                wait_replica_online $primary
                wait_for_isr_count $primary 2

                # Verify writes work when both replicas are healthy
                assert_equal "OK" [$primary set healthy-key healthy-value]

                # Pause replica2 at the OS level (SIGSTOP) so it stops
                # sending ACKs. Its ack offset is frozen, preventing
                # consensus from advancing past new writes.
                set replica2_pid [srv 0 pid]
                pause_process $replica2_pid

                set blocked_before [get_write_blocked_count $primary]

                # Issue a write via deferring client — replica1 will ack
                # but replica2 cannot, so min_offset stays behind.
                set rd [valkey_deferring_client -2]
                $rd set blocked-key blocked-value

                # Verify the write was blocked
                wait_for_condition 50 100 {
                    [get_write_blocked_count $primary] > $blocked_before
                } else {
                    fail "First write was not blocked"
                }

                set blocked_before2 [get_write_blocked_count $primary]

                # Issue another write — also blocked
                $rd set blocked-key2 blocked-value2

                wait_for_condition 50 100 {
                    [get_write_blocked_count $primary] > $blocked_before2
                } else {
                    fail "Second write was not blocked"
                }

                # Resume replica2 — it catches up and acks, both writes unblock
                resume_process $replica2_pid

                assert_equal "OK" [$rd read]
                assert_equal "OK" [$rd read]
                $rd close

                # Cleanup
                teardown_replication [list $replica1 $replica1_host $replica1_port \
                    $replica2 $replica2_host $replica2_port]
            }
        }
    }
}

# ==========================================================================
# Test 4: Replica killed — writes rejected, replica restarts — writes resume.
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 1}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server [list overrides $sc_sync_replica_overrides] {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]

        test "Sync replication ($sc_label): replica killed — writes rejected then resume after restart" {
            # Use a short repl-timeout so the primary detects the dead
            # replica quickly.
            $primary config set repl-timeout 3

            setup_replication $primary $primary_host $primary_port \
                [list $replica $replica_host $replica_port]
            wait_replica_online $primary
            wait_for_isr_count $primary 1

            # Writes should succeed with a healthy replica
            assert_equal "OK" [$primary set key1 value1]

            # Kill the replica
            catch {$replica shutdown nosave}

            # Wait for the primary to disconnect the dead replica
            wait_for_condition 50 200 {
                [s -1 connected_slaves] == 0
            } else {
                fail "Primary did not disconnect the dead replica"
            }

            # Writes must now be rejected — no replicas in ISR
            catch {$primary set key2 value2} err
            assert_match "*CLUSTERDOWN*" $err

            # Restart the replica — in cluster mode it remembers its
            # cluster state and reconnects automatically.
            restart_server 0 true false

            set replica [srv 0 client]
            wait_replica_online $primary
            wait_for_isr_count $primary 1

            # Writes should succeed again
            assert_equal "OK" [$primary set key3 value3]

            # Cleanup
            $primary config set repl-timeout 60
            teardown_replication [list $replica $replica_host $replica_port]
        }
    }
}

# ==========================================================================
# Test 5: Replica paused (SIGSTOP) — writes rejected after ISR timeout,
#         replica resumed — writes accepted again.
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 1}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server [list overrides $sc_sync_replica_overrides] {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_pid [srv 0 pid]

        test "Sync replication ($sc_label): replica paused — writes rejected then resume after SIGCONT" {
            # Use a repl-timeout longer than the ISR timeout so the
            # replica is removed from the ISR but NOT disconnected.
            # ISR timeout is 10 s, so set repl-timeout to 20 s.
            $primary config set repl-timeout 20

            setup_replication $primary $primary_host $primary_port \
                [list $replica $replica_host $replica_port]
            wait_replica_online $primary
            wait_for_isr_count $primary 1

            # Writes should succeed with a healthy replica
            assert_equal "OK" [$primary set key1 value1]

            # Pause the replica — it stays connected but stops ACKing
            pause_process $replica_pid

            # Wait for the ISR timeout to remove the replica from ISR.
            wait_for_condition 150 200 {
                [getInfoProperty [$primary info durability] durability_sync_replicas] == 0
            } else {
                fail "Replica was not removed from ISR after timeout"
            }

            # The replica is still connected (not timed out by
            # repl-timeout which is 20 s) but removed from ISR.
            assert_equal 1 [s -1 connected_slaves]

            # Writes must now be rejected — 0 ISR members
            catch {$primary set key2 value2} err
            assert_match "*CLUSTERDOWN*" $err

            # Resume the replica — it catches up and rejoins the ISR
            resume_process $replica_pid
            wait_for_isr_count $primary 1

            # Writes should succeed again
            set rd [valkey_deferring_client -1]
            $rd set key3 value3
            assert_equal "OK" [$rd read]
            $rd close

            # Cleanup
            $primary config set repl-timeout 60
            teardown_replication [list $replica $replica_host $replica_port]
        }
    }
}

# ==========================================================================
# Test 6: Consensus offset advances based on sync replica only, not
#         regular (non-sync) replicas.
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 1}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Sync replica
    start_server [list overrides $sc_sync_replica_overrides] {
        set sync_replica [srv 0 client]
        set sync_replica_host [srv 0 host]
        set sync_replica_port [srv 0 port]

        # Regular (non-sync) replica
        start_server [list overrides $sc_nonsync_replica_overrides] {
            set regular_replica [srv 0 client]
            set regular_replica_host [srv 0 host]
            set regular_replica_port [srv 0 port]
            set regular_replica_pid [srv 0 pid]

            test "Sync replication ($sc_label): committed offset advances based on sync replica, not regular replica" {
                # Connect both replicas
                setup_replication $primary $primary_host $primary_port \
                    [list $sync_replica $sync_replica_host $sync_replica_port \
                          $regular_replica $regular_replica_host $regular_replica_port]
                wait_replica_online $primary
                wait_for_isr_count $primary 1

                # Verify writes succeed with both replicas healthy
                assert_equal "OK" [$primary set key1 value1]

                # Record the committed offset after the first write.
                set offset_before [getInfoProperty [$primary info durability] durability_committed_offset]
                assert {$offset_before > 0}

                # Pause the regular (non-sync) replica
                pause_process $regular_replica_pid

                # Issue more writes — they should succeed because the
                # sync replica is still healthy and ACKing.
                assert_equal "OK" [$primary set key2 value2]
                assert_equal "OK" [$primary set key3 value3]

                # Verify the committed offset has advanced, proving
                # consensus is driven by the sync replica alone.
                set offset_after [getInfoProperty [$primary info durability] durability_committed_offset]
                assert {$offset_after > $offset_before}

                # Verify the primary still sees 2 connected replicas
                # (the regular one is paused but TCP connection is alive)
                assert_equal 2 [status $primary connected_slaves]

                # Resume the regular replica
                resume_process $regular_replica_pid

                # One more write to confirm everything is still healthy
                assert_equal "OK" [$primary set key4 value4]

                set offset_final [getInfoProperty [$primary info durability] durability_committed_offset]
                assert {$offset_final > $offset_after}

                # Cleanup
                teardown_replication [list $sync_replica $sync_replica_host $sync_replica_port \
                    $regular_replica $regular_replica_host $regular_replica_port]
            }
        }
    }
}

# ==========================================================================
# Test 7: [WBL] Replica blocks reads on uncommitted keys until REPLCONF COMMIT
#         arrives from the primary.
# ==========================================================================

start_server [list tags [list $sc_tags] overrides [concat $sc_primary_overrides {min-sync-replicas 2}]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server [list overrides $sc_sync_replica_overrides] {
        set replica1 [srv 0 client]
        set replica1_host [srv 0 host]
        set replica1_port [srv 0 port]

        start_server [list overrides $sc_sync_replica_overrides] {
            set replica2 [srv 0 client]
            set replica2_host [srv 0 host]
            set replica2_port [srv 0 port]

            test "Sync replication ($sc_label): replica blocks read on uncommitted key until REPLCONF COMMIT" {
                # Connect both replicas and wait for ISR
                setup_replication $primary $primary_host $primary_port \
                    [list $replica1 $replica1_host $replica1_port \
                          $replica2 $replica2_host $replica2_port]
                wait_replica_online $primary
                wait_for_isr_count $primary 2

                # Enable READONLY on replicas so reads are served locally
                # instead of being redirected with MOVED.
                $replica1 READONLY

                # Verify the system is healthy — a write succeeds end-to-end
                assert_equal "OK" [$primary set committed-key committed-value]

                # Verify the replica can read the committed key
                wait_for_condition 50 100 {
                    [$replica1 get committed-key] eq "committed-value"
                } else {
                    fail "Committed key did not replicate to replica"
                }

                # Pause the replication provider on the primary.
                # This freezes the committed offset — new writes will
                # replicate to replicas but REPLCONF COMMIT won't advance.
                $primary DEBUG durability-provider-pause replication

                # Write a key on the primary via a deferring client.
                set writer [valkey_deferring_client -2]
                $writer set uncommitted-key uncommitted-value

                # Wait for the write to replicate to the replica
                wait_for_condition 50 100 {
                    [getInfoProperty [$replica1 info durability] durability_uncommitted_keys] > 0
                } else {
                    fail "Key was not tracked as uncommitted on replica"
                }

                # A client connected to replica1 reads the key.
                # The key exists but is uncommitted — the read should be blocked.
                set replica_blocked_before [getInfoProperty [$replica1 info durability] durability_clients_waiting_ack]

                set reader [valkey_deferring_client -1]
                $reader READONLY
                $reader read ;# consume the READONLY OK reply
                $reader get uncommitted-key

                # Verify the read is blocked on the replica
                wait_for_condition 50 100 {
                    [getInfoProperty [$replica1 info durability] durability_clients_waiting_ack] > $replica_blocked_before
                } else {
                    fail "Read on uncommitted key was not blocked on replica"
                }

                # Resume the replication provider on the primary.
                # The committed offset advances, the primary sends
                # REPLCONF COMMIT, and the replica unblocks the read.
                $primary DEBUG durability-provider-resume replication
                $primary ping ;# force beforeSleep cycle

                # The reader should now get the value
                assert_equal "uncommitted-value" [$reader read]

                $reader close
                $writer close

                # Cleanup
                teardown_replication [list $replica1 $replica1_host $replica1_port \
                    $replica2 $replica2_host $replica2_port]
            }
        }
    }
}

} ;# end foreach side_channel
