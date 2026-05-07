/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Durability side channel: a dedicated thread on the primary that accepts
 * lightweight binary beacons from replicas over per-replica TCP side
 * connections, computes the committed offset as the minimum across ISR
 * members, and shares it atomically with the main thread.
 *
 * The main thread continues to process REPLCONF ACK on the event loop.
 * The replication provider returns max(main_thread_offset, side_channel_offset).
 *
 * Replicas are identified by their cluster node ID (CLUSTER_NAMELEN bytes).
 * This feature is cluster-mode only.
 */

#include "server.h"
#include "durability_thread.h"
#include "cluster.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

/* ================================= Constants ============================== */

#define DT_INITIAL_REPLICA_CAPACITY 16
#define DT_EPOLL_MAX_EVENTS 64
#define DT_EPOLL_TIMEOUT_MS 100
#define DT_HANDSHAKE_MAX_AUTH_LEN 512

/* Handshake response codes */
#define DT_HANDSHAKE_OK 0x00
#define DT_HANDSHAKE_REJECTED 0x01

/* ================================= Globals ================================ */

/* Singleton durability thread context. NULL when thread is not running. */
static durabilityThreadCtx *dt_ctx = NULL;

/* Global ISR snapshot pointer, swapped atomically by the main thread. */
static _Atomic(isrSnapshot *) current_isr_snapshot = NULL;

/* ================================= ISR Snapshot ============================ */

/**
 * Create an ISR snapshot from the current server.replicas list.
 * Called from the main thread. Collects cluster node IDs of all replicas
 * that are online and have is_in_sync == 1.
 */
isrSnapshot *createISRSnapshot(void) {
    isrSnapshot *snap = zmalloc(sizeof(isrSnapshot));
    int capacity = listLength(server.replicas);

    if (capacity == 0) {
        snap->node_ids = NULL;
        snap->count = 0;
        atomic_store_explicit(&snap->refcount, 1, memory_order_relaxed);
        return snap;
    }

    snap->node_ids = zmalloc(sizeof(char[CLUSTER_NAMELEN]) * capacity);
    snap->count = 0;
    atomic_store_explicit(&snap->refcount, 1, memory_order_relaxed);

    listIter li;
    listNode *ln;
    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;
        if (replica->repl_data->repl_state != REPLICA_STATE_ONLINE) continue;
        if (!replica->repl_data->is_in_sync) continue;
        if (!replica->repl_data->replica_nodeid) continue;
        memcpy(snap->node_ids[snap->count], replica->repl_data->replica_nodeid,
               CLUSTER_NAMELEN);
        snap->count++;
    }

    return snap;
}

/**
 * Increment the reference count of an ISR snapshot.
 */
void isrSnapshotRetain(isrSnapshot *snap) {
    if (snap) atomic_fetch_add_explicit(&snap->refcount, 1, memory_order_relaxed);
}

/**
 * Decrement the reference count and free the snapshot when it reaches zero.
 */
void isrSnapshotRelease(isrSnapshot *snap) {
    if (!snap) return;
    if (atomic_fetch_sub_explicit(&snap->refcount, 1, memory_order_acq_rel) == 1) {
        zfree(snap->node_ids);
        zfree(snap);
    }
}

/**
 * Publish a new ISR snapshot. Called from the main thread whenever ISR
 * membership changes (promotion, removal, disconnect).
 *
 * Uses atomic_exchange to swap the global pointer. The old snapshot is
 * released (its refcount was 1 from creation; the durability thread may
 * hold an additional retain).
 */
void durabilityThreadPublishISRSnapshot(void) {
    if (!server.durability_side_channel) return;

    isrSnapshot *snap = createISRSnapshot();
    isrSnapshot *old = atomic_exchange_explicit(&current_isr_snapshot, snap, memory_order_release);
    if (old) isrSnapshotRelease(old);
}

/**
 * Check if a node ID is present in an ISR snapshot.
 * Returns 1 if found, 0 otherwise.
 */
static int isrSnapshotContains(isrSnapshot *snap, const char *node_id) {
    if (!snap) return 0;
    for (int j = 0; j < snap->count; j++) {
        if (memcmp(snap->node_ids[j], node_id, CLUSTER_NAMELEN) == 0) return 1;
    }
    return 0;
}

/**
 * Consume the latest ISR snapshot on the durability thread.
 * If the snapshot hasn't changed since last consumption, this is a no-op.
 */
static void dtConsumeISRSnapshot(durabilityThreadCtx *ctx) {
    isrSnapshot *snap = atomic_load_explicit(&current_isr_snapshot, memory_order_acquire);
    if (snap == ctx->last_snapshot) return;

    isrSnapshotRetain(snap);

    /* Update is_in_isr flags on tracked replicas. */
    for (int i = 0; i < ctx->replica_count; i++) {
        ctx->replicas[i].is_in_isr = isrSnapshotContains(snap, ctx->replicas[i].node_id);
    }

    if (ctx->last_snapshot) isrSnapshotRelease(ctx->last_snapshot);
    ctx->last_snapshot = snap;
}

/* ================================= Replica Array Helpers =================== */

/**
 * Ensure the replica array has room for at least one more entry.
 */
static void dtEnsureReplicaCapacity(durabilityThreadCtx *ctx) {
    if (ctx->replica_count < ctx->replica_capacity) return;
    ctx->replica_capacity = ctx->replica_capacity ? ctx->replica_capacity * 2 : DT_INITIAL_REPLICA_CAPACITY;
    ctx->replicas = zrealloc(ctx->replicas, sizeof(dtReplicaState) * ctx->replica_capacity);
}

/**
 * Find a replica by file descriptor. Returns index or -1.
 */
static int dtFindReplicaByFd(durabilityThreadCtx *ctx, int fd) {
    for (int i = 0; i < ctx->replica_count; i++) {
        if (ctx->replicas[i].fd == fd) return i;
    }
    return -1;
}

/**
 * Remove a replica from the array by index (swap with last element).
 */
static void dtRemoveReplica(durabilityThreadCtx *ctx, int idx) {
    if (idx < ctx->replica_count - 1) {
        ctx->replicas[idx] = ctx->replicas[ctx->replica_count - 1];
    }
    ctx->replica_count--;
}

/**
 * Close a side connection and remove the replica from tracking.
 */
static void dtCloseReplicaConnection(durabilityThreadCtx *ctx, int fd) {
    epoll_ctl(ctx->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    int idx = dtFindReplicaByFd(ctx, fd);
    if (idx >= 0) {
        serverLog(LL_NOTICE, "Durability side channel: replica %.40s disconnected",
                  ctx->replicas[idx].node_id);
        dtRemoveReplica(ctx, idx);
    }
}

/**
 * Close all side connections. Called during shutdown.
 */
static void dtCloseAllConnections(durabilityThreadCtx *ctx) {
    for (int i = 0; i < ctx->replica_count; i++) {
        epoll_ctl(ctx->epoll_fd, EPOLL_CTL_DEL, ctx->replicas[i].fd, NULL);
        close(ctx->replicas[i].fd);
    }
    ctx->replica_count = 0;
}

/* ================================= Handshake Protocol ====================== */

/**
 * Validate the auth token against the server's requirepass.
 * Returns 1 if auth is valid, 0 otherwise.
 *
 * If requirepass is not set, any auth token (including empty) is accepted.
 * If requirepass is set, the token must match exactly.
 */
static int dtValidateAuth(const char *auth_token) {
    if (!server.requirepass) return 1;
    if (!auth_token || auth_token[0] == '\0') return 0;
    return strcmp(auth_token, server.requirepass) == 0;
}

/**
 * Accept a new side connection and process the handshake.
 *
 * Handshake format (replica -> primary):
 *   [CLUSTER_NAMELEN bytes] cluster node ID (raw, not NUL-terminated)
 *   [variable] NUL-terminated auth token
 *
 * Response (primary -> replica):
 *   [1 byte] 0x00 = OK, 0x01 = Rejected
 *
 * The handshake is read synchronously since it's small and happens once
 * per connection. The fd is set to non-blocking after acceptance.
 */
static void dtAcceptConnection(durabilityThreadCtx *ctx) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    int fd = accept(ctx->listen_fd, (struct sockaddr *)&sa, &salen);
    if (fd == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            serverLog(LL_WARNING, "Durability side channel: accept() failed: %s", strerror(errno));
        }
        return;
    }

    /* Read the handshake synchronously with a short timeout. */
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* Read CLUSTER_NAMELEN-byte node ID. */
    char node_id[CLUSTER_NAMELEN];
    ssize_t nread;
    ssize_t total = 0;
    while (total < CLUSTER_NAMELEN) {
        nread = read(fd, node_id + total, CLUSTER_NAMELEN - total);
        if (nread <= 0) {
            serverLog(LL_WARNING, "Durability side channel: handshake read node_id failed: %s",
                      nread == 0 ? "connection closed" : strerror(errno));
            close(fd);
            return;
        }
        total += nread;
    }

    /* Read NUL-terminated auth token. */
    char auth_buf[DT_HANDSHAKE_MAX_AUTH_LEN + 1];
    int auth_len = 0;
    while (auth_len < DT_HANDSHAKE_MAX_AUTH_LEN) {
        nread = read(fd, &auth_buf[auth_len], 1);
        if (nread <= 0) {
            serverLog(LL_WARNING, "Durability side channel: handshake read auth failed: %s",
                      nread == 0 ? "connection closed" : strerror(errno));
            close(fd);
            return;
        }
        if (auth_buf[auth_len] == '\0') break;
        auth_len++;
    }
    auth_buf[auth_len] = '\0';

    /* Note: We do NOT validate the node ID against the ISR snapshot here.
     * The replica connects to the side channel before it joins the ISR
     * (ISR promotion requires a REPLCONF ACK first). Auth is sufficient
     * for access control. The ISR membership check gates whether the
     * replica's beacons contribute to the committed offset computation. */

    /* Validate auth token. */
    if (!dtValidateAuth(auth_buf)) {
        serverLog(LL_WARNING, "Durability side channel: rejected replica %.40s (auth failed)", node_id);
        uint8_t resp = DT_HANDSHAKE_REJECTED;
        write(fd, &resp, 1);
        close(fd);
        return;
    }

    /* Send OK response. */
    uint8_t resp = DT_HANDSHAKE_OK;
    if (write(fd, &resp, 1) != 1) {
        serverLog(LL_WARNING, "Durability side channel: failed to send handshake response to %.40s", node_id);
        close(fd);
        return;
    }

    /* Set fd to non-blocking for beacon reads. */
    anetNonBlock(NULL, fd);
    anetEnableTcpNoDelay(NULL, fd);

    /* Remove any existing connection for this node ID (reconnect case). */
    for (int i = 0; i < ctx->replica_count; i++) {
        if (memcmp(ctx->replicas[i].node_id, node_id, CLUSTER_NAMELEN) == 0) {
            serverLog(LL_NOTICE, "Durability side channel: replacing existing connection for replica %.40s",
                      node_id);
            epoll_ctl(ctx->epoll_fd, EPOLL_CTL_DEL, ctx->replicas[i].fd, NULL);
            close(ctx->replicas[i].fd);
            dtRemoveReplica(ctx, i);
            break;
        }
    }

    /* Add to replica tracking. */
    dtEnsureReplicaCapacity(ctx);
    dtReplicaState *rs = &ctx->replicas[ctx->replica_count++];
    memcpy(rs->node_id, node_id, CLUSTER_NAMELEN);
    rs->fd = fd;
    rs->beacon_offset = 0;
    rs->is_in_isr = isrSnapshotContains(ctx->last_snapshot, node_id);
    memset(rs->read_buf, 0, sizeof(rs->read_buf));
    rs->read_pos = 0;

    /* Register with epoll for reading. */
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        serverLog(LL_WARNING, "Durability side channel: epoll_ctl ADD failed for replica %.40s: %s",
                  node_id, strerror(errno));
        close(fd);
        ctx->replica_count--;
        return;
    }

    serverLog(LL_NOTICE, "Durability side channel: accepted connection from replica %.40s", node_id);
}

/* ================================= Beacon Reading ========================== */

/**
 * Update a replica's beacon offset. Returns 1 if the offset advanced,
 * 0 if it was a duplicate or regression (regression is logged).
 *
 * This is intentionally lightweight — the expensive min-ISR scan is
 * deferred to dtRecomputeCommittedOffset().
 */
static int dtOnBeacon(dtReplicaState *replica, long long offset) {
    /* Ignore offset regression. */
    if (offset <= replica->beacon_offset) {
        if (offset < replica->beacon_offset) {
            serverLog(LL_WARNING,
                      "Durability side channel: beacon offset regression from replica %.40s"
                      " (got %lld, have %lld)",
                      replica->node_id, offset, replica->beacon_offset);
        }
        return 0;
    }
    replica->beacon_offset = offset;
    return 1;
}

/**
 * Recompute the committed offset as the minimum beacon offset across
 * all ISR members. Only advances the committed offset forward (monotonic).
 *
 * Called once after processing all available beacons from a replica,
 * rather than on every individual beacon.
 */
static void dtRecomputeCommittedOffset(durabilityThreadCtx *ctx) {
    long long min_offset = LLONG_MAX;
    int isr_count = 0;

    for (int i = 0; i < ctx->replica_count; i++) {
        if (!ctx->replicas[i].is_in_isr) continue;
        isr_count++;
        if (ctx->replicas[i].beacon_offset < min_offset) {
            min_offset = ctx->replicas[i].beacon_offset;
        }
    }

    /* Don't advance if we don't have quorum. */
    if (isr_count < ctx->min_sync_replicas || min_offset == LLONG_MAX) return;

    /* Only advance forward (monotonic). */
    long long current = atomic_load_explicit(&ctx->committed_offset, memory_order_relaxed);
    if (min_offset > current) {
        atomic_store_explicit(&ctx->committed_offset, min_offset, memory_order_relaxed);
        /* Wake the primary's event loop so it can unblock clients immediately. */
        char c = 'x';
        write(ctx->notify_pipe[1], &c, 1);
    }
}

/**
 * Read beacon data from a replica's side connection.
 *
 * Beacons are fixed 8-byte little-endian uint64 offsets. Partial reads
 * are buffered in the per-replica read_buf/read_pos.
 */
static void dtReadBeacon(durabilityThreadCtx *ctx, int fd) {
    int idx = dtFindReplicaByFd(ctx, fd);
    if (idx < 0) {
        /* Unknown fd — shouldn't happen, clean up. */
        epoll_ctl(ctx->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return;
    }

    dtReplicaState *replica = &ctx->replicas[idx];
    int advanced = 0;

    for (;;) {
        ssize_t nread = read(fd, replica->read_buf + replica->read_pos, 8 - replica->read_pos);
        if (nread <= 0) {
            if (nread == 0) {
                /* Connection closed. */
                dtCloseReplicaConnection(ctx, fd);
                return;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* No more data available right now. */
                break;
            }
            /* Read error. */
            serverLog(LL_WARNING, "Durability side channel: read error from replica %.40s: %s",
                      replica->node_id, strerror(errno));
            dtCloseReplicaConnection(ctx, fd);
            return;
        }

        replica->read_pos += nread;

        /* Process complete beacons — just update the per-replica offset. */
        while (replica->read_pos >= 8) {
            uint64_t raw_offset;
            memcpy(&raw_offset, replica->read_buf, 8);
            memrev64ifbe(&raw_offset);
            long long offset = (long long)raw_offset;

            if (dtOnBeacon(replica, offset)) advanced = 1;

            /* Shift any remaining bytes (handles pipelined beacons). */
            int remaining = replica->read_pos - 8;
            if (remaining > 0) {
                memmove(replica->read_buf, replica->read_buf + 8, remaining);
            }
            replica->read_pos = remaining;
        }
    }

    /* Single min-ISR scan after draining all available beacons. */
    if (advanced) dtRecomputeCommittedOffset(ctx);
}

/* ================================= Thread Lifecycle ======================== */

/**
 * Durability thread main loop.
 *
 * Uses epoll_wait to multiplex:
 *   - listen_fd: accept new side connections
 *   - wakeup_fd: shutdown signal from main thread
 *   - replica fds: read beacons
 */
static void *durabilityThreadMain(void *arg) {
    durabilityThreadCtx *ctx = arg;
    struct epoll_event events[DT_EPOLL_MAX_EVENTS];

    serverLog(LL_NOTICE, "Durability side channel thread started on port %d", server.durability_side_channel_port);

    while (atomic_load_explicit(&ctx->running, memory_order_relaxed)) {
        int nfds = epoll_wait(ctx->epoll_fd, events, DT_EPOLL_MAX_EVENTS, DT_EPOLL_TIMEOUT_MS);

        if (nfds < 0) {
            if (errno == EINTR) continue;
            serverLog(LL_WARNING, "Durability side channel: epoll_wait error: %s", strerror(errno));
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == ctx->wakeup_fd) {
                /* Shutdown signal. Drain the eventfd. */
                uint64_t val;
                read(ctx->wakeup_fd, &val, sizeof(val));
                goto shutdown;
            } else if (fd == ctx->listen_fd) {
                dtAcceptConnection(ctx);
            } else {
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    dtCloseReplicaConnection(ctx, fd);
                } else if (events[i].events & EPOLLIN) {
                    dtReadBeacon(ctx, fd);
                }
            }
        }

        /* Check for ISR snapshot updates. */
        dtConsumeISRSnapshot(ctx);
    }

shutdown:
    dtCloseAllConnections(ctx);
    serverLog(LL_NOTICE, "Durability side channel thread stopped");
    return NULL;
}

/**
 * Create a listening socket on the durability port.
 * Binds to all interfaces (0.0.0.0). Returns fd or -1 on error.
 */
static int dtCreateListenSocket(int port) {
    int fd = anetTcpServer(server.neterr, port, NULL, server.tcp_backlog, 0);
    if (fd == ANET_ERR) {
        serverLog(LL_WARNING, "Durability side channel: failed to bind port %d: %s", port, server.neterr);
        return -1;
    }
    anetNonBlock(NULL, fd);
    anetCloexec(fd);
    return fd;
}

/**
 * Initialize and start the durability thread.
 * Called from the main thread during server init or on promotion to primary.
 */
void durabilityThreadInit(void) {
    if (dt_ctx) {
        serverLog(LL_WARNING, "Durability side channel: thread already running");
        return;
    }

    int port = server.durability_side_channel_port;
    if (port == 0) port = server.port + 30000;
    server.durability_side_channel_port = port;

    /* Create listening socket. */
    int listen_fd = dtCreateListenSocket(port);
    if (listen_fd == -1) {
        serverLog(LL_WARNING, "Durability side channel: failed to start (port bind failed)");
        return;
    }

    /* Create epoll instance. */
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd == -1) {
        serverLog(LL_WARNING, "Durability side channel: epoll_create1 failed: %s", strerror(errno));
        close(listen_fd);
        return;
    }

    /* Create eventfd for shutdown signaling. */
    int wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd == -1) {
        serverLog(LL_WARNING, "Durability side channel: eventfd failed: %s", strerror(errno));
        close(epoll_fd);
        close(listen_fd);
        return;
    }

    /* Create pipe for notifying the primary's event loop when the
     * committed offset advances. The read end is registered with ae,
     * the write end is used by the durability thread. */
    int notify_pipe[2];
    if (anetPipe(notify_pipe, O_NONBLOCK | O_CLOEXEC, O_NONBLOCK | O_CLOEXEC) == -1) {
        serverLog(LL_WARNING, "Durability side channel: pipe creation failed: %s", strerror(errno));
        close(wakeup_fd);
        close(epoll_fd);
        close(listen_fd);
        return;
    }

    /* Allocate context. */
    dt_ctx = zcalloc(sizeof(durabilityThreadCtx));
    dt_ctx->epoll_fd = epoll_fd;
    dt_ctx->listen_fd = listen_fd;
    dt_ctx->wakeup_fd = wakeup_fd;
    dt_ctx->notify_pipe[0] = notify_pipe[0];
    dt_ctx->notify_pipe[1] = notify_pipe[1];
    dt_ctx->replicas = zmalloc(sizeof(dtReplicaState) * DT_INITIAL_REPLICA_CAPACITY);
    dt_ctx->replica_count = 0;
    dt_ctx->replica_capacity = DT_INITIAL_REPLICA_CAPACITY;
    atomic_store_explicit(&dt_ctx->committed_offset, 0, memory_order_relaxed);
    atomic_store_explicit(&dt_ctx->running, 1, memory_order_relaxed);
    dt_ctx->min_sync_replicas = server.min_sync_replicas;
    dt_ctx->last_snapshot = NULL;

    /* Register listen_fd and wakeup_fd with epoll. */
    struct epoll_event ev;

    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        serverLog(LL_WARNING, "Durability side channel: epoll_ctl listen_fd failed: %s", strerror(errno));
        goto cleanup;
    }

    ev.events = EPOLLIN;
    ev.data.fd = wakeup_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wakeup_fd, &ev) == -1) {
        serverLog(LL_WARNING, "Durability side channel: epoll_ctl wakeup_fd failed: %s", strerror(errno));
        goto cleanup;
    }

    /* Publish initial ISR snapshot so the thread has membership data. */
    durabilityThreadPublishISRSnapshot();

    /* Spawn the thread. */
    if (pthread_create(&dt_ctx->thread_id, NULL, durabilityThreadMain, dt_ctx) != 0) {
        serverLog(LL_WARNING, "Durability side channel: pthread_create failed: %s", strerror(errno));
        goto cleanup;
    }

    serverLog(LL_NOTICE, "Durability side channel initialized on port %d", port);
    return;

cleanup:
    close(wakeup_fd);
    close(epoll_fd);
    close(listen_fd);
    zfree(dt_ctx->replicas);
    zfree(dt_ctx);
    dt_ctx = NULL;
}

/**
 * Shut down the durability thread. Called from the main thread during
 * server shutdown or on demotion from primary.
 *
 * Signals the thread via eventfd, joins it, and frees all resources.
 */
void durabilityThreadShutdown(void) {
    if (!dt_ctx) return;

    /* Signal the thread to stop. */
    atomic_store_explicit(&dt_ctx->running, 0, memory_order_relaxed);
    uint64_t val = 1;
    write(dt_ctx->wakeup_fd, &val, sizeof(val));

    /* Wait for the thread to finish. */
    pthread_join(dt_ctx->thread_id, NULL);

    /* Clean up resources. */
    close(dt_ctx->wakeup_fd);
    close(dt_ctx->notify_pipe[0]);
    close(dt_ctx->notify_pipe[1]);
    close(dt_ctx->epoll_fd);
    close(dt_ctx->listen_fd);

    if (dt_ctx->last_snapshot) isrSnapshotRelease(dt_ctx->last_snapshot);
    zfree(dt_ctx->replicas);
    zfree(dt_ctx);
    dt_ctx = NULL;

    /* Release the global ISR snapshot. */
    isrSnapshot *snap = atomic_exchange_explicit(&current_isr_snapshot, NULL, memory_order_relaxed);
    if (snap) isrSnapshotRelease(snap);

    serverLog(LL_NOTICE, "Durability side channel shut down");
}

/**
 * Check if the durability thread is currently running.
 */
bool durabilityThreadIsRunning(void) {
    return dt_ctx != NULL && atomic_load_explicit(&dt_ctx->running, memory_order_relaxed);
}

/**
 * Read the committed offset computed by the durability thread.
 * Called from the main thread (O(1) atomic load).
 */
long long durabilityThreadGetCommittedOffset(void) {
    if (!dt_ctx) return 0;
    return atomic_load_explicit(&dt_ctx->committed_offset, memory_order_relaxed);
}

/**
 * Get the number of active side channel connections.
 * Called from the main thread for INFO output.
 */
int durabilityThreadGetConnectionCount(void) {
    if (!dt_ctx) return 0;
    return dt_ctx->replica_count;
}
/**
 * Get the notify pipe read fd for registration with the primary's aeEventLoop.
 * Returns -1 if the durability thread is not running.
 */
int durabilityThreadGetNotifyFd(void) {
    if (!dt_ctx) return -1;
    return dt_ctx->notify_pipe[0];
}
