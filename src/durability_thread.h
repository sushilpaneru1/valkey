#ifndef DURABILITY_THREAD_H
#define DURABILITY_THREAD_H

#include <stdint.h>
#include <stdbool.h>
#include "cluster.h" /* CLUSTER_NAMELEN */

#ifndef __cplusplus
#include <stdatomic.h>
#endif

/*================================= ISR Snapshot ============================= */

/**
 * A read-only snapshot of ISR membership, shared lock-free between the
 * main thread (publisher) and the durability thread (consumer).
 *
 * The main thread creates a new snapshot on every ISR change and swaps
 * the global pointer atomically. The durability thread reads the pointer
 * and retains/releases via refcount.
 *
 * Replicas are identified by their cluster node ID (CLUSTER_NAMELEN bytes).
 * The durability side channel is cluster-mode only.
 */
typedef struct isrSnapshot {
    char (*node_ids)[CLUSTER_NAMELEN]; /* Array of node IDs currently in ISR */
    int count;                         /* Number of ISR members */
    _Atomic(int) refcount;             /* Reference counting for safe deallocation */
} isrSnapshot;

/*================================= Per-Replica State ======================== */

/**
 * Per-replica state tracked by the durability thread.
 * One entry per side connection. Identified by cluster node ID.
 */
typedef struct dtReplicaState {
    char node_id[CLUSTER_NAMELEN]; /* Cluster node ID to correlate with main-thread replica */
    int fd;                        /* Side connection file descriptor */
    long long beacon_offset;       /* Latest beacon offset from this replica */
    int is_in_isr;                 /* From ISR snapshot */
    char read_buf[8];              /* Partial read buffer for beacon */
    int read_pos;                  /* Bytes read so far into read_buf */
} dtReplicaState;

/*================================= Thread Context =========================== */

/**
 * Durability thread context. Heap-allocated, primary only.
 * The durability thread owns this struct; the main thread accesses
 * only the atomic fields (committed_offset, running).
 */
typedef struct durabilityThreadCtx {
    pthread_t thread_id;
    int epoll_fd;                             /* epoll instance */
    int listen_fd;                            /* Listening socket on durability port */
    int wakeup_fd;                            /* eventfd for shutdown signaling */
    int notify_pipe[2];                       /* Pipe to wake primary event loop:
                                                 [0]=read (ae handler), [1]=write (DT signals) */

    /* Per-replica tracking */
    dtReplicaState *replicas;
    int replica_count;
    int replica_capacity;

    /* Committed offset (shared atomically with main thread) */
    _Atomic(long long) committed_offset;

    /* Lifecycle */
    _Atomic(int) running;

    /* Config cache */
    int min_sync_replicas;

    /* ISR snapshot */
    isrSnapshot *last_snapshot;
} durabilityThreadCtx;

/*================================= Public Interface ========================= */

/* Lifecycle — called from main thread */
void durabilityThreadInit(void);
void durabilityThreadShutdown(void);
bool durabilityThreadIsRunning(void);

/* ISR snapshot publishing — called from main thread */
void durabilityThreadPublishISRSnapshot(void);

/* Atomic committed offset — read by main thread */
long long durabilityThreadGetCommittedOffset(void);

/* Connection count — read by main thread for INFO output */
int durabilityThreadGetConnectionCount(void);

/* Notify pipe read fd — registered with primary's aeEventLoop to wake
 * the main thread when the committed offset advances. Returns -1 if
 * the durability thread is not running. */
int durabilityThreadGetNotifyFd(void);

/* ISR snapshot helpers */
isrSnapshot *createISRSnapshot(void);
void isrSnapshotRetain(isrSnapshot *snap);
void isrSnapshotRelease(isrSnapshot *snap);

#endif /* DURABILITY_THREAD_H */
