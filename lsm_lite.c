#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "lsm_skiplist.h" // Your Week 1 header

#define LSM_MAX_NODES 500000 // roughly 16MB depending on struct size

PG_MODULE_MAGIC;

// The Global Manifest 
typedef struct {
    int current_l0_files;
    SkipList active_mem;  // The struct is embedded directly
    // Note: We don't embed the SkipNode array here. We put it right after.
} LsmManifest;

static LsmManifest *Manifest = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

// 1. Request the exact bytes needed
static void lsm_shmem_request(void) {
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    
    // We need space for the Manifest + The massive array of nodes
    Size total_size = sizeof(LsmManifest) + (LSM_MAX_NODES * sizeof(SkipNode));
    RequestAddinShmemSpace(total_size);
    RequestNamedLWLockTranche("lsm_lite_locks", 128);
}

// 2. Initialize memory (Safe inside Postmaster)
static void lsm_shmem_startup(void) {
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    // Grab the massive block of memory
    Size total_size = sizeof(LsmManifest) + (LSM_MAX_NODES * sizeof(SkipNode));
    Manifest = ShmemInitStruct("LsmLiteManifest", total_size, &found);

    if (!found) {
        Manifest->current_l0_files = 0;
        
        // --- POINTER ARITHMETIC MAGIC ---
        // The node array starts exactly where the Manifest struct ends in memory.
        SkipNode* node_pool = (SkipNode*) ((char*)Manifest + sizeof(LsmManifest));
        
        // Initialize the embedded SkipList struct
        Manifest->active_mem.capacity = LSM_MAX_NODES;
        Manifest->active_mem.nodes = node_pool; // Point it to our shared memory array
        
        // Run your Week 1 initialization logic directly in shared memory
        Manifest->active_mem.head_idx = 0;
        Manifest->active_mem.free_head_idx = 1;
        Manifest->active_mem.current_level = 1;

        for (int i = 0; i < LSM_MAX_NODES - 1; i++) {
            Manifest->active_mem.nodes[i].next[0] = i + 1;
        }
        Manifest->active_mem.nodes[LSM_MAX_NODES - 1].next[0] = LSM_NULL_INDEX;

        for (int i = 0; i < MAX_LEVEL; i++) {
            Manifest->active_mem.nodes[0].next[i] = LSM_NULL_INDEX;
        }

        elog(LOG, "LSM-Lite: Static Shared Memory Skip List perfectly initialized.");
    }

    LWLockRelease(AddinShmemInitLock);
}

void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR, (errmsg("lsm_lite must be loaded via shared_preload_libraries")));

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = lsm_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = lsm_shmem_startup;
}

// Postgres requires this macro to expose functions to SQL
PG_FUNCTION_INFO_V1(lsm_put);
PG_FUNCTION_INFO_V1(lsm_get);

Datum lsm_put(PG_FUNCTION_ARGS) {
    int32 key = PG_GETARG_INT32(0);
    int32 val = PG_GETARG_INT32(1);

    LWLockPadded *lock_array = GetNamedLWLockTranche("lsm_lite_locks");
    // Use Lock 0 as the global MemTable Write Lock
    LWLock *memtable_lock = &lock_array[0].lock; 

    // Acquire EXCLUSIVE lock. Only one process can insert at a exact millisecond.
    LWLockAcquire(memtable_lock, LW_EXCLUSIVE);

    bool success = sl_insert(&Manifest->active_mem, (uint32_t)key, (uint32_t)val);

    LWLockRelease(memtable_lock);

    PG_RETURN_BOOL(success);
}

Datum lsm_get(PG_FUNCTION_ARGS) {
    int32 key = PG_GETARG_INT32(0);
    uint32_t val;

    LWLockPadded *lock_array = GetNamedLWLockTranche("lsm_lite_locks");
    // Use Lock 0 as the global MemTable Read Lock
    LWLock *memtable_lock = &lock_array[0].lock; 

    // Acquire SHARED lock. Multiple SELECTs can read simultaneously!
    LWLockAcquire(memtable_lock, LW_SHARED);

    bool found = sl_search(&Manifest->active_mem, (uint32_t)key, &val);

    LWLockRelease(memtable_lock);

    if (found) {
        PG_RETURN_INT32((int32)val);
    } else {
        PG_RETURN_NULL();
    }
}
