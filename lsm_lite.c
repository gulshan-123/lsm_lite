#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "pgstat.h"
#include "lsm_skiplist.h" // Your Week 1 header
#include "lsm_sstable.h"

#include <sys/stat.h> // For mkdir

#define LSM_MAX_NODES 20 // roughly 16MB depending on struct size
#define LSM_MEMTABLE_NODES (LSM_MAX_NODES/2)
#define MAX_LSM_LEVELS 10
#define COMPACTION_THRESHOLD 4

// Use the maximum positive 32-bit integer as the delete marker
#define LSM_TOMBSTONE_VAL 2147483647

PG_MODULE_MAGIC;

// The Global Manifest 
typedef struct {
    int file_counts[MAX_LSM_LEVELS]; // Tracks how many files exist at each level
    bool is_flushing;
    Latch bgw_latch;       // The signal bell for our background worker
    SkipList active_mem;
    SkipList immutable_mem;
} LsmManifest;

static LsmManifest *Manifest = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

// 1. Request the exact bytes needed
static void lsm_shmem_request(void) {
    if (prev_shmem_request_hook) prev_shmem_request_hook();
    // Request space for Manifest + Two Node Pools
    Size total_size = sizeof(LsmManifest) + (2 * LSM_MEMTABLE_NODES * sizeof(SkipNode));
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
    Size total_size = sizeof(LsmManifest) + (2 * LSM_MEMTABLE_NODES * sizeof(SkipNode));
    Manifest = ShmemInitStruct("LsmLiteManifest", total_size, &found);

    if (!found) {
        for (int i = 0; i < MAX_LSM_LEVELS; i++) {
            Manifest->file_counts[i] = 0;
        }
        Manifest->is_flushing = false;
        InitSharedLatch(&Manifest->bgw_latch);

        // Pointer math for the two contiguous pools
        SkipNode* active_pool = (SkipNode*) ((char*)Manifest + sizeof(LsmManifest));
        SkipNode* immutable_pool = active_pool + LSM_MEMTABLE_NODES;

        // Init Active MemTable
        Manifest->active_mem.capacity = LSM_MEMTABLE_NODES;
        Manifest->active_mem.nodes = active_pool;
        Manifest->active_mem.head_idx = 0;
        Manifest->active_mem.free_head_idx = 1;
        Manifest->active_mem.current_level = 1;
        for (int i = 0; i < LSM_MEMTABLE_NODES - 1; i++) 
            Manifest->active_mem.nodes[i].next[0] = i + 1;
        Manifest->active_mem.nodes[LSM_MEMTABLE_NODES - 1].next[0] = LSM_NULL_INDEX;
        for (int i = 0; i < MAX_LEVEL; i++) 
            Manifest->active_mem.nodes[0].next[i] = LSM_NULL_INDEX;

        // Init Immutable MemTable (Identical setup)
        Manifest->immutable_mem.capacity = LSM_MEMTABLE_NODES;
        Manifest->immutable_mem.nodes = immutable_pool;
        Manifest->immutable_mem.head_idx = 0;
        Manifest->immutable_mem.free_head_idx = 1; // It starts "empty" too
        Manifest->immutable_mem.current_level = 1;
        for (int i = 0; i < LSM_MEMTABLE_NODES - 1; i++) 
            Manifest->immutable_mem.nodes[i].next[0] = i + 1;
        Manifest->immutable_mem.nodes[LSM_MEMTABLE_NODES - 1].next[0] = LSM_NULL_INDEX;
        for (int i = 0; i < MAX_LEVEL; i++) 
            Manifest->immutable_mem.nodes[0].next[i] = LSM_NULL_INDEX;

        
        // --- 1. MANIFEST RECOVERY ---
        FILE* manifest_fp = fopen("lsm_data/manifest.bin", "rb");
        if (manifest_fp) {
            fread(Manifest->file_counts, sizeof(int), MAX_LSM_LEVELS, manifest_fp);
            fclose(manifest_fp);
            elog(LOG, "LSM Recovery: Manifest loaded successfully.");
        }

        // --- 2. WAL RECOVERY (Immutable) ---
        // If the database crashed while the BGWorker was flushing, this file still exists!
        FILE* wal_immut = fopen("lsm_data/wal_immutable.bin", "rb");
        if (wal_immut) {
            uint32_t k, v;
            while (fread(&k, sizeof(uint32_t), 1, wal_immut) == 1 && 
                   fread(&v, sizeof(uint32_t), 1, wal_immut) == 1) {
                sl_insert(&Manifest->immutable_mem, k, v);
            }
            fclose(wal_immut);
            // Tell the BGWorker to immediately flush this recovered data when it starts
            Manifest->is_flushing = true; 
            elog(LOG, "LSM Recovery: Immutable WAL recovered.");
        }

        // --- 3. WAL RECOVERY (Active) ---
        FILE* wal_active = fopen("lsm_data/wal_active.bin", "rb");
        if (wal_active) {
            uint32_t k, v;
            while (fread(&k, sizeof(uint32_t), 1, wal_active) == 1 && 
                   fread(&v, sizeof(uint32_t), 1, wal_active) == 1) {
                sl_insert(&Manifest->active_mem, k, v);
            }
            fclose(wal_active);
            elog(LOG, "LSM Recovery: Active WAL recovered.");
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

    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "lsm_lite");
    sprintf(worker.bgw_function_name, "lsm_worker_main");
    sprintf(worker.bgw_name, "LSM Flusher Worker");
    sprintf(worker.bgw_type, "LSM Flusher Worker");
    RegisterBackgroundWorker(&worker);
}

// Postgres requires this macro to expose functions to SQL
PG_FUNCTION_INFO_V1(lsm_put);
PG_FUNCTION_INFO_V1(lsm_get);
PG_FUNCTION_INFO_V1(lsm_delete);

Datum lsm_put(PG_FUNCTION_ARGS) {
    int32 key = PG_GETARG_INT32(0);
    int32 val = PG_GETARG_INT32(1);

    LWLockPadded *lock_array = GetNamedLWLockTranche("lsm_lite_locks");
    LWLock *memtable_lock = &lock_array[0].lock; 

retry:
    LWLockAcquire(memtable_lock, LW_EXCLUSIVE);

    // CHECK CAPACITY: Is the Active MemTable full?
    if (Manifest->active_mem.free_head_idx == LSM_NULL_INDEX) {
        if (Manifest->is_flushing) {
            // COMPACTION STALL: The background worker hasn't finished writing 
            // the previous MemTable to disk. We MUST wait to avoid memory corruption.
            LWLockRelease(memtable_lock);
            pg_usleep(10000); // Sleep for 10ms
            goto retry;
        } else {
            // THE $O(1)$ SWAP
            SkipList temp = Manifest->active_mem;
            Manifest->active_mem = Manifest->immutable_mem;
            Manifest->immutable_mem = temp;
            
            // The active_mem is already perfectly clean thanks to the BGWorker!
            rename("lsm_data/wal_active.bin", "lsm_data/wal_immutable.bin");    
            Manifest->is_flushing = true;
            SetLatch(&Manifest->bgw_latch);
        }
    }

    FILE* wal_fp = fopen("lsm_data/wal_active.bin", "ab");
    if (wal_fp) {
        fwrite(&key, sizeof(uint32_t), 1, wal_fp);
        fwrite(&val, sizeof(uint32_t), 1, wal_fp);
        fclose(wal_fp);
    }

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

    if (!found) {
        // Search Disk: Level by Level, Newest to Oldest
        for (int lvl = 0; lvl < MAX_LSM_LEVELS; lvl++) {
            for (int idx = Manifest->file_counts[lvl] - 1; idx >= 0; idx--) {
                char filename[256];
                snprintf(filename, sizeof(filename), "lsm_data/L%d_%d.sst", lvl, idx);
                
                if (sst_read(filename, (uint32_t)key, &val)) {
                    found = true;
                    goto end_search; // Break out of nested loops
                }
            }
        }
    }

end_search:
    LWLockRelease(memtable_lock);

    if (found) {
        if (val == LSM_TOMBSTONE_VAL) {
            // We found the key, but it's a Tombstone! It has been deleted.
            PG_RETURN_NULL();
        } else {
            PG_RETURN_INT32((int32)val);
        }
    } else {
        PG_RETURN_NULL();
    }
}

Datum lsm_delete(PG_FUNCTION_ARGS) {
    int32 key = PG_GETARG_INT32(0);

    LWLockPadded *lock_array = GetNamedLWLockTranche("lsm_lite_locks");
    LWLock *memtable_lock = &lock_array[0].lock; 

retry:
    LWLockAcquire(memtable_lock, LW_EXCLUSIVE);

    if (Manifest->active_mem.free_head_idx == LSM_NULL_INDEX) {
        if (Manifest->is_flushing) {
            LWLockRelease(memtable_lock);
            pg_usleep(10000);
            goto retry;
        } else {
            // THE SWAP
            SkipList temp = Manifest->active_mem;
            Manifest->active_mem = Manifest->immutable_mem;
            Manifest->immutable_mem = temp;
            
            rename("lsm_data/wal_active.bin", "lsm_data/wal_immutable.bin");
            Manifest->is_flushing = true;
            SetLatch(&Manifest->bgw_latch);
        }
    }

    // Write Tombstone to WAL
    FILE* wal_fp = fopen("lsm_data/wal_active.bin", "ab");
    if (wal_fp) {
        uint32_t tombstone = LSM_TOMBSTONE_VAL;
        fwrite(&key, sizeof(uint32_t), 1, wal_fp);
        fwrite(&tombstone, sizeof(uint32_t), 1, wal_fp);
        fclose(wal_fp);
    }

    // Insert Tombstone into memory
    bool success = sl_insert(&Manifest->active_mem, (uint32_t)key, LSM_TOMBSTONE_VAL);

    LWLockRelease(memtable_lock);
    PG_RETURN_BOOL(success);
}

// Define flag for loop exit
static volatile sig_atomic_t got_sigterm = false;
static void lsm_sigterm(SIGNAL_ARGS) { 
    got_sigterm = true; 
    if (Manifest) {
        SetLatch(&Manifest->bgw_latch); // Wake the correct latch!
    }
}

// The main loop for the background worker
void PGDLLEXPORT
lsm_worker_main(Datum main_arg) {
    pqsignal(SIGTERM, lsm_sigterm);
    BackgroundWorkerUnblockSignals();

    mkdir("lsm_data", 0700);
    elog(LOG, "LSM Flusher Worker started.");

    // Explicitly claim ownership of the latch so we can legally sleep on it.
    OwnLatch(&Manifest->bgw_latch);

    while (!got_sigterm) {
        WaitLatch(&Manifest->bgw_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1, PG_WAIT_EXTENSION);
        ResetLatch(&Manifest->bgw_latch);

        if (Manifest->is_flushing) {
            char filename[256];
            snprintf(filename, sizeof(filename), "lsm_data/L0_%d.sst", Manifest->file_counts[0]);

            bool success = sst_write(&Manifest->immutable_mem, filename);

            if (success)
            {
                elog(LOG, "LSM Flusher: Successfully wrote %s", filename);
                
                // REBUILD THE FREE LIST IN THE BACKGROUND
                Manifest->immutable_mem.head_idx = 0;
                Manifest->immutable_mem.free_head_idx = 1;
                Manifest->immutable_mem.current_level = 1;
                
                for(int i=0; i<MAX_LEVEL; i++) {
                    Manifest->immutable_mem.nodes[0].next[i] = LSM_NULL_INDEX;
                }
                for (int i = 1; i < Manifest->immutable_mem.capacity - 1; i++) {
                    Manifest->immutable_mem.nodes[i].next[0] = i + 1;
                }
                Manifest->immutable_mem.nodes[Manifest->immutable_mem.capacity - 1].next[0] = LSM_NULL_INDEX;

                // Now it is safe to release the backpressure
                LWLockAcquire(&GetNamedLWLockTranche("lsm_lite_locks")[0].lock, LW_EXCLUSIVE);
                Manifest->file_counts[0]++; // Increment L0 count
                Manifest->is_flushing = false; 
                LWLockRelease(&GetNamedLWLockTranche("lsm_lite_locks")[0].lock);
                
                // === THE CASCADING COMPACTION TRIGGER ===
                int current_level = 0;
                while (Manifest->file_counts[current_level] >= COMPACTION_THRESHOLD) {
                    elog(LOG, "LSM Compaction: Triggering Level %d -> Level %d", current_level, current_level + 1);
                    
                    int out_idx = Manifest->file_counts[current_level + 1];
                    
                    if (sst_compact(current_level, COMPACTION_THRESHOLD, out_idx)) {
                        // Success! Update the manifest.
                        LWLockAcquire(&GetNamedLWLockTranche("lsm_lite_locks")[0].lock, LW_EXCLUSIVE);
                        Manifest->file_counts[current_level] = 0; // The 4 old files are gone
                        Manifest->file_counts[current_level + 1]++; // We have 1 new file
                        LWLockRelease(&GetNamedLWLockTranche("lsm_lite_locks")[0].lock);
                        
                        current_level++; // Loop again to see if L1 -> L2 is needed!
                    } else {
                        elog(ERROR, "LSM Compaction Failed!");
                        break;
                    }
                }
                // --- DELETE THE OBSOLETE WAL ---
                unlink("lsm_data/wal_immutable.bin");
                // --- SAVE THE MANIFEST TO DISK ---
                // We write to a temporary file and rename it to prevent corruption 
                // if the server crashes exactly while writing the manifest.
                FILE* manifest_fp = fopen("lsm_data/manifest.tmp", "wb");
                if (manifest_fp) {
                    fwrite(Manifest->file_counts, sizeof(int), MAX_LSM_LEVELS, manifest_fp);
                    fclose(manifest_fp);
                    rename("lsm_data/manifest.tmp", "lsm_data/manifest.bin");
                }
            } 
            else 
            {
                elog(ERROR, "LSM Flusher: Failed to write %s", filename);
            }
        }
    }
    
    DisownLatch(&Manifest->bgw_latch);
    proc_exit(0);
}

