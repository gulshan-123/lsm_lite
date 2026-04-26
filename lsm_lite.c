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

// for fdw
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"

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
    bool is_flushing;
    int file_counts[MAX_LSM_LEVELS]; // Tracks how many files exist at each level
    int current_wal_gen;  // Tracks WAL rotations
    Latch bgw_latch;       // The signal bell for our background worker
    SkipList active_mem;
    SkipList immutable_mem;
} LsmManifest;

// Process-local cache (Not in shared memory!)
static LsmManifest *Manifest = NULL;
static FILE* local_wal_fp = NULL;
static int local_wal_gen = -1;

// since we focused only on point updates (hack for delete)
static int32 current_query_key = -1;

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
        mkdir("lsm_data", 0700); // Ensure the directory for SSTables and WALs exists
        for (int i = 0; i < MAX_LSM_LEVELS; i++) {
            Manifest->file_counts[i] = 0;
        }
        Manifest->is_flushing = false;
        Manifest->current_wal_gen = 0;

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

            Manifest->current_wal_gen++;

            // If *this* process has an open file, close it
            if (local_wal_fp) {
                fclose(local_wal_fp);
                local_wal_fp = NULL;
            }
            
            // The active_mem is already perfectly clean thanks to the BGWorker!
            rename("lsm_data/wal_active.bin", "lsm_data/wal_immutable.bin");    
            Manifest->is_flushing = true;
            SetLatch(&Manifest->bgw_latch);
        }
    }

    // --- CACHED WRITE-AHEAD LOG ---
    // Invalidate stale file descriptors if another process rotated the WAL
    if (local_wal_fp != NULL && local_wal_gen != Manifest->current_wal_gen) {
        fclose(local_wal_fp);
        local_wal_fp = NULL;
    }

    // Open file if we don't have one cached
    if (local_wal_fp == NULL) {
        local_wal_fp = fopen("lsm_data/wal_active.bin", "ab");
        local_wal_gen = Manifest->current_wal_gen; // Sync with global generation
    }

    if (local_wal_fp) {
        fwrite(&key, sizeof(uint32_t), 1, local_wal_fp);
        fwrite(&val, sizeof(uint32_t), 1, local_wal_fp);
        fflush(local_wal_fp); // Push to OS cache immediately!
    }

    bool success = sl_insert(&Manifest->active_mem, (uint32_t)key, (uint32_t)val);

    LWLockRelease(memtable_lock);
    PG_RETURN_BOOL(success);
}

// Put this above your SQL functions
bool lsm_internal_get(uint32_t key, uint32_t *out_val) {
    bool found = false;
    LWLockPadded *lock_array = GetNamedLWLockTranche("lsm_lite_locks");
    LWLock *memtable_lock = &lock_array[0].lock; 

    // Memory Search
    LWLockAcquire(memtable_lock, LW_SHARED);
    found = sl_search(&Manifest->active_mem, key, out_val);
    if (!found && Manifest->is_flushing)
        found = sl_search(&Manifest->immutable_mem, key, out_val);
        
    // Disk Search (Thread-Safe)
    if (!found) {
        for (int lvl = 0; lvl < MAX_LSM_LEVELS; lvl++) {
            for (int idx = Manifest->file_counts[lvl] - 1; idx >= 0; idx--) {
                char filename[256];
                snprintf(filename, sizeof(filename), "lsm_data/L%d_%d.sst", lvl, idx);
                if (sst_read(filename, key, out_val)) {
                    found = true;
                    goto end_search; 
                }
            }
        }
    }
    
end_search:
    LWLockRelease(memtable_lock);
    if (found && *out_val == LSM_TOMBSTONE_VAL) return false;
    return found;
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

    // Search immutable_mem if it has data that hasn't been flushed yet.
    // is_flushing = true means BGWorker is reading immutable_mem (safe for us to read too)
    // is_flushing = false means immutable_mem is being rebuilt (empty, skip it)
    if (!found && Manifest->is_flushing) {
        found = sl_search(&Manifest->immutable_mem, (uint32_t)key, &val);
    }

    
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

/* Shared logic for both UDFs and FDW */
bool lsm_internal_put(uint32_t key, uint32_t val) {
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
            /* Swap Active/Immutable */
            SkipList temp = Manifest->active_mem;
            Manifest->active_mem = Manifest->immutable_mem;
            Manifest->immutable_mem = temp;
            Manifest->current_wal_gen++;
            if (local_wal_fp) { fclose(local_wal_fp); local_wal_fp = NULL; }
            rename("lsm_data/wal_active.bin", "lsm_data/wal_immutable.bin");
            Manifest->is_flushing = true;
            SetLatch(&Manifest->bgw_latch);
        }
    }

    /* WAL Write */
    if (local_wal_fp == NULL || local_wal_gen != Manifest->current_wal_gen) {
        if (local_wal_fp) fclose(local_wal_fp);
        local_wal_fp = fopen("lsm_data/wal_active.bin", "ab");
        local_wal_gen = Manifest->current_wal_gen;
    }
    if (local_wal_fp) {
        fwrite(&key, sizeof(uint32_t), 1, local_wal_fp);
        fwrite(&val, sizeof(uint32_t), 1, local_wal_fp);
        fflush(local_wal_fp);
    }

    bool success = sl_insert(&Manifest->active_mem, key, val);
    LWLockRelease(memtable_lock);
    return success;
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

            Manifest->current_wal_gen++;

            // If *this* process has an open file, close it
            if (local_wal_fp) {
                fclose(local_wal_fp);
                local_wal_fp = NULL;
            }
            
            rename("lsm_data/wal_active.bin", "lsm_data/wal_immutable.bin");
            Manifest->is_flushing = true;
            SetLatch(&Manifest->bgw_latch);
        }
    }

    // Write Tombstone to WAL
    // --- CACHED WRITE-AHEAD LOG ---
    // Invalidate stale file descriptors if another process rotated the WAL
    if (local_wal_fp != NULL && local_wal_gen != Manifest->current_wal_gen) {
        fclose(local_wal_fp);
        local_wal_fp = NULL;
    }

    // Open file if we don't have one cached
    if (local_wal_fp == NULL) {
        local_wal_fp = fopen("lsm_data/wal_active.bin", "ab");
        local_wal_gen = Manifest->current_wal_gen; // Sync with global generation
    }
    if (local_wal_fp) {
        uint32_t tombstone = LSM_TOMBSTONE_VAL;
        fwrite(&key, sizeof(uint32_t), 1, local_wal_fp);
        fwrite(&tombstone, sizeof(uint32_t), 1, local_wal_fp);
        fflush(local_wal_fp);
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

                // --- DELETE THE OBSOLETE WAL ---
                unlink("lsm_data/wal_immutable.bin");
                
                // REBUILD THE FREE LIST IN THE BACKGROUND
                LWLockAcquire(&GetNamedLWLockTranche("lsm_lite_locks")[0].lock, LW_EXCLUSIVE);

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
                        Manifest->file_counts[current_level] -= COMPACTION_THRESHOLD; // The 4 old files are gone
                        Manifest->file_counts[current_level + 1]++; // We have 1 new file
                        LWLockRelease(&GetNamedLWLockTranche("lsm_lite_locks")[0].lock);
                        
                        current_level++; // Loop again to see if L1 -> L2 is needed!
                    } else {
                        elog(ERROR, "LSM Compaction Failed!");
                        break;
                    }
                }
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

typedef struct {
    int32 search_key;
    bool already_returned;
} LsmFdwState;

// --- 1. PLANNER: Extract 'key = X' from the AST ---
static void lsmGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    ListCell *lc;
    bool found_key = false;

    foreach(lc, baserel->baserestrictinfo) {
        RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
        if (IsA(ri->clause, OpExpr)) {
            OpExpr *op = (OpExpr *) ri->clause;
            if (list_length(op->args) == 2) {
                Node *left = linitial(op->args);
                Node *right = lsecond(op->args);
                // Check if it is "column = constant"
                if (IsA(left, Var) && IsA(right, Const)) {
                    Var *var = (Var *) left;
                    Const *cst = (Const *) right;
                    // Enforce that they are searching on the first column (key)
                    if (var->varattno == 1 && !cst->constisnull) {
                        found_key = true;
                        break;
                    }
                }
            }
        }
    }

    if (!found_key) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("LSM-Lite requires a 'WHERE key = <constant>' clause.")));
    }
    
    baserel->rows = 1; // We only return 1 row for a KV lookup
}

static void lsmGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    add_path(baserel, (Path *) create_foreignscan_path(root, baserel, NULL, baserel->rows, 10.0, 10.0, NIL, NULL, NULL, NIL, NIL));
}

static ForeignScan *lsmGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan) {
    int32 extracted_key = -1;
    ListCell *lc;
    
    // Extract the exact integer value to pass to the executor
    foreach(lc, baserel->baserestrictinfo) {
        RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
        if (IsA(ri->clause, OpExpr)) {
            OpExpr *op = (OpExpr *) ri->clause;
            if (IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Const)) {
                Const *cst = (Const *) lsecond(op->args);
                extracted_key = DatumGetInt32(cst->constvalue);
                break;
            }
        }
    }

    // Pass the extracted key safely using an integer list in fdw_private
    List *fdw_private = list_make1_int(extracted_key);
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    
    return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, fdw_private, NIL, NIL, outer_plan);
}

// --- 2. EXECUTOR: Build the Tuple ---
static void lsmBeginForeignScan(ForeignScanState *node, int eflags) {
    LsmFdwState *state = (LsmFdwState *) palloc(sizeof(LsmFdwState));
    state->already_returned = false;
    
    // Deserialize the key we packed in the planner
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    state->search_key = linitial_int(fsplan->fdw_private);

    current_query_key = state->search_key;
    
    node->fdw_state = (void *) state;
}

static TupleTableSlot *lsmIterateForeignScan(ForeignScanState *node) {
    LsmFdwState *state = (LsmFdwState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

    ExecClearTuple(slot);

    // If we already returned the row, tell Postgres the scan is done
    if (state->already_returned) return slot;

    uint32_t val;
    bool found = lsm_internal_get((uint32_t)state->search_key, &val);

    if (found) {
        Datum values[2];
        bool nulls[2] = {false, false};

        values[0] = Int32GetDatum(state->search_key);
        values[1] = Int32GetDatum(val);

        // Convert our C variables into a Postgres HeapTuple
        HeapTuple tuple = heap_form_tuple(slot->tts_tupleDescriptor, values, nulls);
        ExecStoreHeapTuple(tuple, slot, false);
    }

    state->already_returned = true;
    return slot;
}

static void lsmReScanForeignScan(ForeignScanState *node) {
    LsmFdwState *state = (LsmFdwState *) node->fdw_state;
    state->already_returned = false;
}

static void lsmEndForeignScan(ForeignScanState *node) {
    // Memory is freed automatically by Postgres context manager
}

/* Mandatory for DML support: Tells Postgres which columns are needed */
static void lsmAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation) {
    /* No special targets needed for LSM point-ops */
}

static List *lsmPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index) {
    return NIL;
}

static void lsmBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags) {
    /* No per-query state needed for writes */
}

static TupleTableSlot *lsmExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {
    bool isnull;
    /* Extract 'key' (attr 1) and 'val' (attr 2) from the slot */
    Datum key_datum = slot_getattr(slot, 1, &isnull);
    Datum val_datum = slot_getattr(slot, 2, &isnull);

    if (!lsm_internal_put(DatumGetUInt32(key_datum), DatumGetUInt32(val_datum)))
        ereport(ERROR, (errmsg("LSM-Lite: Insert failed (MemTable full/System error)")));

    return slot;
}

static TupleTableSlot *lsmExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {
    bool isnull;
    /* In LSM, Delete is just an Insert with a Tombstone */
    if (!lsm_internal_put(DatumGetUInt32(current_query_key), LSM_TOMBSTONE_VAL))
        ereport(ERROR, (errmsg("LSM-Lite: Delete failed")));

    return slot;
}

static TupleTableSlot *lsmExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {
    /* In LSM, Update is exactly the same as Insert */
    return lsmExecForeignInsert(estate, rinfo, slot, planSlot);
}

static void lsmEndForeignModify(EState *estate, ResultRelInfo *rinfo) {
    /* Cleanup */
}

// --- 3. REGISTRATION ---
PG_FUNCTION_INFO_V1(lsm_fdw_handler);
Datum lsm_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);
    
    /* Read Path Callbacks */
    fdwroutine->GetForeignRelSize = lsmGetForeignRelSize;
    fdwroutine->GetForeignPaths = lsmGetForeignPaths;
    fdwroutine->GetForeignPlan = lsmGetForeignPlan;
    fdwroutine->BeginForeignScan = lsmBeginForeignScan;
    fdwroutine->IterateForeignScan = lsmIterateForeignScan;
    fdwroutine->ReScanForeignScan = lsmReScanForeignScan;
    fdwroutine->EndForeignScan = lsmEndForeignScan;

    /* Write Path Callbacks */
    fdwroutine->AddForeignUpdateTargets = lsmAddForeignUpdateTargets;
    fdwroutine->PlanForeignModify = lsmPlanForeignModify;
    fdwroutine->BeginForeignModify = lsmBeginForeignModify;
    fdwroutine->ExecForeignInsert = lsmExecForeignInsert;
    fdwroutine->ExecForeignUpdate = lsmExecForeignUpdate;
    fdwroutine->ExecForeignDelete = lsmExecForeignDelete;
    fdwroutine->EndForeignModify = lsmEndForeignModify;
    
    PG_RETURN_POINTER(fdwroutine);
}

