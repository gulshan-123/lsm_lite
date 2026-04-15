#ifndef LSM_SSTABLE_H
#define LSM_SSTABLE_H

#include "lsm_skiplist.h"
#include <stdint.h>
#include <stdbool.h>

#define BLOOM_FILTER_SIZE_BYTES 4096
#define SPARSE_INTERVAL 16  // Record an index entry every 16 keys

// Writes the in-memory Skip List to a perfectly formatted binary SSTable.
bool sst_write(SkipList* list, const char* filename);
bool sst_read(const char* filename, uint32_t search_key, uint32_t* out_value);
bool sst_compact(int target_level, int num_files, int out_idx);

#endif