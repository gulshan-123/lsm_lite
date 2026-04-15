#include "lsm_sstable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    FILE* fp;
    uint64_t end_offset;
    uint32_t current_key;
    uint32_t current_val;
    bool eof;
} MergeStream;



// Simple FNV-1a Hash for the Bloom Filter
static uint32_t hash_fnv1a(uint32_t key) {
    uint32_t hash = 2166136261u;
    uint8_t* bytes = (uint8_t*)&key;
    for (int i = 0; i < 4; i++) {
        hash ^= bytes[i];
        hash *= 16777619;
    }
    return hash;
}

// Helper: Check if a bit is set in the Bloom filter
static bool check_bloom_bit(uint8_t* filter, uint32_t key) {
    uint32_t hash = hash_fnv1a(key);
    uint32_t bit_index = hash % (BLOOM_FILTER_SIZE_BYTES * 8);
    uint32_t byte_index = bit_index / 8;
    uint8_t bit_offset = bit_index % 8;
    return (filter[byte_index] & (1 << bit_offset)) != 0;
}

static void set_bloom_bit(uint8_t* filter, uint32_t key) {
    uint32_t hash = hash_fnv1a(key);
    uint32_t bit_index = hash % (BLOOM_FILTER_SIZE_BYTES * 8);
    uint32_t byte_index = bit_index / 8;
    uint8_t bit_offset = bit_index % 8;
    filter[byte_index] |= (1 << bit_offset);
}

bool sst_write(SkipList* list, const char* filename) {
    int current = list->nodes[list->head_idx].next[0];
    if (current == LSM_NULL_INDEX) {
        return false; // Empty list
    }

    FILE* fp = fopen(filename, "wb");
    if (!fp) return false;

    // Temporary arrays for metadata
    uint8_t bloom_filter[BLOOM_FILTER_SIZE_BYTES];
    memset(bloom_filter, 0, BLOOM_FILTER_SIZE_BYTES);

    // Dynamic array for sparse index. Capacity / interval + 1 is the max possible entries.
    int max_sparse_entries = (list->capacity / SPARSE_INTERVAL) + 1;
    uint32_t* sparse_keys = malloc(max_sparse_entries * sizeof(uint32_t));
    uint64_t* sparse_offsets = malloc(max_sparse_entries * sizeof(uint64_t));
    int sparse_count = 0;

    int total_count = 0;

    // 1. WRITE DATA BLOCKS
    while (current != LSM_NULL_INDEX) {
        uint32_t key = list->nodes[current].key;
        uint32_t val = list->nodes[current].value;
        uint64_t current_offset = ftell(fp);

        // Record sparse index entry
        if (total_count % SPARSE_INTERVAL == 0) {
            sparse_keys[sparse_count] = key;
            sparse_offsets[sparse_count] = current_offset;
            sparse_count++;
        }

        // Populate Bloom Filter
        set_bloom_bit(bloom_filter, key);

        // Write [KeyLen(4)][Key(4)][ValLen(4)][Val(4)]
        uint32_t len = 4;
        fwrite(&len, sizeof(uint32_t), 1, fp);
        fwrite(&key, sizeof(uint32_t), 1, fp);
        fwrite(&len, sizeof(uint32_t), 1, fp);
        fwrite(&val, sizeof(uint32_t), 1, fp);

        current = list->nodes[current].next[0];
        total_count++;
    }

    // 2. WRITE SPARSE INDEX
    uint64_t sparse_index_offset = ftell(fp);
    fwrite(&sparse_count, sizeof(int), 1, fp); // Write how many entries there are
    for (int i = 0; i < sparse_count; i++) {
        fwrite(&sparse_keys[i], sizeof(uint32_t), 1, fp);
        fwrite(&sparse_offsets[i], sizeof(uint64_t), 1, fp);
    }

    // 3. WRITE BLOOM FILTER
    uint64_t bloom_filter_offset = ftell(fp);
    fwrite(bloom_filter, sizeof(uint8_t), BLOOM_FILTER_SIZE_BYTES, fp);

    // 4. WRITE FOOTER
    fwrite(&sparse_index_offset, sizeof(uint64_t), 1, fp);
    fwrite(&bloom_filter_offset, sizeof(uint64_t), 1, fp);

    free(sparse_keys);
    free(sparse_offsets);
    fclose(fp);
    return true;
}

bool sst_read(const char* filename, uint32_t search_key, uint32_t* out_value) {
    FILE* fp = fopen(filename, "rb");
    if (!fp) return false;

    // 1. READ FOOTER
    fseek(fp, -16, SEEK_END);
    uint64_t sparse_offset, bloom_offset;
    fread(&sparse_offset, sizeof(uint64_t), 1, fp);
    fread(&bloom_offset, sizeof(uint64_t), 1, fp);

    // 2. CHECK BLOOM FILTER (In RAM)
    fseek(fp, bloom_offset, SEEK_SET);
    uint8_t bloom_filter[BLOOM_FILTER_SIZE_BYTES];
    fread(bloom_filter, sizeof(uint8_t), BLOOM_FILTER_SIZE_BYTES, fp);
    
    if (!check_bloom_bit(bloom_filter, search_key)) {
        // The magic of LSM: We saved a heavy disk search because the filter said no.
        fclose(fp);
        return false; 
    }

    // 3. LOAD & BINARY SEARCH SPARSE INDEX
    fseek(fp, sparse_offset, SEEK_SET);
    int sparse_count;
    fread(&sparse_count, sizeof(int), 1, fp);
    
    uint32_t* sparse_keys = malloc(sparse_count * sizeof(uint32_t));
    uint64_t* sparse_offsets = malloc(sparse_count * sizeof(uint64_t));
    
    // FIX: Read interleaved exactly as it was written
    for (int i = 0; i < sparse_count; i++) {
        fread(&sparse_keys[i], sizeof(uint32_t), 1, fp);
        fread(&sparse_offsets[i], sizeof(uint64_t), 1, fp);
    }

    // Find the largest sparse key that is <= search_key
    int left = 0, right = sparse_count - 1;
    int best_idx = 0;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        if (sparse_keys[mid] == search_key) {
            best_idx = mid;
            break; // Exact match in index
        } else if (sparse_keys[mid] < search_key) {
            best_idx = mid; 
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    // 4. EXACT DISK SEEK & LINEAR SCAN
    fseek(fp, sparse_offsets[best_idx], SEEK_SET);
    uint64_t end_of_data = sparse_offset; // Data blocks stop where sparse index begins
    bool found = false;

    while (ftell(fp) < end_of_data) {
        uint32_t klen, k, vlen, v;
        fread(&klen, sizeof(uint32_t), 1, fp);
        fread(&k, sizeof(uint32_t), 1, fp);
        fread(&vlen, sizeof(uint32_t), 1, fp);
        fread(&v, sizeof(uint32_t), 1, fp);

        if (k == search_key) {
            *out_value = v;
            found = true;
            break;
        }
        if (k > search_key) {
            // Because the file is perfectly sorted, if we pass the key, it doesn't exist.
            break; 
        }
    }

    free(sparse_keys);
    free(sparse_offsets);
    fclose(fp);
    return found;
}

bool sst_compact(int target_level, int num_files, int out_idx) {
    char out_name[256];
    snprintf(out_name, sizeof(out_name), "lsm_data/L%d_%d.sst", target_level + 1, out_idx);
    FILE* out_fp = fopen(out_name, "wb");
    if (!out_fp) return false;

    MergeStream* streams = malloc(num_files * sizeof(MergeStream));
    
    // 1. Initialize streams (Open all L0 files)
    for (int i = 0; i < num_files; i++) {
        char in_name[256];
        snprintf(in_name, sizeof(in_name), "lsm_data/L%d_%d.sst", target_level, i);
        streams[i].fp = fopen(in_name, "rb");
        
        // Find where data ends by reading footer
        fseek(streams[i].fp, -16, SEEK_END);
        fread(&streams[i].end_offset, sizeof(uint64_t), 1, streams[i].fp);
        fseek(streams[i].fp, 0, SEEK_SET);

        // Pre-load the first KV pair
        if (ftell(streams[i].fp) < streams[i].end_offset) {
            uint32_t len;
            fread(&len, sizeof(uint32_t), 1, streams[i].fp); // key len
            fread(&streams[i].current_key, sizeof(uint32_t), 1, streams[i].fp);
            fread(&len, sizeof(uint32_t), 1, streams[i].fp); // val len
            fread(&streams[i].current_val, sizeof(uint32_t), 1, streams[i].fp);
            streams[i].eof = false;
        } else {
            streams[i].eof = true;
        }
    }

    // Prepare metadata for the output file
    uint8_t bloom_filter[BLOOM_FILTER_SIZE_BYTES];
    memset(bloom_filter, 0, BLOOM_FILTER_SIZE_BYTES);
    
    int max_sparse = 100000; // Simplified capacity for brevity
    uint32_t* sparse_keys = malloc(max_sparse * sizeof(uint32_t));
    uint64_t* sparse_offsets = malloc(max_sparse * sizeof(uint64_t));
    int sparse_count = 0;
    int total_count = 0;

    // 2. The K-Way Merge Loop
    while (true) {
        // Find the absolute minimum key across all active streams
        uint32_t min_key = UINT32_MAX;
        int best_stream = -1;

        for (int i = 0; i < num_files; i++) {
            if (!streams[i].eof && streams[i].current_key <= min_key) {
                min_key = streams[i].current_key;
                // If keys are equal, higher stream index (i) overrides 
                // because it represents the newer file!
                best_stream = i; 
            }
        }

        if (best_stream == -1) break; // All files are fully read!

        uint32_t final_val = streams[best_stream].current_val;

        // Write to output file
        uint64_t current_offset = ftell(out_fp);
        if (total_count % SPARSE_INTERVAL == 0) {
            sparse_keys[sparse_count] = min_key;
            sparse_offsets[sparse_count] = current_offset;
            sparse_count++;
        }
        set_bloom_bit(bloom_filter, min_key); // Use your existing helper

        uint32_t len = 4;
        fwrite(&len, sizeof(uint32_t), 1, out_fp);
        fwrite(&min_key, sizeof(uint32_t), 1, out_fp);
        fwrite(&len, sizeof(uint32_t), 1, out_fp);
        fwrite(&final_val, sizeof(uint32_t), 1, out_fp);
        total_count++;

        // Advance ALL streams that had this min_key to bypass duplicate/old data
        for (int i = 0; i < num_files; i++) {
            if (!streams[i].eof && streams[i].current_key == min_key) {
                if (ftell(streams[i].fp) < streams[i].end_offset) {
                    fread(&len, sizeof(uint32_t), 1, streams[i].fp);
                    fread(&streams[i].current_key, sizeof(uint32_t), 1, streams[i].fp);
                    fread(&len, sizeof(uint32_t), 1, streams[i].fp);
                    fread(&streams[i].current_val, sizeof(uint32_t), 1, streams[i].fp);
                } else {
                    streams[i].eof = true;
                }
            }
        }
    }

    // 3. Write Footer exactly as you did in sst_write (Sparse Index, Bloom, Footer)
    uint64_t sparse_index_offset = ftell(out_fp);
    fwrite(&sparse_count, sizeof(int), 1, out_fp);
    for (int i = 0; i < sparse_count; i++) {
        fwrite(&sparse_keys[i], sizeof(uint32_t), 1, out_fp);
        fwrite(&sparse_offsets[i], sizeof(uint64_t), 1, out_fp);
    }
    uint64_t bloom_filter_offset = ftell(out_fp);
    fwrite(bloom_filter, sizeof(uint8_t), BLOOM_FILTER_SIZE_BYTES, out_fp);
    fwrite(&sparse_index_offset, sizeof(uint64_t), 1, out_fp);
    fwrite(&bloom_filter_offset, sizeof(uint64_t), 1, out_fp);

    // 4. Cleanup and Delete Old Files
    fclose(out_fp);
    for (int i = 0; i < num_files; i++) {
        fclose(streams[i].fp);
        char in_name[256];
        snprintf(in_name, sizeof(in_name), "lsm_data/L%d_%d.sst", target_level, i);
        unlink(in_name); // DELETE old file
    }
    
    free(streams);
    free(sparse_keys);
    free(sparse_offsets);
    return true;
}

