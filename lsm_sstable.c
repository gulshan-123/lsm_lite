#include "lsm_sstable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef struct {
    FILE* fp;
    uint64_t end_offset;
    uint32_t current_key;
    uint32_t current_val;
    bool eof;
} MergeStream;



// Fast, lightweight 32-bit hash (FNV-1a)
static uint32_t hash32(uint32_t key, uint32_t seed) {
    uint32_t hash = seed;
    hash ^= key;
    hash *= 0x01000193; 
    return hash;
}

// Checks the 7 bits. Returns true if it MIGHT exist. Returns false if it DEFINITELY does not.
static bool bloom_check(uint8_t* filter, uint32_t num_bits, uint32_t key) {
    uint32_t h1 = hash32(key, 0x811C9DC5);
    uint32_t h2 = hash32(key, 0x12345678);
    for (int i = 0; i < 7; i++) {
        uint32_t bit_idx = (h1 + i * h2) % num_bits;
        if (!(filter[bit_idx / 8] & (1 << (bit_idx % 8)))) {
            return false;
        }
    }
    return true;
}

// Dynamically sets bits using 7 simulated hash functions
static void bloom_add(uint8_t* filter, uint32_t num_bits, uint32_t key) {
    uint32_t h1 = hash32(key, 0x811C9DC5);
    uint32_t h2 = hash32(key, 0x12345678);
    for (int i = 0; i < 7; i++) {
        uint32_t bit_idx = (h1 + i * h2) % num_bits;
        filter[bit_idx / 8] |= (1 << (bit_idx % 8));
    }
}

bool sst_write(SkipList* list, const char* filename) {
    int current = list->nodes[list->head_idx].next[0];
    if (current == LSM_NULL_INDEX) {
        return false; // Empty list
    }

    FILE* fp = fopen(filename, "wb");
    if (!fp) return false;

    // 1. Dynamic Bloom Filter Allocation (10 bits per key)
    uint32_t max_keys = list->capacity; 
    uint32_t num_bits = max_keys * 10;
    uint32_t bloom_bytes = (num_bits + 7) / 8;
    num_bits = bloom_bytes * 8;
    uint8_t* bloom_filter = calloc(bloom_bytes, 1);

    // Dynamic array for sparse index. Capacity / interval + 1 is the max possible entries.
    int max_sparse_entries = ((list->capacity * 16) / BLOCK_SIZE) + 2;
    uint32_t* sparse_keys = malloc(max_sparse_entries * sizeof(uint32_t));
    uint64_t* sparse_offsets = malloc(max_sparse_entries * sizeof(uint64_t));
    
    int sparse_count = 0;
    uint64_t last_block_offset = 0;

    int total_count = 0;

    // 1. WRITE DATA BLOCKS
    while (current != LSM_NULL_INDEX) {
        uint32_t key = list->nodes[current].key;
        uint32_t val = list->nodes[current].value;
        uint64_t current_offset = ftell(fp);

        // Force the first key, or record if we crossed 4096 bytes
        if (sparse_count == 0 || (current_offset - last_block_offset) >= BLOCK_SIZE) {
            sparse_keys[sparse_count] = key;
            sparse_offsets[sparse_count] = current_offset;
            sparse_count++;
            last_block_offset = current_offset; // Reset the 4KB tracker
        }

        // Populate Bloom Filter
        bloom_add(bloom_filter, num_bits, key);

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
    fwrite(bloom_filter, sizeof(uint8_t), bloom_bytes, fp);

    // 4. WRITE FOOTER
    fwrite(&sparse_index_offset, sizeof(uint64_t), 1, fp);
    fwrite(&bloom_filter_offset, sizeof(uint64_t), 1, fp);
    fwrite(&bloom_bytes, sizeof(uint32_t), 1, fp);

    free(sparse_keys);
    free(sparse_offsets);
    free(bloom_filter);
    fclose(fp);
    return true;
}

bool sst_read(const char* filename, uint32_t search_key, uint32_t* out_value) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) return false;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 20) {
        close(fd);
        return false;
    }
    size_t file_size = st.st_size;

    // === 1. MEMORY MAP THE ENTIRE FILE (ZERO-COPY) ===
    uint8_t* map = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map == MAP_FAILED) {
        close(fd);
        return false;
    }

    // === 2. SAFE FOOTER PARSING (Pointer Math) ===
    uint64_t sparse_offset, bloom_offset;
    uint32_t bloom_bytes;
    memcpy(&sparse_offset, map + file_size - 20, sizeof(uint64_t));
    memcpy(&bloom_offset, map + file_size - 12, sizeof(uint64_t));
    memcpy(&bloom_bytes, map + file_size - 4, sizeof(uint32_t));

    // === 3. ZERO-COPY BLOOM CHECK ===
    uint8_t* bloom_filter = map + bloom_offset;
    uint32_t num_bits = bloom_bytes * 8;
    if (!bloom_check(bloom_filter, num_bits, search_key)) {
        munmap(map, file_size);
        close(fd);
        return false;
    }

    // === 4. ZERO-COPY SPARSE INDEX BINARY SEARCH ===
    int sparse_count;
    memcpy(&sparse_count, map + sparse_offset, sizeof(int));
    
    // The interleaved array starts right after the count
    uint8_t* index_ptr = map + sparse_offset + sizeof(int); 
    
    int left = 0, right = sparse_count - 1;
    int best_idx = 0;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        uint32_t mid_key;
        // Key is at offset 0, Size is 4. Total record size is 12 (4 key + 8 offset).
        memcpy(&mid_key, index_ptr + (mid * 12), sizeof(uint32_t)); 

        if (mid_key == search_key) {
            best_idx = mid;
            break;
        } else if (mid_key < search_key) {
            best_idx = mid;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    // Extract the block offset for our best match
    uint64_t data_offset;
    memcpy(&data_offset, index_ptr + (best_idx * 12) + 4, sizeof(uint64_t));

    // === 5. ZERO-COPY DATA SCAN ===
    uint64_t end_of_data = sparse_offset;
    uint8_t* data_ptr = map + data_offset;
    bool found = false;

    // Scan the 4KB block directly in the memory-mapped space
    while (data_ptr < map + end_of_data) {
        uint32_t k;
        // Skip 4-byte KeyLen, read Key
        memcpy(&k, data_ptr + 4, sizeof(uint32_t));
        
        if (k == search_key) {
            // Skip 4-byte KeyLen + 4-byte Key + 4-byte ValLen, read Val
            memcpy(out_value, data_ptr + 12, sizeof(uint32_t));
            found = true;
            break;
        }
        if (k > search_key) break;
        
        data_ptr += 16; // Advance to the next 16-byte record
    }

    // === 6. CLEANUP ===
    munmap(map, file_size); // Instantly releases the mapping
    close(fd);
    return found;
}

bool sst_compact(int target_level, int num_files, int out_idx) {
    char out_name[256];
    snprintf(out_name, sizeof(out_name), "lsm_data/L%d_%d.sst", target_level + 1, out_idx);
    FILE* out_fp = fopen(out_name, "wb");
    if (!out_fp) return false;

    MergeStream* streams = malloc(num_files * sizeof(MergeStream));

    uint64_t total_input_size = 0;
    // 1. Initialize streams (Open all L0 files)
    for (int i = 0; i < num_files; i++) {
        char in_name[256];
        snprintf(in_name, sizeof(in_name), "lsm_data/L%d_%d.sst", target_level, i);
        streams[i].fp = fopen(in_name, "rb");
        
        // Find where data ends by reading footer
        fseek(streams[i].fp, -20, SEEK_END);
        fread(&streams[i].end_offset, sizeof(uint64_t), 1, streams[i].fp);
        
        total_input_size += streams[i].end_offset;

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
    uint32_t estimated_keys = (total_input_size / 16) + 1000; // +1000 safety buffer
    uint32_t num_bits = estimated_keys * 10;
    uint32_t bloom_bytes = (num_bits + 7) / 8;
    num_bits = bloom_bytes * 8;
    uint8_t* bloom_filter = calloc(bloom_bytes, 1);
    uint64_t total_data_bytes = 0;
    for (int i = 0; i < num_files; i++) {
        total_data_bytes += streams[i].end_offset; // end_offset = start of sparse index = end of data
    }

    // Each KV record is exactly 16 bytes (4+4+4+4)
    size_t max_sparse = (total_data_bytes / BLOCK_SIZE) + 2;
    uint32_t* sparse_keys = malloc(max_sparse * sizeof(uint32_t));
    uint64_t* sparse_offsets = malloc(max_sparse * sizeof(uint64_t));
    
    int sparse_count = 0;
    uint64_t last_block_offset = 0;
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
        if (sparse_count == 0 || (current_offset - last_block_offset) >= BLOCK_SIZE) {
            sparse_keys[sparse_count] = min_key;
            sparse_offsets[sparse_count] = current_offset;
            sparse_count++;
            last_block_offset = current_offset;
        }
        bloom_add(bloom_filter, num_bits, min_key); // Use your existing helper

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
    fwrite(bloom_filter, sizeof(uint8_t), bloom_bytes, out_fp);
    fwrite(&sparse_index_offset, sizeof(uint64_t), 1, out_fp);
    fwrite(&bloom_filter_offset, sizeof(uint64_t), 1, out_fp);
    fwrite(&bloom_bytes, sizeof(uint32_t), 1, out_fp);

    // 4. Cleanup and Delete Old Files
    fclose(out_fp);
    for (int i = 0; i < num_files; i++) {
        fclose(streams[i].fp);
    }
    free(bloom_filter);
    free(streams);
    free(sparse_keys);
    free(sparse_offsets);
    return true;
}

