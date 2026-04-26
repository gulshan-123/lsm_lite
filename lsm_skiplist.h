#ifndef LSM_SKIPLIST_H
#define LSM_SKIPLIST_H

#include <stdint.h>
#include <stdbool.h>

#define MAX_LEVEL 12
#define LSM_NULL_INDEX -1
#define SKIPLIST_P 4

// A node in the skip list. 
// Instead of pointers, we use integer indices to prepare for shared memory.
typedef struct {
    uint32_t key;   // Using uint32 for standalone testing. Will be byte arrays later.
    uint32_t value;
    int next[MAX_LEVEL]; 
} SkipNode;

// The main struct. In Week 2, this entire struct will live in Postgres dsa_area.
typedef struct {
    SkipNode* nodes;      // The pre-allocated pool of nodes
    int capacity;         // Max number of nodes
    int head_idx;         // Index of the dummy head node
    int free_head_idx;    // Head of the free list for O(1) allocation
    int current_level;
} SkipList;

// Initialization and memory management
SkipList* sl_create(int capacity);

// Core operations
bool sl_insert(SkipList* list, uint32_t key, uint32_t value);
bool sl_search(SkipList* list, uint32_t key, uint32_t* out_value);

#endif