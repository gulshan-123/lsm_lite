#include "lsm_skiplist.h"
#include <stdlib.h>
#include <stdio.h>

SkipList* sl_create(int capacity, void* list_mem, void* nodes_mem) {
    SkipList* list = list_mem;
    if (list_mem == NULL)
        list = (SkipList*)malloc(sizeof(SkipList));
    list->capacity = capacity;

    list->nodes = nodes_mem;
    if (nodes_mem == NULL)
        list->nodes = (SkipNode*)malloc(sizeof(SkipNode) * capacity);
    
    // Initialize the free list pool. 
    // next[0] acts as the pointer to the next free node.
    for (int i = 0; i < capacity - 1; i++) {
        list->nodes[i].next[0] = i + 1;
    }
    list->nodes[capacity - 1].next[0] = LSM_NULL_INDEX;
    list->free_head_idx = 1; // 0 is reserved for the head node

    // Initialize head node (index 0)
    list->head_idx = 0;
    for (int i = 0; i < MAX_LEVEL; i++) {
        list->nodes[0].next[i] = LSM_NULL_INDEX;
    }
    list->current_level = 1;

    return list;
}

// Internal function to grab a node from the pool
static int allocate_node(SkipList* list) {
    int new_idx = list->free_head_idx;
    if (new_idx == LSM_NULL_INDEX) {
        // Pool exhausted. In Week 2, this is where we trigger mt_switch_and_flush()
        return LSM_NULL_INDEX; 
    }
    // Advance the free list head
    list->free_head_idx = list->nodes[new_idx].next[0];
    return new_idx;
}

bool sl_insert(SkipList* list, uint32_t key, uint32_t value) {
    int update[MAX_LEVEL];
    int current = list->head_idx;

    // Traverse and record the path
    for (int i = list->current_level - 1; i >= 0; i--) {
        while (list->nodes[current].next[i] != LSM_NULL_INDEX &&
               list->nodes[list->nodes[current].next[i]].key < key) {
            current = list->nodes[current].next[i];
        }
        update[i] = current;
    }

    // Check for duplicate (if you want to overwrite, do it here)
    int next_idx = list->nodes[current].next[0];
    if (next_idx != LSM_NULL_INDEX && list->nodes[next_idx].key == key) {
        list->nodes[next_idx].value = value;
        return true;
    }

    // Allocate new node
    int new_idx = allocate_node(list);
    if (new_idx == LSM_NULL_INDEX) {
        fprintf(stderr, "LSM: node pool exhausted\n");
        return false;
    }

    // Roll random level (simplified)
    int new_level = 1; 
    while ((rand() % SKIPLIST_P) == 0 && new_level < MAX_LEVEL) {
        new_level++;
    }

    if (new_level > list->current_level) {
        for (int i = list->current_level; i < new_level; i++) {
            update[i] = list->head_idx;
        }
        list->current_level = new_level;
    }

    // Insert the node
    list->nodes[new_idx].key = key;
    list->nodes[new_idx].value = value;
    for (int i = 0; i < new_level; i++) {
        list->nodes[new_idx].next[i] = list->nodes[update[i]].next[i];
        list->nodes[update[i]].next[i] = new_idx;
    }

    return true;
}

