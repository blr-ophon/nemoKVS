#ifndef BPTREE_H
#define BPTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#include "nodes.h"
#include "kvpair.h"

typedef struct{
    uint8_t degree;
    BPtreeNode *root;
}BPtree;

BPtree *BPtree_create(uint8_t degree);
void BPtree_free(BPtree *ptr);

bool BPtree_insertR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv);
BPtreeNode *BPtree_search(BPtree *btree, uint32_t key, int *idx);

#endif
