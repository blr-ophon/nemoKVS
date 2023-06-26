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
#include "pager.h"

typedef struct{
    uint8_t degree;
    uint64_t Mroot_id;
    //root 
}BPtree;

void BPtree_print(PageTable *t, BPtree *tree);

BPtree *BPtree_create(PageTable *t, uint8_t degree);
void BPtree_free(BPtree *ptr);

void BPtree_insert(PageTable *t, BPtree *tree, KVpair *kv);
BPtreeNode *BPtree_search(PageTable *t, BPtree *tree, KVpair *kv, int *ret_idx);
void BPT_delete(PageTable *t, BPtree* tree, KVpair *kv);

int NextChildIDX(BPtreeNode *node, KVpair *kv);

//Returns the leftmost kv pair in a subtree
KVpair *BPtree_getLeftmostKV(PageTable *t, int subTreeRoot_id);
void linkUpdate(PageTable *t, uint64_t Pidx, int Cidx, uint64_t newLink);

#endif
