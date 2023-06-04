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
    //root 
}BPtree;

void BPtree_print(BPtree *tree);

BPtree *BPtree_create(uint8_t degree);
void BPtree_free(BPtree *ptr);

void BPtree_insert(BPtree *tree, KVpair *kv);
BPtreeNode *BPtree_search(BPtree *btree, KVpair *kv, int *idx);
void BPtree_delete(BPtree *tree, KVpair *kv);

#endif
