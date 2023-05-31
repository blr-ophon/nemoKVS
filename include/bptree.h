#ifndef BPTREE_H
#define BPTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

//KV-PAIR
////////////////////////////////////////////////////////////////////////////////

typedef struct{
    uint32_t klen;
    uint32_t vlen;
    uint8_t *key;
    uint8_t *val;
}KVpair;

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, void *key, void *val);
void KVpair_free(KVpair *ptr);

//NODE
////////////////////////////////////////////////////////////////////////////////

typedef struct BPtreeNode{
    uint8_t type;                   //internal or external
    uint8_t nkeys;                  //number of keys
    struct BPtreeNode **children;   //array of children
    void *keyOffsets;               //array of offsets to key_values (8bytes * nkeys);
    KVpair *key_values;            //whatever is stored    (nkeys*klen + nkeys*vlen)
}BPtreeNode;

BPtreeNode *BPtreeNode_create(uint8_t n);
void BPtreeNode_free(BPtreeNode *node);

void BPtreeNode_insert(BPtreeNode *node, KVpair *kv);
void BPtreeNode_split(struct BPtreeNode **node);
void BPtreeNode_merge(BPtreeNode *node, BPtreeNode *p);

//TREE
////////////////////////////////////////////////////////////////////////////////

typedef struct{
    uint8_t degree;
    BPtreeNode *root;
}BPtree;

BPtree *BPtree_create(uint8_t degree);
void BPtree_free(BPtree *ptr);


#endif
