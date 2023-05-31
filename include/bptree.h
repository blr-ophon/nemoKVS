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
    char *key;
    uint8_t *val;
}KVpair;

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, char *key, void *val);
void KVpair_free(KVpair *ptr);

uint32_t KVpair_getKey(KVpair *ptr);

//NODE
////////////////////////////////////////////////////////////////////////////////

enum NODE_TYPE{
    NT_IN,
    NT_EX
};

typedef struct BPtreeNode{
    uint8_t type;                   //internal or external
    uint8_t nkeys;                  //number of keys

    //Array of pointers to children
    struct BPtreeNode **children;   //(8bytes * (nkeys+1));

    //Array of pointers to key values
    KVpair **keyOffsets;            //(8bytes * nkeys);

    //array of bytes with key values
    uint8_t *key_values;            //(nkeys*klen + nkeys*vlen)
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

void BPtree_insert(BPtree *tree, BPtreeNode *root, KVpair *kv);
BPtreeNode *BPtree_search(BPtree *btree, uint32_t key, int *idx);

#endif
