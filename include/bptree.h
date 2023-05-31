#ifndef BPTREE_H
#define BPTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>


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

uint32_t KVpair_getKey(KVpair *ptr);        //string to uint32t

KVpair *KVpair_decode(uint8_t *data);
uint8_t *KVpair_encode(KVpair *kv);

//NODE
////////////////////////////////////////////////////////////////////////////////

//| type | nkeys |  children  |   offsets  | key-values
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...

//TODO:for external memory
//-children becomes an array of pointers to disk pages (uint64_t)
//-each page contains a node
//
enum NODE_TYPE{
    NT_INT,         //internal 
    NT_EXT          //external
};


typedef struct BPtreeNode{
    uint16_t type;                   //internal or external
    uint16_t nkeys;                  //number of keys
    struct BPtreeNode **children;    //Array of pointers to children
    uint16_t *keyOffsets;            //Array of offsets to key values
    uint8_t *key_values;             //array of bytes with key values
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
