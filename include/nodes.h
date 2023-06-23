#ifndef NODES_H
#define NODES_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#include "kvpair.h"
#include "pager.h"


//| type | nkeys |  children  |   offsets  | key-values
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...

//TODO:for external memory
//-children becomes an array of pointers to disk pages (uint64_t)
//-each page contains a node
//
enum NODE_TYPE{
    NT_INT = 'I',          //internal 
    NT_EXT = 'E',          //external
    NT_ROOT = 'R'
};


typedef struct BPtreeNode{
    uint16_t type;                   //internal or external
    uint16_t nkeys;                  //number of keys
    uint64_t dataSize;               //size of key_values in bytes
    //struct BPtreeNode **children;  //Array of pointers to children
    uint64_t *childLinks;            //Array of page indexes
    uint16_t *keyOffsets;            //Array of offsets to key values
    uint8_t *key_values;             //array of bytes with key values
}BPtreeNode;

void BPtreeNode_print(BPtreeNode *node);

BPtreeNode *BPtreeNode_create(uint8_t n);
void BPtreeNode_free(BPtreeNode *node);

int BPtreeNode_search(BPtreeNode* node, KVpair *kv);
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *idx);
BPtreeNode *BPtreeNode_delete(BPtreeNode *node, KVpair *kv, int *idx);

BPtreeNode *BPtreeNode_split(PageTable *t, BPtreeNode *node);                   //ONLY FUNCTION THAT WRITES
//BPtreeNode *BPtreeNode_mergeSplitted(BPtreeNode *node, BPtreeNode *splitted);
BPtreeNode *BPtreeNode_shinMergeSplitted(BPtreeNode *node, BPtreeNode *splitted, int ptospl_idx);
BPtreeNode *BPtreeNode_shrink(BPtreeNode *node, int child_idx);

KVpair *BPtreeNode_getKV(BPtreeNode *node, int idx);
void BPtreeNode_appendKV(BPtreeNode *node, int idx, KVpair *kv);

int BPtreeNode_getSize(BPtreeNode *node);
uint8_t *BPtreeNode_encode(BPtreeNode *node);
BPtreeNode *BPtreeNode_decode(uint8_t *bytestream);

int nodeWrite(PageTable *table, BPtreeNode *node);
BPtreeNode *nodeRead(PageTable *table, int page_n);
void nodeOverwrite(PageTable *table, uint64_t page_n, BPtreeNode *node);
void linkUpdate(PageTable *t, uint64_t node_pid, int child_id, uint64_t newLink);
void node_free(PageTable *t, uint64_t node_pid);

#endif
