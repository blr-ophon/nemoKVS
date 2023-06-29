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


//NODE
//| type | nkeys |  children  |   offsets  | key-values
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...
//
//KEY-VALUE
//| klen | vlen |  key |  value  | 
//|  2B  |  2B  | klen |  vlen   | 

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
                                     
    uint64_t *childLinks;            //Array of page indexes
    uint16_t *keyOffsets;            //Array of offsets to key values
    uint8_t *key_values;             //array of bytes with key values
}BPtreeNode;


BPtreeNode *BPtreeNode_create(uint8_t nkeys, int type);
void BPtreeNode_free(BPtreeNode *node);

void BPtreeNode_print(BPtreeNode *node);
KVpair *BPtreeNode_getKV(BPtreeNode *node, int Kidx);
void BPtreeNode_appendKV(BPtreeNode *node, int Kidx, KVpair *kv);
int BPtreeNode_getSize(BPtreeNode *node);



//nodes_op.c
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *ret_Kidx);
BPtreeNode *BPtreeNode_shrink(BPtreeNode *node, int del_Cidx);
BPtreeNode *BPtreeNode_delete(BPtreeNode *node, KVpair *kv, int *ret_Kidx);
int BPtreeNode_search(BPtreeNode* node, KVpair *kv);
BPtreeNode *BPTNode_swapKey(BPtreeNode *node, int swap_Kidx, KVpair *newKV);
BPtreeNode *BPTNode_prepend(BPtreeNode *node, KVpair *kv);

//nodes_insert_op.c 
BPtreeNode *BPtreeNode_split(PageTable *t, BPtreeNode *node);                   
BPtreeNode *BPtreeNode_mergeSplitted(BPtreeNode *node, BPtreeNode *splitted, int ptospl_Cidx);

//nodes_delete_op.c 
//Borrow a kv pair from src to dst
BPtreeNode *BPTNode_borrow(PageTable *t, BPtreeNode *p, int p_Kidx, bool fromRight);
//Merges inferior(leftmost), superior(rightmost) and a parent node (if internal). Opposite of split
BPtreeNode *BPTNode_merge(PageTable *t, BPtreeNode *node, int merge_Kidx);



//node_io.c
uint8_t *BPtreeNode_encode(BPtreeNode *node);
BPtreeNode *BPtreeNode_decode(uint8_t *bytestream);

int nodeWrite(PageTable *table, BPtreeNode *node);
BPtreeNode *nodeRead(PageTable *table, uint64_t Pidx);
void nodeOverwrite(PageTable *table, uint64_t Pidx, BPtreeNode *node);
void node_free(PageTable *t, uint64_t node_Pidx);

#endif
