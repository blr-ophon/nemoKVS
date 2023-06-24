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
                                     
    uint64_t *childLinks;            //Array of page indexes
    uint16_t *keyOffsets;            //Array of offsets to key values
    uint8_t *key_values;             //array of bytes with key values
}BPtreeNode;


BPtreeNode *BPtreeNode_create(uint8_t nkeys, int type);
void BPtreeNode_free(BPtreeNode *node);

//auxiliary
void BPtreeNode_print(BPtreeNode *node);
KVpair *BPtreeNode_getKV(BPtreeNode *node, int Kidx);
void BPtreeNode_appendKV(BPtreeNode *node, int Kidx, KVpair *kv);
int BPtreeNode_search(BPtreeNode* node, KVpair *kv);

//operations (insert)
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *ret_Kidx);
BPtreeNode *BPtreeNode_split(PageTable *t, BPtreeNode *node);                   
BPtreeNode *BPtreeNode_mergeSplitted(BPtreeNode *node, BPtreeNode *splitted, int ptospl_Cidx);

//operations (delete)
BPtreeNode *BPtreeNode_shrink(BPtreeNode *node, int del_Cidx);
BPtreeNode *BPtreeNode_delete(BPtreeNode *node, KVpair *kv, int *ret_Kidx);



//pager related
int BPtreeNode_getSize(BPtreeNode *node);
uint8_t *BPtreeNode_encode(BPtreeNode *node);
BPtreeNode *BPtreeNode_decode(uint8_t *bytestream);

int nodeWrite(PageTable *table, BPtreeNode *node);
BPtreeNode *nodeRead(PageTable *table, uint64_t Pidx);
void nodeOverwrite(PageTable *table, uint64_t Pidx, BPtreeNode *node);
void node_free(PageTable *t, uint64_t node_Pidx);
void linkUpdate(PageTable *t, uint64_t Pidx, int Cidx, uint64_t newLink);

#endif
