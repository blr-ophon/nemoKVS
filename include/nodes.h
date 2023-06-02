#ifndef NODES_H
#define NODES_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#include "kvpair.h"


//| type | nkeys |  children  |   offsets  | key-values
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...

//TODO:for external memory
//-children becomes an array of pointers to disk pages (uint64_t)
//-each page contains a node
//
enum NODE_TYPE{
    NT_INT,          //internal 
    NT_EXT,          //external
    NT_ROOT
};


typedef struct BPtreeNode{
    uint16_t type;                   //internal or external
    uint16_t nkeys;                  //number of keys
    size_t dataSize;                 //size of key_values in bytes
    struct BPtreeNode **children;    //Array of pointers to children
    uint16_t *keyOffsets;            //Array of offsets to key values
    uint8_t *key_values;             //array of bytes with key values
}BPtreeNode;


void BPtreeNode_print(BPtreeNode *node);

BPtreeNode *BPtreeNode_create(uint8_t n);
void BPtreeNode_free(BPtreeNode *node);

BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv);

BPtreeNode *BPtreeNode_split(struct BPtreeNode *node);
BPtreeNode *BPtreeNode_merge(BPtreeNode *node, BPtreeNode *splitted);

KVpair *BPtreeNode_getKV(BPtreeNode *node, int idx);
void BPtreeNode_appendKV(BPtreeNode *node, int idx, KVpair *kv);



//LATER
BPtreeNode *BPtreeNode_pageLoad(FILE *page);
int BPtreeNode_pageDump(FILE *page, BPtreeNode *node);

#endif
