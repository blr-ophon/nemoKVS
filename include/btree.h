#ifndef BTREE_H
#define BTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

////////////////////////////////////////////////////////////////////////////////

//the children of key[i] are always children[i] and children[i+1]
typedef struct BtreeNode{
    uint8_t size;                   //number of keys
    uint32_t *keys;                 //array of keys
    struct BtreeNode **children;    //array of children
    void *record;                   //whatever is stored
}BtreeNode;

BtreeNode *BtreeNode_create(uint8_t n);
void BtreeNode_free(BtreeNode *node);

void BtreeNode_insert(BtreeNode *node, uint32_t key, void *val);
void BtreeNode_split(struct BtreeNode **node);
void BtreeNode_merge(BtreeNode *node, BtreeNode *p);

////////////////////////////////////////////////////////////////////////////////

typedef struct Btree{
    uint8_t degree;
    struct BtreeNode *root;
}Btree;

Btree *Btree_create(uint8_t degree);
void Btree_free(Btree *btree);

BtreeNode *Btree_search(Btree *btree, uint32_t key, int *idx);
void Btree_insert(Btree *btree, BtreeNode *root, uint32_t key, void *val);
void Btree_delete(Btree *btree, void *val);

////////////////////////////////////////////////////////////////////////////////

#endif
