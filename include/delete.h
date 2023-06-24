#ifndef DELETE_H
#define DELETE_H

#include "bptree.h"


//Delete a kv pair from a tree
void BPT_delete(PageTable *t, BPtree* tree, KVpair *kv);

//Returns the leftmost kv pair in a subtree
KVpair *BPtree_getLeftmostKV(PageTable *t, int subTreeRoot_id);

//Inserts a kv pair with empty children at the beginning of node
//Used because insert doesn't necessarily do this when comparing 
//equal keys
BPtreeNode *BPTNode_prepend(BPtreeNode *node, KVpair *kv);

//Borrow a kv pair from src to dst
BPtreeNode *BPTNode_borrow(PageTable *t, BPtreeNode *p, int pKV_idx, bool fromRight);

//Merges inferior(leftmost), superior(rightmost) and a parent node (if internal). Opposite of split
BPtreeNode *BPTNode_merge(PageTable *t, BPtreeNode *node, int kv_idx);

//Swap keys in internal nodes and overwrites, else link changes would cascade to root. 
BPtreeNode *BPTNode_swapKey(BPtreeNode *node, int kv_idx, KVpair *newKV);

#endif
