#ifndef DELETE_H
#define DELETE_H

#include "bptree.h"

void BPT_delete(BPtree *tree, KVpair *kv);

BPtreeNode *shinBPT_borrow(BPtreeNode *dst, BPtreeNode *src, int dst_idx, bool fromRight, BPtreeNode *p);

BPtreeNode *shinBPT_mergeINT(BPtreeNode *inferior, BPtreeNode *superior, BPtreeNode *p, int idx_from_p);

#endif
