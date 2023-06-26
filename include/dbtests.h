#ifndef DBTESTS_H
#define DBTESTS_H
#include "bptree.h"

void DBtests_all(PageTable *t,BPtree *tree, int n);

void DBtests_custom(PageTable *t, BPtree *tree);

void DBtests_inorder(PageTable *t, BPtree *tree, int n);

void DBtests_revorder(PageTable *t, BPtree *tree, int n);

int DBtests_randorder(PageTable *t, BPtree *tree, int n);

int DBtests_search(PageTable *t, BPtree *tree, KVpair **KVs, int n);

#endif
