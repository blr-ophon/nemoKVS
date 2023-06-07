#ifndef DBTESTS_H
#define DBTESTS_H
#include "bptree.h"

void DBtests_all(BPtree *tree, int n);

void DBtests_inorder(BPtree *tree, int n);

void DBtests_revorder(BPtree *tree, int n);

void DBtests_randorder(BPtree *tree, int n);

#endif
