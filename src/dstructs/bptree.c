#include "bptree.h"


void BPtree_print(BPtree *tree){
    if(!tree){
        printf("> Empty tree\n");
        return;
    }
    printf("\n====== TREE ======\n");
    printf("Degree: %d\n", tree->degree); 
    printf("Root:");
    BPtreeNode_print(tree->root);
    //TODO: print all nodes of the tree in order
}

BPtree *BPtree_create(uint8_t degree){
    BPtree *rv = malloc(sizeof(BPtree));
    rv->degree = degree;

    //master root
    rv->root = BPtreeNode_create(0);
    rv->root->type = NT_ROOT;
    rv->root->nkeys = 0;
    rv->root->children[0] = NULL;       //tree root

    return rv;
}

static void BPtree_free_rec(BPtreeNode *node){
    if(node == NULL) return;

    for(int i = 0; i < node->nkeys; i++){
        BPtree_free_rec(node->children[i]);
    }
    BPtreeNode_free(node);
}

void BPtree_free(BPtree *ptr){
    BPtree_free_rec(ptr->root);
    free(ptr);
}

//returns ID of the next child. Used to traverse the tree
static int NextChildIDX(BPtreeNode *node, KVpair *kv){
    for(int i = 0; i < node->nkeys; i++){
        //key is bigger than every node key
        if(i == node->nkeys){ 
            //go to highest child
            return i+1;
        }

        KVpair *crntKV = BPtreeNode_getKV(node, i);
        KVpair *nextKV = BPtreeNode_getKV(node, i+1);

        //if kv inferior to current kv
        if(KVpair_compare(kv, crntKV) < 0){
            //go to left child
            KVpair_free(crntKV);
            KVpair_free(nextKV);
            return i;
        }

        //kv is between kv i and i+1 of node
        if(KVpair_compare(kv, crntKV) >= 0 && KVpair_compare(kv, nextKV) < 0){
            //go to right child
            KVpair_free(crntKV);
            KVpair_free(nextKV);
            return i+1;
        }

        KVpair_free(crntKV);
        KVpair_free(nextKV);

        //none of the cases:
        //key is bigger than current and next node key increment i and continue
    }
    
    return 0;
}

static bool BPtree_insertR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv){
    //TODO: set node type correctly 
    bool split = false;

    if(node->type == NT_EXT){
        //insert kv
        BPtreeNode *inserted = BPtreeNode_insert(node, kv);
        inserted->type = NT_EXT;
        node = inserted;
        //link parent to new node with inserted value
        p->children[NextChildIDX(p, kv)] = inserted;

    }else{
        //traverse next children
        BPtreeNode *next = node->children[NextChildIDX(node, kv)];
        split = BPtree_insertR(tree, next, node, kv);
    }

    if(split){
        //merge node with it`s child that splitted
        BPtreeNode *splittedChild = node->children[NextChildIDX(node,kv)];
        BPtreeNode *merged = BPtreeNode_merge(splittedChild, node);
        //link parent to new merged node
        p->children[NextChildIDX(p, kv)] = merged;
    }

    //if insert or merging makes node full
    if(node->nkeys >= tree->degree){
        //split
        BPtreeNode *splitted = BPtreeNode_split(node);
        //link parent to new splitted node
        p->children[NextChildIDX(p, kv)] = splitted;
        split = true;
    }

    return split;
}

void BPtree_insert(BPtree *tree, KVpair *kv){
    if(!tree->root->children[0]){
        //Empty tree. Create first node and insert KV
        tree->root->children[0] = BPtreeNode_create(1);
        tree->root->children[0]->type = NT_EXT;
        BPtreeNode_appendKV(tree->root->children[0], 0,  kv);
        return;
    }

    BPtree_insertR(tree, tree->root->children[0], tree->root, kv);
}

//returns node pointer and the id of the key in idx
BPtreeNode *BPtree_search(BPtree *btree, uint32_t key, int *idx){
    //TODO: copy from btree.c
    //traverse until entry or empty node is found
    return NULL;
}
