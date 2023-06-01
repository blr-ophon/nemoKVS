#include "bptree.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000

static void assert_page(BPtreeNode *node){
    //assert node fits in a page
}

//key-value offset for item idx
int offsetPos(BPtreeNode *node, int idx){
    
}

////////////////////////////////////////////////////////////////////////////////

BPtree *BPtree_create(uint8_t degree){
    BPtree *rv = malloc(sizeof(BPtree));

    rv->root = BPtreeNode_create(degree);
    rv->degree = degree;

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
    uint32_t key = KVpair_getKey(kv);
    for(int i = 0; i < node->nkeys; i++){
        //key is bigger than every node key
        if(i == node->nkeys){ 
            //go to highest child
            return i+1;
        }

        //key is smaller than node key i
        uint16_t node_key = KVpair_getKey(BPtreeNode_getKV(node, i));
        if(key < node_key){ 
            //go to left child
            return i;
        }

        //key is between nodekey i and i+1
        uint16_t next_node_key = KVpair_getKey(BPtreeNode_getKV(node, i+1));
        if(key >= node_key && key < next_node_key){
            //go to right child
            return i+1;
        }

        //none of the cases:
        //key is bigger than current and next node key increment i and continue
    }
}

bool BPtree_insert(BPtree *tree, BPtreeNode *node, KVpair *kv){
    bool split = false;
    if(!node){
        return false;
    }

    if(node->type == NT_EXT){
        BPtreeNode_insert(node, kv);
    }else{
        //next children
        BPtreeNode *next = node->children[NextChildIDX(node, kv)];
        split = BPtree_insert(tree, next, kv);
    }

    //merge if a split happened
    if(split){
        //merge
    }

    //if insert or merging makes node full
    if(node->nkeys >= tree->degree){
        //split
        split = true;
    }

    return split;
}

//returns node pointer and the id of the key in idx
BPtreeNode *BPtree_search(BPtree *btree, uint32_t key, int *idx){
    //TODO: copy from btree.c
    //traverse until entry or empty node is found
    return NULL;
}
