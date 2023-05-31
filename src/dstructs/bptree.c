#include "bptree.h"

//KV-PAIR
////////////////////////////////////////////////////////////////////////////////

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, void *key, void *val){
    KVpair *kv = malloc(sizeof(KVpair));

    kv->klen = klen;
    kv->vlen = vlen;

    kv->key = malloc(klen);
    memcpy(kv->key, key, klen);

    kv->val = malloc(vlen);
    memcpy(kv->val, val, vlen);

    return kv;
}

void KVpair_free(KVpair *ptr){
    if(ptr->key) free(ptr->key);
    if(ptr->val) free(ptr->val);
    if(ptr) free(ptr);
}

//NODE
////////////////////////////////////////////////////////////////////////////////

BPtreeNode *BPtreeNode_create(uint8_t n){
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    rv->children = calloc(n, sizeof(void*));
    rv->keyOffsets = calloc(n, sizeof(uint32_t));
    return rv;
}

void BPtreeNode_free(BPtreeNode *node){
    free(node->children);
    free(node->keyOffsets);
    //TODO free record
    free(node);
}


//TREE
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

