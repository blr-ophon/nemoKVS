#include "bptree.h"

//KV-PAIR
////////////////////////////////////////////////////////////////////////////////

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, char *key, void *val){
    KVpair *kv = malloc(sizeof(KVpair));

    kv->klen = klen;
    kv->vlen = vlen;

    kv->key = malloc(klen);
    strncpy(kv->key, key, klen);

    kv->val = malloc(vlen);
    memcpy(kv->val, val, vlen);

    return kv;
}

void KVpair_free(KVpair *ptr){
    if(ptr->key) free(ptr->key);
    if(ptr->val) free(ptr->val);
    if(ptr) free(ptr);
}

uint32_t KVpair_getKey(KVpair *ptr){
    return atol(ptr->key);
}

//NODE
////////////////////////////////////////////////////////////////////////////////

//create a node with size n
BPtreeNode *BPtreeNode_create(uint8_t nkeys){
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    rv->children = calloc(nkeys, sizeof(void*));
    rv->keyOffsets = calloc(nkeys, sizeof(void*));

    rv->nkeys = nkeys;
    return rv;
}

void BPtreeNode_free(BPtreeNode *node){
    free(node->children);
    free(node->keyOffsets);
    //TODO free record
    free(node);
}

void BPtreeNode_insert(BPtreeNode *node, KVpair *kv){
    //type set and check of split is done by BPtree_insert
    if(!node){
        node = BPtreeNode_create(0);
    }

    //append kv to key_values
    
    //Update keyOffsets and children links
    
    node->nkeys++;
}


//TREE
////////////////////////////////////////////////////////////////////////////////

//returns node pointer and the id of the key in idx
BPtreeNode *BPtree_search(BPtree *btree, uint32_t key, int *idx){
    //TODO: copy from btree.c
    return NULL;
}

void BPtree_insert(BPtree *tree, BPtreeNode *root, KVpair *kv){
    uint32_t key = KVpair_getKey(kv);
    BPtreeNode *tmp = root;
    BPtreeNode *p;   //parent node
                    
    //case 1: empty tree 
    if(!tmp){
        //TODO
        return;
    }

    //traverse until leaf node is found
    int i;
    while(tmp != NULL){
        p = tmp;
        for(i = 0; i < tmp->nkeys; i++){
            //key is bigger than every node key
            if(i == tmp->nkeys){ 
                //go to highest child
                tmp = tmp->children[i+1];
                break;
            }

            //key is smaller than node key i
            if(key < KVpair_getKey(tmp->keyOffsets[i])){ 
                //go to left child
                tmp = tmp->children[i];
                break;
            }
            //key is between nodekey i and i+1
            if(key >= KVpair_getKey(tmp->keyOffsets[i]) && key < KVpair_getKey(tmp->keyOffsets[i+1])){ 
                //go to right child
                tmp = tmp->children[i+1];
                break;
            }
        }
    }

    //case 2: leaf node with empty space
    if(p->nkeys < tree->degree){
        BPtreeNode_insert(p, kv);
        
    }else{ 
        //case 3: leaf node full 
        //split and insert 
        //TODO: analyze what happens when parent becomes full after split
        BPtreeNode_split(&p); 
        BPtree_insert(tree, p, kv);
    }
}

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

//NOTES
//whenever there is a split, a single parent node rises and merges to parent.
//The parent can never be full, its max size is degree-1.
//As soon as it becomes full, it splits and is merged to it's parent

//splits node and returns it's pointer. returned node must be merged with parent node
void BPtreeNode_split(BPtreeNode **node){
    //splits an m-node into 2 (m/2)-nodes 
    //must keep children
}

//To be used with split. Merges splited 'node' with its parent
void BPtreeNode_merge(BPtreeNode *node, BPtreeNode *p){
    /*
     * (insert all keys and children of 'node' into 'p')
     * if(mergednode->size >= max){
     *      split node
     * }
     */
}
