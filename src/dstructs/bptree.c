#include "bptree.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000

static void assert_page(BPtreeNode *node){
    //assert node fits in a page
}

//| type | nkeys |  children  |   offsets  | key-values
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...

//key-value offset for item idx
int offsetPos(BPtreeNode *node, int idx){
    
}

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

KVpair *BPtreeNode_getKV(BPtreeNode *node, int idx){
    //casting byte array to KVpair
    //NOTE: If this doesnt work, use kvpair as parameter and memcpy byte array
    uint16_t offset = node->keyOffsets[idx];
    return (KVpair*) node->key_values[offset];
}


//TREE
////////////////////////////////////////////////////////////////////////////////


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

    if(node->nkeys >= tree->degree){
        //MUST KNOW ITS PARENT TO MERGE or set a flag
        //split
        split = true;
    }

    return split;
}

//returns node pointer and the id of the key in idx
BPtreeNode *BPtree_search(BPtree *btree, uint32_t key, int *idx){
    //TODO: copy from btree.c
    return NULL;
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
