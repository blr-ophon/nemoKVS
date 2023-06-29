#include "nodes.h"
#include "pager.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000


/*
 * Prints a node struct
 */
void BPtreeNode_print(BPtreeNode *node){
    //--- as is
    printf("\n==== NODE ====\n");
    if(!node){
        printf("> Empty node\n");
        return;
    }
    printf("> %p\n", (void*) node);
    printf("type: ");
    switch(node->type){
        case NT_INT:
            printf("Internal\n");
            break;
        case NT_EXT:
            printf("External\n");
            break;
        case NT_ROOT:
            printf("Root\n");
            break;
        default:
            printf("????\n");
            break;
    }
    printf("No. of keys: %u\n", node->nkeys);
    printf("Size of data: %lu\n", node->dataSize);
    printf("Child links: ");
    for(uint32_t i = 0; i < node->nkeys+1; i++){
        printf("%lx | ", node->childLinks[i]);
    }
    printf("\nKey offsets: ");
    for(uint32_t i = 0; i < node->nkeys; i++){
        printf("%#x(%d) | ", node->keyOffsets[i], node->keyOffsets[i]);
    }
    printf("\nKV pairs dump: ");
    for(uint32_t i = 0; i < node->dataSize; i++){
        if(i % 16 == 0){
            printf("\n");
        }
        printf("%x ", node->key_values[i]);
        //printf("%x(%c) ", node->key_values[i], node->key_values[i]);
    }
    printf("\n\n");
}


BPtreeNode *BPtreeNode_create(uint8_t nkeys, int type){
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    if(nkeys){
        rv->childLinks = calloc(nkeys+1, sizeof(uint64_t));
        rv->keyOffsets = calloc(nkeys, sizeof(uint16_t));
        rv->keyOffsets[0] = 0;
        rv->key_values = NULL;              //key_values have variable size
    }else{  //used for master root
        rv->childLinks = calloc(1, sizeof(uint64_t));
    }
    rv->nkeys = nkeys;
    rv->type = type;
    return rv;
}


void BPtreeNode_free(BPtreeNode *node){
    free(node->childLinks);
    free(node->keyOffsets);
    free(node->key_values);
    free(node);
}

KVpair *BPtreeNode_getKV(BPtreeNode *node, int Kidx){
    if(Kidx + 1 > node->nkeys){ 
        return NULL;
    }
    assert(node->keyOffsets);
    return KVpair_decode(&node->key_values[node->keyOffsets[Kidx]]);
}

/*
 * Appends a kv to a node, ignoring comparisons and childLinks
 * Expects an empty node, created with it's maximum size already
 */
void BPtreeNode_appendKV(BPtreeNode *node, int Kidx, KVpair *kv){
    //assert(node->type == NT_EXT);
    //expects keyoffsets[idx] to be previously filled by previous node
    //- keyOffsets[0] is set to 0 in node creation
    if(Kidx >= node->nkeys){
        //TODO: set errno
        return;
    }
    uint16_t offset = node->keyOffsets[Kidx]; 
    size_t kv_size = KVpair_getSize(kv);

    node->dataSize += kv_size;
    node->key_values = realloc(node->key_values, node->dataSize);

    uint8_t *bytestream = KVpair_encode(kv);
    memcpy(&node->key_values[offset], bytestream, kv_size);

    if(Kidx == 0){
        node->keyOffsets[Kidx] = 0;
    }else{
        node->keyOffsets[Kidx] = node->keyOffsets[Kidx-1] + kv_size;
    }

    /*
    //appends offset for the next key
    if(Kidx + 1 <= node->nkeys){
        node->keyOffsets[Kidx+1] = node->keyOffsets[Kidx] + kv_size;
    }
    */

    free(bytestream);
}


/*
 * Returns size of a node in bytes
 */
int BPtreeNode_getSize(BPtreeNode *node){
    //header
    int size = 2*sizeof(uint16_t) + sizeof(size_t);
    //children
    size += (node->nkeys+1) * sizeof(uint64_t);
    //offsets
    size += (node->nkeys) * sizeof(uint16_t);
    //kv pairs
    size += node->dataSize;
    return size;
}

