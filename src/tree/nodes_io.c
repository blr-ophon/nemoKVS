#include "nodes.h"

/*
 * Encodes node struct to bytestream.
 */
uint8_t *BPtreeNode_encode(BPtreeNode *node){
    int size = BPtreeNode_getSize(node);
    uint8_t *bytestream = malloc(size);
    
    uint8_t *offset = bytestream;
    
    //header
    memcpy(offset, &node->type, sizeof(uint16_t));
    offset += 2;
    memcpy(offset, &node->nkeys, sizeof(uint16_t));
    offset += 2;
    memcpy(offset, &node->dataSize, sizeof(uint64_t));
    offset += 8;

    //children
    for(int i = 0; i < node->nkeys + 1; i++){
        memcpy(offset, &node->childLinks[i], sizeof(uint64_t));
        offset += 8;
    }

    //offsets
    for(int i = 0; i < node->nkeys; i++){
        memcpy(offset, &node->keyOffsets[i], sizeof(uint16_t));
        offset += 2;
    }

    //data
    if(node->nkeys){
        memcpy(offset, node->key_values, node->dataSize);
    }

    return bytestream;
}

/*
 * Decodes bytestream to a node struct.
 */
BPtreeNode *BPtreeNode_decode(uint8_t *bytestream){
    //header
    uint16_t nkeys;
    memcpy(&nkeys, &bytestream[2], 2);
    
    BPtreeNode *node = BPtreeNode_create(nkeys, 0);
    node->nkeys = nkeys;

    memcpy(&node->type, &bytestream[0], 2);
    int offset = 4;
    memcpy(&node->dataSize, &bytestream[offset], sizeof(uint64_t));
    offset += 8;

    //children
    for(int i = 0; i < node->nkeys + 1; i++){
        memcpy(&node->childLinks[i], &bytestream[offset], sizeof(uint64_t));
        offset += 8;
    }

    //offsets
    for(int i = 0; i < node->nkeys; i++){
        memcpy(&node->keyOffsets[i], &bytestream[offset], sizeof(uint16_t));
        offset += 2;
    }

    //data
    if(node->nkeys){
        node->key_values = malloc(node->dataSize);
        memcpy(node->key_values, &bytestream[offset],  node->dataSize);
    }
    
    return node;
}

/*
 * Writes node to some page and returns the page index
 */
int nodeWrite(PageTable *table, BPtreeNode *node){
    //allocate a page
    int page_n = pager_alloc(table);

    //encodes node
    uint8_t *bytestream = BPtreeNode_encode(node);

    //write to said page
    int size = BPtreeNode_getSize(node);
    uint8_t *page = table->entries[page_n];
    memcpy(page, bytestream, size);

    free(bytestream);
    return page_n;
}

/*
 * Reads page and returns a node struct
 */
BPtreeNode *nodeRead(PageTable *table, uint64_t Pidx){
    //receives a page address, decodes page to a BPtreeNode
    uint8_t *bytestream = table->entries[Pidx];
    BPtreeNode *node = BPtreeNode_decode(bytestream);
    return node;
}

/*
 * Writes a node over a specified page. 
 * Used in link updates
 */
void nodeOverwrite(PageTable *table, uint64_t Pidx, BPtreeNode *node){
    //encodes node
    uint8_t *bytestream = BPtreeNode_encode(node);

    //write to said page
    int size = BPtreeNode_getSize(node);
    uint8_t *page = table->entries[Pidx];
    memcpy(page, bytestream, size);

    free(bytestream);
}

void node_free(PageTable *t, uint64_t node_Pidx){
    pager_free(t, node_Pidx);    
}

