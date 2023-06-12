#include "nodes.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000

//TODO: external nodes dont need children links. Wasted space

static void assert_page(BPtreeNode *node){
    //assert node fits in a page
}

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
        printf("%lx | ", (uint64_t) node->children[i]);
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


//TODO; return int64 page address
//create a node with size n
BPtreeNode *BPtreeNode_create(uint8_t nkeys){
    //--- as is
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    if(nkeys){
        rv->children = calloc(nkeys+1, sizeof(void*));
        rv->keyOffsets = calloc(nkeys, sizeof(uint16_t));
        rv->keyOffsets[0] = 0;
        rv->key_values = NULL;              //key_values have variable size
    }else{  //used for master root
        rv->children = calloc(1, sizeof(void*));
    }
    rv->nkeys = nkeys;
    return rv;
}

void BPtreeNode_free(BPtreeNode *node){
    free(node->children);
    free(node->keyOffsets);
    free(node->key_values);
    free(node);
}



int BPtreeNode_search(BPtreeNode* node, KVpair *kv){
    //-- PAGE READ
    for(int i = 0; i < node->nkeys; i++){
        KVpair *crntKV = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, crntKV) == 0){
            return i;
        }
        KVpair_free(crntKV);
    }

    //kv not found in node
    return -1;
}

//TODO; return int64 page address
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *idx){
    //-- PAGE READ
    if(!node){
        node = BPtreeNode_create(0);
    }

    //create a copy of the node with inserted value
    BPtreeNode *newNode = BPtreeNode_create(node->nkeys + 1);
    newNode->type = node->type;

    if(node->nkeys == 0){
        //This causes problems with borrow in trees of degree 4.
        //The single children can go either to the left or right of
        //the inserted node. The borrow function handles this.
        BPtreeNode_appendKV(newNode, 0, kv);
        BPtreeNode_free(node);
        return newNode;
    }

    //read all kvs and save the position of kv
    int newKVpos = 0;   //is kv is not bigger than any kv of node, it is before all, in 0
    KVpair **nodeKVs = calloc(node->nkeys, sizeof(KVpair*));
    for(int i = 0; i < node->nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, nodeKVs[i]) >= 0){
            newKVpos = i+1;
        }
    }
    if(idx) *idx = newKVpos;

    newNode->children[0] = node->children[0];
    int kv_idx = 0; //increased every time a kv pair from node is appended
    for(int i = 0; i < newNode->nkeys; i++){
        if(i == newKVpos){ //append kv
            BPtreeNode_appendKV(newNode, i, kv);
            if(i == 0){ //only case where the empty node comes before the inserted kv
                newNode->children[i] = NULL;
            }else{
                newNode->children[i+1] = NULL;
            }
            continue;
        }
        //append old kvs
        BPtreeNode_appendKV(newNode, i, nodeKVs[kv_idx]);
        newNode->children[i+1] = node->children[kv_idx+1];
        kv_idx++;
    }

    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);

    BPtreeNode_free(node);
    //-- PAGE WRITE 
    return newNode;
}

//TODO; return int64 page address
//Delete kv from an internal node based on the position of a null child
//provided in del_child_idx
BPtreeNode *BPtreeNode_shrink(BPtreeNode *node, int del_child_idx){
    //---- PAGE READ
    /*
     * I almost went insane for errors caused by this simple thing. I
     * know it's ugly with all these loops, but it works... for now
     */

    //indexes of node and child marked for deletion
    int del_kv_idx;
    if(del_child_idx == 0){
        del_kv_idx = 0;
    }else{
        del_kv_idx = del_child_idx -1;
    }

    int nkeys = node->nkeys;
    BPtreeNode *newNode = BPtreeNode_create(nkeys -1);
    newNode->type = node->type;

    KVpair **nodeKVs = calloc(nkeys, sizeof(KVpair*));
    for(int i = 0; i < nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
    }

    int i = 0;          //index for traversing the old node
    int nn_i = 0;       //index for insertion in new node
    for(; i < node->nkeys; i++ ){
        //append or skip kv
        if(i != del_kv_idx){
            BPtreeNode_appendKV(newNode, nn_i, nodeKVs[i]);
            nn_i++;
        }
    }

    i = 0; nn_i = 0;
    for(; i < node->nkeys+1; i++ ){
        //append or skip child 
        if(i != del_child_idx){
            newNode->children[nn_i] = node->children[i];
            nn_i++;
        }
    }

    for(int i = 0; i < nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);
    return newNode;
    //---- PAGE WRITE 
}

//TODO; return int64 page address
//Delete kv of a node. Only works for external nodes. 
BPtreeNode *BPtreeNode_delete(BPtreeNode *node, KVpair *kv, int *idx){
    //---- PAGE READ
    if(!node) return NULL;
    if(node->nkeys == 0) return NULL;

    //create a smaller copy of the node 
    int nkeys = node->nkeys;
    BPtreeNode *newNode = BPtreeNode_create(nkeys -1);
    newNode->type = node->type;

    int delKVpos = -1;   
    KVpair **nodeKVs = calloc(nkeys, sizeof(KVpair*));
    for(int i = 0; i < nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, nodeKVs[i]) == 0){
            delKVpos = i;
        }
    }
    if(idx) *idx = delKVpos;

    if(delKVpos != -1){ 
        //append all old kvs to new node except the deleted one
        int kv_idx = 0;
        int i = 0;
        if(delKVpos == 0) i = 1;

        for(; i < node->nkeys; i++){
            if(i == delKVpos){
                continue;
            }
            BPtreeNode_appendKV(newNode, kv_idx, nodeKVs[i]);
            kv_idx++;
        }
        BPtreeNode_free(node);
    }else{
        //no kv found
        BPtreeNode_free(newNode);
        newNode = NULL;
    }

    for(int i = 0; i < nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }

    free(nodeKVs);
    return newNode;
    //---- PAGE WRITE 
}


//TODO; return int64 page address
//splits node and returns it's pointer. returned node must be merged with parent node
BPtreeNode *BPtreeNode_split(BPtreeNode *node){
    //--- PAGE READ
    int i;
    bool internal = (node->type == NT_INT);
    bool odd = (node->nkeys % 2 != 0);

    //first half
    BPtreeNode *Lnode = BPtreeNode_create(node->nkeys/2);
    Lnode->type = node->type;
    for(i = 0; i < (node->nkeys)/2; i++){
        //insert data from old to new node
        KVpair *tmpKV = BPtreeNode_getKV(node, i);
        BPtreeNode_appendKV(Lnode, i, tmpKV);
        KVpair_free(tmpKV);
        Lnode->children[i] = node->children[i];
    }
    Lnode->children[i] = node->children[i];

    //create parent node 
    BPtreeNode *p = BPtreeNode_create(1);
    p->type = NT_INT;
    KVpair *p_kv = BPtreeNode_getKV(node, (node->nkeys)/2);
    KVpair_removeVal(p_kv);
    BPtreeNode_appendKV(p, 0, p_kv);
    KVpair_free(p_kv);

    //second half
    BPtreeNode *Rnode = BPtreeNode_create(node->nkeys/2 + odd - internal);
    Rnode->type = node->type;
    i+= internal;   
    int RNode_idx = 0;
    for(; i < node->nkeys; i++){
        //insert data from old to new node
        KVpair *tmpKV = BPtreeNode_getKV(node, i);
        BPtreeNode_appendKV(Rnode, RNode_idx, tmpKV);
        KVpair_free(tmpKV);
        Rnode->children[RNode_idx] = node->children[i];
        RNode_idx++;
    }
    Rnode->children[RNode_idx] = node->children[i];

    //link parent node to 2 children
    p->children[0] = Lnode;
    p->children[1] = Rnode;
    //PAGE WRITE ALL 2 CHILDREN NODES
    //  return node with these two 
    
    //destroy node
    BPtreeNode_free(node);
    return p;
}

//TODO; return int64 page address
//To be used with split. Merges 'splited' node with it's parent
//expects splitted to be of size 1, coming from a split
//expects splitted and node to be internal nodes
BPtreeNode *BPtreeNode_merge(BPtreeNode *node, BPtreeNode *splitted){
    //TODO: read all tmpKVs to an array
    //insert splitted to node
    KVpair *splittedKV = BPtreeNode_getKV(splitted, 0);
    int idx = 0;
    BPtreeNode *merged = BPtreeNode_insert(node, splittedKV, &idx);

    merged->children[idx] = splitted->children[0];        //left child
    merged->children[idx+1] = splitted->children[1];      //right child
    
    BPtreeNode_free(splitted);
    return merged;
}

KVpair *BPtreeNode_getKV(BPtreeNode *node, int idx){
    if(idx + 1 > node->nkeys){ 
        return NULL;
    }
    assert(node->keyOffsets);
    return KVpair_decode(&node->key_values[node->keyOffsets[idx]]);
}

//Overwrites key
void BPtreeNode_appendKV(BPtreeNode *node, int idx, KVpair *kv){
    //assert(node->type == NT_EXT);
    //expects keyoffsets[idx] to be previously filled by previous node
    //- keyOffsets[0] is set to 0 in node creation
    if(idx >= node->nkeys){
        //TODO: set errno
        return;
    }
    uint16_t offset = node->keyOffsets[idx]; 
    size_t kv_size = KVpair_getSize(kv);

    node->dataSize += kv_size;
    node->key_values = realloc(node->key_values, node->dataSize);

    uint8_t *bytestream = KVpair_encode(kv);
    memcpy(&node->key_values[offset], bytestream, kv_size);

    //appends offset for the next key
    if(idx + 1 <= node->nkeys){
        node->keyOffsets[idx+1] = node->keyOffsets[idx] + kv_size;
    }

    free(bytestream);
}

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
        memcpy(offset, &node->children[i], sizeof(uint64_t));
        offset += 8;
    }

    //offsets
    for(int i = 0; i < node->nkeys; i++){
        memcpy(offset, &node->keyOffsets[i], sizeof(uint16_t));
        offset += 2;
    }

    //data
    memcpy(offset, node->key_values, node->dataSize);

    return bytestream;
}

BPtreeNode *BPtreeNode_decode(uint8_t *bytestream){
    //header
    uint16_t nkeys;
    memcpy(&nkeys, &bytestream[2], 2);
    
    BPtreeNode *node = BPtreeNode_create(nkeys);
    node->nkeys = nkeys;

    memcpy(&node->type, &bytestream[0], 2);
    int offset = 4;
    memcpy(&node->dataSize, &bytestream[offset], sizeof(uint64_t));
    offset += 8;

    //children
    for(int i = 0; i < node->nkeys + 1; i++){
        memcpy(&node->children[i], &bytestream[offset], sizeof(uint64_t));
        offset += 8;
    }

    //offsets
    for(int i = 0; i < node->nkeys + 1; i++){
        memcpy(&node->keyOffsets[i], &bytestream[offset], sizeof(uint16_t));
        offset += 2;
    }

    //data
    memcpy(node->key_values, &bytestream[offset],  node->dataSize);
    
    return node;
}
