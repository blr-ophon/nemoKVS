#include "nodes.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000


static void assert_page(BPtreeNode *node){
    //assert node fits in a page
}

void BPtreeNode_print(BPtreeNode *node){
    if(!node){
        printf("> Empty node\n");
        return;
    }
    printf("\n==== NODE ====\n");
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


//create a node with size n
BPtreeNode *BPtreeNode_create(uint8_t nkeys){
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



BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *idx){
    if(!node){
        node = BPtreeNode_create(0);
    }

    //create a copy of the node with inserted value
    BPtreeNode *newNode = BPtreeNode_create(node->nkeys + 1);
    newNode->children[0] = node->children[0];
    newNode->type = node->type;

    if(node->nkeys == 0){
        //TODO: fix
        BPtreeNode_appendKV(newNode, 0, kv);
        BPtreeNode_free(node);
        return newNode;
    }

    //Iterate through all kvs of node. when one superior to kv is found, append kv
    KVpair **nodeKVs = calloc(node->nkeys, sizeof(KVpair*));
    for(int i = 0; i < node->nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
    }

    bool kvInserted = false;
    int kv_idx = 0;

    for(int i = 0; i < newNode->nkeys; i++){
        if(kvInserted){ //after kv is inserted, no comparisons are needed
            //append child and key value of nodeKVS[i]
            newNode->children[i+1] = node->children[kv_idx];
            BPtreeNode_appendKV(newNode, i, nodeKVs[kv_idx++]);
            continue;
        }

        //kv is superior to all kvs in node. All kvs from node were already appended.
        //special case because tmpKV is NULL and cant be compared
        if(kv_idx == node->nkeys){ 
            //append child and key value
            newNode->children[i+1] = NULL;
            BPtreeNode_appendKV(newNode, i, kv);
            if(idx) *idx = i;
            break;
        }

        //kv is inferior to tmpKV OR kv is the last key. Append kv
        if(KVpair_compare(kv,nodeKVs[kv_idx]) < 0){
            //append child and key value 
            if(i == 0){ //kv is inferior to all kvs in node
                newNode->children[0] = NULL;
                newNode->children[1] = node->children[0];
            }else{
                newNode->children[i+1] = NULL;
            }
            BPtreeNode_appendKV(newNode, i, kv);
            kvInserted = true;
            if(idx) *idx = i;
            continue;
        }

        //kv is superior to nodeKV[i] (but is not last the key). Append nodeKV[i]
        newNode->children[i+1] = node->children[kv_idx];
        BPtreeNode_appendKV(newNode, i, nodeKVs[kv_idx++]);
    }

    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);

    BPtreeNode_free(node);
    return newNode;
}


//splits node and returns it's pointer. returned node must be merged with parent node
BPtreeNode *BPtreeNode_split(BPtreeNode *node){
    int i;
    bool internal = (node->type == NT_INT);
    bool odd = (node->nkeys % 2 != 0);

    //first half
    //create node
    BPtreeNode *Lnode = BPtreeNode_create(node->nkeys/2);
    Lnode->type = node->type;
    for(i = 0; i < (node->nkeys)/2; i++){
        //insert data from old to new node
        KVpair *tmpKV = BPtreeNode_getKV(node, i);
        BPtreeNode_appendKV(Lnode, i, tmpKV);
        KVpair_free(tmpKV);
    }
    memcpy(Lnode->children, node->children, (node->nkeys)/2);


    //create parent node 
    BPtreeNode *p = BPtreeNode_create(1);
    p->type = NT_INT;
    KVpair *p_kv = BPtreeNode_getKV(node, (node->nkeys)/2);
    KVpair_removeVal(p_kv);
    BPtreeNode_appendKV(p, 0, p_kv);
    KVpair_free(p_kv);


    //second half
    //create node
    BPtreeNode *Rnode = BPtreeNode_create(node->nkeys/2 + odd - internal);
    Rnode->type = node->type;
    i+= internal;   
    int RNode_idx = 0;
    for(; i < node->nkeys; i++){
        //insert data from old to new node
        KVpair *tmpKV = BPtreeNode_getKV(node, i);
        BPtreeNode_appendKV(Rnode, RNode_idx++, tmpKV);
        KVpair_free(tmpKV);
    }
    int half = (node->nkeys)/2;
    memcpy(Rnode->children, &node->children[half], half + odd - internal);

    //link parent node to 2 children
    p->children[0] = Lnode;
    p->children[1] = Rnode;
    
    //destroy node
    BPtreeNode_free(node);
    return p;
}

//To be used with split. Merges 'splited' node with it's parent
//expects splitted to be of size 1, coming from a split
//expects splitted and node to be internal nodes
BPtreeNode *BPtreeNode_merge(BPtreeNode *node, BPtreeNode *splitted){
    //TODO: read all tmpKVs to an array
    //insert splitted to node
    KVpair *splittedKV = BPtreeNode_getKV(splitted, 0);
    BPtreeNode *merged = BPtreeNode_insert(node, splittedKV, NULL);

    //find which children will receive the children of splitted
    int child_idx = -1;
    int splittted_child_idx = 0;
    for(int i = 0; i < merged->nkeys; i++){
        KVpair *tmpKV = BPtreeNode_getKV(merged, i);
        //if splittedKV superior to tmpKV
        if(KVpair_compare(splittedKV, tmpKV) > 0){
            child_idx = i;
            KVpair_free(tmpKV);
            break;
        }

        //child not related to inserted value
        merged->children[i] = splitted->children[splittted_child_idx++];   

        KVpair_free(tmpKV);
    }
    KVpair_free(splittedKV);

    if(child_idx == -1){    //splittedKV superior to all kvs of node
        child_idx = merged->nkeys -1;
    }

    merged->children[child_idx] = splitted->children[0];        //left child
    merged->children[child_idx+1] = splitted->children[1];      //right child
    
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
    uint16_t offset = node->keyOffsets[idx]; 
    size_t kv_size = KVpair_getSize(kv);

    node->dataSize += kv_size;
    node->key_values = realloc(node->key_values, node->dataSize);

    uint8_t *bytestream = KVpair_encode(kv);
    //for(uint32_t i = 0; i < kv_size; i++){
    //    if(i % 16 == 0){
    //        printf("\n");
    //    }
    //    printf("%d ", bytestream[i]);
    //}
    //printf("\n\n");
    memcpy(&node->key_values[offset], bytestream, kv_size);

    //appends offset for the next key
    if(idx + 1 <= node->nkeys){
        node->keyOffsets[idx+1] = node->keyOffsets[idx] + kv_size;
    }

    free(bytestream);
}
