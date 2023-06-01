#include "nodes.h"

//create a node with size n
BPtreeNode *BPtreeNode_create(uint8_t nkeys){
    //TODO: check number of links
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    rv->children = calloc(nkeys, sizeof(void*));
    rv->keyOffsets = calloc(nkeys, sizeof(uint16_t));
    rv->key_values = NULL;              //key_values have variable size
    rv->nkeys = nkeys;
    return rv;
}

void BPtreeNode_free(BPtreeNode *node){
    free(node->children);
    free(node->keyOffsets);
    free(node->key_values);
    free(node);
}

//creates a copy of the node with inserted value
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv){
    //type set and check of split is done by BPtree_insert
    
    if(!node){
        node = BPtreeNode_create(0);
    }
    BPtreeNode *newNode = BPtreeNode_create(node->nkeys + 1);

    //append kv to key_values
    for(int i = 0; i < newNode->nkeys; i++){
        KVpair tmpKV = BPtreeNode_getKV(node, i);

        if(KVpair_getKey(tmpKV) > KVpair_getKey(kv) || i == newNode->nkeys){
            //if key being looked is larger than the one being inserted or is the last key,
            //append it here

            //append child to children array (empty)
            //append offset to keyOffsets
            //append kv pair to key_values
            i++;
            continue;
        }
        //append child to children array
        //append offset to keyOffsets
        //append kv pair to key_values

        newNode->children[i] = node->children[i];
        newNode->keyOffsets[i] = node->keyOffsets[i];
        memcpy(&newNode->key_values[i], &newNode->key_values[i], KVpair_getSize(tmpKV));

        newNode->key_values[i] = node->key_values[i];
    }
    
    //Update keyOffsets and children links
    
    node->nkeys++;
}


//splits node and returns it's pointer. returned node must be merged with parent node
//TODO: works differently for external and internal nodes
BPtreeNode *BPtreeNode_split(BPtreeNode *node){
    //TODO test for arbtrary values
    int i;
    bool internal = (node->type == NT_INT);
    bool odd = (node->nkeys % 2 != 0);

    //first half
    //create node
    BPtreeNode *Lnode = BPtreeNode_create(node->nkeys/2);
    for(i = 0; i < (node->nkeys)/2 - internal; i++){
        //insert data from old to new node
        BPtreeNode_insert(Lnode, BPtreeNode_getKV(node, i));
    }
    memcpy(Lnode->children, node->children, (node->nkeys)/2 - internal);
    memcpy(Lnode->keyOffsets, node->keyOffsets, (node->nkeys)/2 - internal);
    memcpy(Lnode->key_values, node->key_values, (node->nkeys)/2 - internal);
    Lnode->type = node->type;


    //create parent node 
    BPtreeNode *p = BPtreeNode_create(1);
    BPtreeNode_insert(p, BPtreeNode_getKV(node, (node->nkeys)/2 - !odd));
    p->type = NT_INT;


    //second half
    //create node
    BPtreeNode *Rnode = BPtreeNode_create(node->nkeys/2 + odd - internal);
    i+= internal;
    for(; i < node->nkeys; i++){
        BPtreeNode_insert(Rnode, BPtreeNode_getKV(node, i));
    }
    int half = (node->nkeys)/2;
    memcpy(Rnode->children, &node->children[half], half + odd - internal);
    memcpy(Rnode->keyOffsets, &node->keyOffsets[half], half + odd - internal);
    memcpy(Rnode->key_values, &node->key_values[half], half + odd - internal);
    Lnode->type = node->type;

    //link parent node to 2 children
    p->children[0] = Lnode;
    p->children[1] = Rnode;
    
    //destroy node
    BPtreeNode_free(node);
    return p;
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

KVpair BPtreeNode_getKV(BPtreeNode *node, int idx){
    //casting byte array to KVpair
    //NOTE: If this doesnt work, use kvpair as parameter and memcpy byte array
    uint16_t offset = node->keyOffsets[idx];
    //TODO: decode and return
    //return (KVpair*) node->key_values[offset];
}
