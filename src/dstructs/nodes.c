#include "nodes.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000



static void assert_page(BPtreeNode *node){
    //assert node fits in a page
}



//create a node with size n
BPtreeNode *BPtreeNode_create(uint8_t nkeys){
    //TODO: check number of links
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    rv->children = calloc(nkeys+1, sizeof(void*));
    rv->keyOffsets = calloc(nkeys, sizeof(uint16_t));
    rv->keyOffsets[0] = 0;
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



BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv){
    //type set and check of split is done by BPtree_insert
    
    if(!node){
        node = BPtreeNode_create(0);
    }

    //create a copy of the node with inserted value
    BPtreeNode *newNode = BPtreeNode_create(node->nkeys + 1);
    newNode->children[0] = node->children[0];


    //append kv to key_values
    for(int i = 0; i < newNode->nkeys; i++){
        KVpair *tmpKV = BPtreeNode_getKV(node, i);

        if(i == 0){  //test if tmpKV will be inserted in position 0
            if(KVpair_compare(kv, tmpKV) < 0){
                //add new children to the beginning
                newNode->children[0] = NULL;
                newNode->children[1] = node->children[0];

                //append kv pair and offset
                BPtreeNode_appendKV(newNode, i, kv);
                KVpair_free(tmpKV);
                continue;
            }
        }

        if(KVpair_compare(tmpKV, kv) > 0 || i == newNode->nkeys){
            //if key being looked is larger than the one being inserted or is the last key,
            //append it here

            //append child to children array (empty)
            newNode->children[i+1] = NULL;

            //append kv pair and offset 
            BPtreeNode_appendKV(newNode, i, kv);
            KVpair_free(tmpKV);
            continue;
        }

        //append child to children array
        newNode->children[i+1] = node->children[i];

        //append kv pair and offset 
        BPtreeNode_appendKV(newNode, i, tmpKV);
        KVpair_free(tmpKV);
    }

    //TODO: if i free the node, how do i link to its parent?
    //-the callee is responsible for linking
    BPtreeNode_free(node);
    return newNode;
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
    KVpair *p_kv = BPtreeNode_getKV(node, (node->nkeys)/2 - !odd);
    KVpair_removeVal(p_kv);
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

//To be used with split. Merges 'splited' node with it's parent
//expects splitted to be of size 1, coming from a split
//expects splitted and node to be internal nodes
//
//How it works:
//before splitting 
BPtreeNode *BPtreeNode_merge(BPtreeNode *node, BPtreeNode *splitted){
    //insert splitted to node
    KVpair *splittedKV = BPtreeNode_getKV(splitted, 0);
    BPtreeNode *merged = BPtreeNode_insert(node, splittedKV);

    //find which children will receive the children of splitted
    int child_idx = -1;
    for(int i = 0; i < merged->nkeys; i++){
        KVpair *tmpKV = BPtreeNode_getKV(splitted, i);
        if(KVpair_compare(splittedKV, tmpKV) < 0){
            child_idx = i;
            KVpair_free(tmpKV);
            break;
        }
        KVpair_free(tmpKV);
    }
    KVpair_free(splittedKV);

    if(child_idx == -1){    //splittedKV superior to all kvs of node
        child_idx = merged->nkeys -1;
    }

    merged->children[child_idx] = splitted->children[0];        //left child
    merged->children[child_idx+1] = splitted->children[1];      //right child
    
    BPtreeNode_free(node);
    BPtreeNode_free(splitted);
    return merged;
}

KVpair *BPtreeNode_getKV(BPtreeNode *node, int idx){
    return KVpair_decode(&node->key_values[node->keyOffsets[idx]]);
}

void BPtreeNode_appendKV(BPtreeNode *node, int idx, KVpair *kv){
    assert(node->type == NT_EXT);
    //expects keyoffsets[idx] to be previously filled by previous node
    //keyOffsets[0] is set to 0 in node creation
    uint16_t offset = node->keyOffsets[idx]; 
    size_t kv_size = KVpair_getSize(kv);

    uint8_t *bytestream = KVpair_encode(kv);
    memcpy(&node->key_values[offset], bytestream, kv_size);

    //appends offset for the next key
    if(idx + 1 <= node->nkeys){
        node->keyOffsets[idx+1] = node->keyOffsets[idx] + kv_size;
    }

    free(bytestream);
}
