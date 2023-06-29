#include "nodes.h"

/*
 * Returns index of the specified KV pair in the node
 */
int BPtreeNode_search(BPtreeNode* node, KVpair *kv){
    for(int i = 0; i < node->nkeys; i++){
        KVpair *crntKV = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, crntKV) == 0){
            KVpair_free(crntKV);
            return i;
        }
        KVpair_free(crntKV);
    }

    //kv not found in node
    return -1;
}

/*
 * Insert a KV pair in the node based on KVpair_compare
 * Returns node and the position of the key in Kidx
 */
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *ret_Kidx){
    if(!node){
        node = BPtreeNode_create(0, node->type);
    }

    //create a copy of the node with inserted value
    BPtreeNode *newNode = BPtreeNode_create(node->nkeys + 1, node->type);

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
    if(ret_Kidx) *ret_Kidx = newKVpos;

    newNode->childLinks[0] = node->childLinks[0];
    int Kidx_it = 0; //increased every time a kv pair from node is appended
    for(int i = 0; i < newNode->nkeys; i++){
        if(i == newKVpos){ //append kv
            BPtreeNode_appendKV(newNode, i, kv);
            if(i == 0){ //only case where the empty node comes before the inserted kv
                newNode->childLinks[i] = 0;
            }else{
                newNode->childLinks[i+1] = 0;
            }
            continue;
        }
        //append old kvs
        BPtreeNode_appendKV(newNode, i, nodeKVs[Kidx_it]);
        newNode->childLinks[i+1] = node->childLinks[Kidx_it+1];
        Kidx_it++;
    }

    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);

    BPtreeNode_free(node);
    return newNode;
}

/*
 *  Delete kv from an internal node based on the position of a null child
 *  provided in del_Cidx. Used when a node's child becomes empty.
 *  Frees old node and returns shrinked node.
 */
BPtreeNode *BPtreeNode_shrink(BPtreeNode *node, int del_Cidx){
    //indexes of node and child marked for deletion
    int del_Kidx;
    if(del_Cidx == 0){
        del_Kidx = 0;
    }else{
        del_Kidx = del_Cidx -1;
    }

    int nkeys = node->nkeys;
    BPtreeNode *newNode = BPtreeNode_create(nkeys -1, node->type);

    KVpair **nodeKVs = calloc(nkeys, sizeof(KVpair*));
    for(int i = 0; i < nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
    }

    int i = 0;          //index for traversing the old node
    int nn_i = 0;       //index for insertion in new node
    for(; i < node->nkeys; i++ ){
        //append or skip kv
        if(i != del_Kidx){
            BPtreeNode_appendKV(newNode, nn_i, nodeKVs[i]);
            nn_i++;
        }
    }

    i = 0; nn_i = 0;
    for(; i < node->nkeys+1; i++ ){
        //append or skip child 
        if(i != del_Cidx){
            newNode->childLinks[nn_i] = node->childLinks[i];
            nn_i++;
        }
    }

    for(int i = 0; i < nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);
    BPtreeNode_free(node);
    return newNode;
}

/*
 * Deletes a KV of an external node ignoring children. Use shrink for internal.
 * Frees node and return new node with deleted kv. Kidx is filled with the index
 * of the deleted kv
 */
BPtreeNode *BPtreeNode_delete(BPtreeNode *node, KVpair *kv, int *ret_Kidx){
    if(!node) return NULL;
    if(node->nkeys == 0) return NULL;

    //create a smaller copy of the node 
    int nkeys = node->nkeys;
    BPtreeNode *newNode = BPtreeNode_create(nkeys -1, node->type);

    //Find index of the kv to be deleted
    int del_Kidx = -1;   
    KVpair **nodeKVs = calloc(nkeys, sizeof(KVpair*));
    for(int i = 0; i < nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, nodeKVs[i]) == 0){
            del_Kidx = i;
        }
    }
    if(ret_Kidx) *ret_Kidx = del_Kidx;

    if(del_Kidx != -1){ 
        //append all old kvs to new node except the deleted one
        int Kidx_it = 0;    //iterator
        int i = 0;
        if(del_Kidx == 0) i = 1;

        for(; i < node->nkeys; i++){
            if(i == del_Kidx){
                continue;
            }
            BPtreeNode_appendKV(newNode, Kidx_it, nodeKVs[i]);
            Kidx_it++;
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
}

/*
 * Swaps KV in (swap_Kidx) position of node with newKV
 * Frees node and returns node with swapped keys
 */
BPtreeNode *BPTNode_swapKey(BPtreeNode *node, int swap_Kidx, KVpair *newKV){
    KVpair **KVs = malloc(node->nkeys * sizeof(void*));  
    for(int i = 0; i < node->nkeys; i++){
        KVs[i] = BPtreeNode_getKV(node, i);
    }
    KVpair_removeVal(newKV);

    BPtreeNode *newNode = BPtreeNode_create(node->nkeys, node->type);
    int i = 0;
    for(; i < node->nkeys; i++){
        if(i == swap_Kidx){
            BPtreeNode_appendKV(newNode, i, newKV);
        }else{
            BPtreeNode_appendKV(newNode, i, KVs[i]);
        }
        newNode->childLinks[i] = node->childLinks[i];
    }
    newNode->childLinks[i] = node->childLinks[i];

    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);
    BPtreeNode_free(node);
    return newNode;
}

/*
 * Inserts kv at Kidx 0 of node independent of the keys stored there. Used
 * because when the node has a kv equal to the one being inserted, it is inser-
 * ted after using normal insert function, which breaks left to right borrow.
 */
BPtreeNode *BPTNode_prepend(BPtreeNode *node, KVpair *kv){
    KVpair **KVs = malloc(node->nkeys * sizeof(void*));
    for(int i = 0; i < node->nkeys; i++){
        KVs[i] = BPtreeNode_getKV(node, i);
    }

    //append first key
    BPtreeNode *prepended = BPtreeNode_create(node->nkeys+1, node->type);
    BPtreeNode_appendKV(prepended, 0, kv);

    //append other keys
    for(int i = 1; i < prepended->nkeys; i++){
        BPtreeNode_appendKV(prepended, i, KVs[i-1]);
    }

    //append children 
    prepended->childLinks[0] = -1;
    for(int i = 1; i < prepended->nkeys +1; i++){
        prepended->childLinks[i] = node->childLinks[i-1];
    }
    
    //free memory and return
    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);
    BPtreeNode_free(node);
    return prepended;
}

