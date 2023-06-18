#include "delete.h"

//TODO test what happens when using degree 4 and internal node becomes empty

//(C)**TODO: Change completely...
//used to update the parent node when a borrow occurs
KVpair *BPtree_getLeftmostKV(PageTable *t, int subTreeRoot_id){
    //traverse until reach an external node
    BPtreeNode *tmp = nodeRead(t, subTreeRoot_id);
    while(tmp->type != NT_EXT){
        tmp = nodeRead(t, tmp->childLinks[0]);
    }

    //find of the key in the node
    KVpair *rv = BPtreeNode_getKV(tmp, 0);
    return rv;
}


//***unchanged (full in-mem). Send to nodes.c
//Inserts kv at 0 position (prepend) independent of whetever are the keys
//in the node. Only use is when a borrow from left to right occurs,
//when the borrowed kv is equal to the first kv of destination node.
BPtreeNode *BPtreeNode_prepend(BPtreeNode *node, KVpair *kv){
    KVpair **KVs = malloc(node->nkeys * sizeof(void*));
    for(int i = 0; i < node->nkeys; i++){
        KVs[i] = BPtreeNode_getKV(node, i);
    }

    //append first key
    BPtreeNode *prepended = BPtreeNode_create(node->nkeys+1);
    prepended->type = node->type;
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

//***free 3 nodes -> write 3 nodes
//pass extreme kv of src to extreme of dst. 
//TODO: split this into borrowRight and borrowLeft
BPtreeNode *BPTNode_borrow(BPtreeNode *dst, BPtreeNode *src, int dst_idx, bool fromRight, BPtreeNode *p){
    //remove last (or first) kv from src 
    
    KVpair *delKV;                          //deleted key
    BPtreeNode *delChild;                   //child of the deleted key
    BPtreeNode *deleted = NULL;
    if(fromRight){  //leftmost kv of src
        delKV = BPtreeNode_getKV(src, 0);
        delChild = src->children[0];
        deleted = BPtreeNode_shrink(src, 0);
    }else{          //rightmost kv of src
        delKV = BPtreeNode_getKV(src, src->nkeys-1);
        delChild = src->children[src->nkeys];
        deleted = BPtreeNode_shrink(src, src->nkeys);
    }
    //deleted = BPtreeNode_delete(src, delKV, NULL);

    //insert the borrowed kv in dst with its children
     
    //(***) special case for trees of degree <= 4
    BPtreeNode *keylessChild = NULL;
    if(dst->nkeys == 0){
        keylessChild = dst->children[0];
    }//(***)

    //if the key passed to dst has child, update the first key of dst node
    KVpair *insrtKV = delKV;
    if(delChild){
        if(fromRight){
            insrtKV = BPtreeNode_getKV(delChild, 0);
        }else{
            insrtKV = BPtreeNode_getKV(dst->children[0], 0);
        }
    }
    if(dst->type == NT_INT) KVpair_removeVal(insrtKV);
    
    //TODO: this shouldnt use insert, for the same reasons merge stopped using it
    //- if borrow is from left to right, and the inserted value is the same as the 
    //  first key of the inserted node, it will be inserted after it, messing with
    //  child links.
    BPtreeNode *inserted;
    if(fromRight){
        inserted = BPtreeNode_insert(dst, insrtKV, NULL);
    }else{
        inserted = BPtreeNode_prepend(dst, insrtKV);
    }
    //insert child at the beginning of dst children
    if(fromRight){
        inserted->children[inserted->nkeys] = delChild;
    }else{
        inserted->children[0] = delChild;
    }
    //(***) special case for trees of degree <= 4
    if(keylessChild){
        inserted->children[!fromRight] = keylessChild;
    }//(***)


    //Update the value of the parent node through which the key is passed
    
    //get first leftmost kv to the right of p
    KVpair *pKV;
    if(fromRight){ 
        pKV = BPtree_getLeftmostKV(deleted);
    }else{
        pKV = BPtree_getLeftmostKV(inserted);
    }
    KVpair_removeVal(pKV);

    //index of the kv in p through which the borrow happens
    int kv_idx = fromRight? dst_idx :  dst_idx-1;                   
    
    KVpair_removeVal(pKV);
    BPtreeNode *updated_p = BPTNode_swapKey(p, kv_idx, pKV);

    //(***) special case for trees of degree <= 4
    if(inserted->nkeys == 1 && fromRight){
        //the key to the left of the updated one must also be updated
        //because the borrowed key became the first (only for degree <= 4)
        KVpair *pKV_2 = BPtreeNode_getKV(inserted, 0);
        updated_p = BPTNode_swapKey(updated_p, kv_idx-1, pKV_2);
    }//(***)

    //link updated parent to shrinked and inserted node
    int srcSide = fromRight? 1 : -1;
    updated_p->children[dst_idx] = inserted;
    updated_p->children[dst_idx + srcSide] = deleted;

    KVpair_free(pKV);
    KVpair_free(delKV);
    if(insrtKV != delKV) KVpair_free(insrtKV);
    return updated_p;
}

//***Free 2 nodes. Return one in-mem node
//Merges the inferior and superior child of a kv in a node. Reverse of split
//kv_idx is the kv index in parent where whose 2 children are trying to merge
BPtreeNode *BPTNode_merge(BPtreeNode *inferior, BPtreeNode *superior, BPtreeNode *node, int kv_idx){
    bool internal = inferior->type == NT_INT;

    //create node from node1, first kv of parent and node2  
    BPtreeNode *merged = BPtreeNode_create(inferior->nkeys + internal + superior->nkeys);
    merged->type = inferior->type;
    int merged_idx = 0; //used to insert kvs and children in 'merged' node
    //inferior
    for(int i = 0; i < inferior->nkeys; i++, merged_idx++){
        KVpair *tmpKV = BPtreeNode_getKV(inferior, i);
        BPtreeNode_appendKV(merged, merged_idx, tmpKV); 
        KVpair_free(tmpKV);
        merged->children[merged_idx] = inferior->children[i];
    }

    //parent
    //append parent kv to merged node if nodes are internal
    if(internal){
        //TODO: delete appended kv from p   (what??)
        KVpair *pKV = BPtree_getLeftmostKV(superior);
        BPtreeNode_appendKV(merged, merged_idx, pKV); 
        merged->children[merged_idx] = inferior->children[inferior->nkeys];
        merged_idx++;
        KVpair_free(pKV);
    }

    //superior
    for(int i = 0; i < superior->nkeys; i++, merged_idx++){
        KVpair *tmpKV = BPtreeNode_getKV(superior, i);
        BPtreeNode_appendKV(merged, merged_idx, tmpKV); 
        KVpair_free(tmpKV);
        merged->children[merged_idx] = superior->children[i];
    }
    merged->children[merged_idx] = superior->children[superior->nkeys];

    //parent loses one child and is shrinked
    BPtreeNode *shrinked = BPtreeNode_shrink(node, kv_idx+1);
    shrinked->children[kv_idx] = merged;    //TODO: probably wrong and with special case in 0
                                                
    BPtreeNode_free(inferior);
    BPtreeNode_free(superior);
    BPtreeNode_free(node);
    return shrinked;
    //NOTE: callee must link grandparent to shrinked
}

static bool isSmallNode(BPtreeNode *node, int minvalue){
    //TODO: use this to check page size instead of number of keys
    return node->nkeys < minvalue;
}


//***must be persistent: 1 free -> 1 (over)write
BPtreeNode *BPTNode_swapKey(BPtreeNode *node, int kv_idx, KVpair *newKV){
    KVpair **KVs = malloc(node->nkeys * sizeof(void*));  
    for(int i = 0; i < node->nkeys; i++){
        KVs[i] = BPtreeNode_getKV(node, i);
    }
    KVpair_removeVal(newKV);

    BPtreeNode *newNode = BPtreeNode_create(node->nkeys);
    int i = 0;
    for(; i < node->nkeys; i++){
        if(i == kv_idx){
            BPtreeNode_appendKV(newNode, i, newKV);
        }else{
            BPtreeNode_appendKV(newNode, i, KVs[i]);
        }
        newNode->children[i] = node->children[i];
    }
    newNode->children[i] = node->children[i];

    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);
    BPtreeNode_free(node);
    return newNode;
}


//***TODO: smallNode becomes pointer to the merged node
typedef struct{
    bool smallNode;         //returned when a node is too small and must borrow or merge
    KVpair *leftmostKV;     //returned when a kv pair must be updated in one or more parents
}ret_flags;

ret_flags shinBPT_deleteR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv){
    ret_flags rv;
    rv.smallNode = 0;
    rv.leftmostKV = NULL;

    //TODO: pageread node and p

    bool tryWrite = false;  //becomes true when delete, borrow or merge happens

    if(node->type == NT_EXT){
        //search kv pair and remove if it exists
        int ptonode_idx = NextChildIDX(p,kv);
        int deletedKV_idx;
        BPtreeNode *deleted = BPtreeNode_delete(node, kv, &deletedKV_idx); 
        if(!deleted){ //kv not found in node
            return rv; //all false
        }
        if(deletedKV_idx == 0) rv.leftmostKV = BPtreeNode_getKV(deleted, 0); 

        //link to parent 
        p->children[ptonode_idx] = deleted;
        node = deleted;
        if(node->nkeys < tree->degree/2 - 1){
            rv.smallNode = true;
            return rv;
        }
        //TODO: else: SIMPLE DELETION (free 'node' and write 'deleted')
    }else{
        int nodetonext_idx = NextChildIDX(node, kv);
        BPtreeNode *next = node->children[nodetonext_idx];
        rv = shinBPT_deleteR(tree, next, node, kv);
        if(rv.leftmostKV){
            if(nodetonext_idx != 0){
                BPtreeNode *swapped = BPTNode_swapKey(node, nodetonext_idx-1, rv.leftmostKV);
                p->children[NextChildIDX(p,kv)] = swapped;
                node = swapped;
                KVpair_free(rv.leftmostKV);
                rv.leftmostKV = NULL;           
            }//else: the node to be updated is higher on the tree
        }
    }


    if(rv.smallNode){
        
        bool operationDone = false;
        int ptoc_idx = NextChildIDX(node,kv);

        //as parent. try giving from the left sibling to the node
        if(ptoc_idx != 0){  //left sibling does not exist / node is child 0 of p
            //if(!isSmallNode(node->children[ptoc_idx-1], tree->degree/2 -1)){
            if(node->children[ptoc_idx-1]->nkeys -1 >= tree->degree/2 -1){
                //TODO read 2 children (node already read). Borrow auto frees and writes the 3 nodes
                BPtreeNode *updated = BPTNode_borrow(node->children[ptoc_idx], 
                        node->children[ptoc_idx-1], ptoc_idx, 0, node);
                
                if(ptoc_idx == 1){  //borrow occurs in leftmost KV of node
                    //1 because for borrow left in kv 0, child 1 asks for a kv in child 0
                    rv.leftmostKV = BPtree_getLeftmostKV(updated);
                }

                p->children[NextChildIDX(p,kv)] = updated;
                operationDone = true;
                node = updated;
            }
        }


        //if left sibling too smal or inexistent, try the right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            if(node->children[ptoc_idx+1]->nkeys -1 >= tree->degree/2 -1){
                //TODO read 2 children (node already read). Borrow auto frees and writes the 3 nodes
                BPtreeNode *updated = BPTNode_borrow(node->children[ptoc_idx], 
                        node->children[ptoc_idx+1], ptoc_idx, 1, node);
                p->children[NextChildIDX(p,kv)] = updated;
                operationDone = true;
                node = updated;

                if(ptoc_idx == 0){  //borrow occurs in leftmost KV of node 
                    //0 because for borrow right in kv 0, child 0 asks for a kv in child 1
                    //rv.leftmostKV = BPtreeNode_getKV(updated, 0);
                    rv.leftmostKV = BPtree_getLeftmostKV(updated);
                }
            }
        }

        //if right sibling too small or inexistent, merge with left sibling
        if(ptoc_idx != 0 && !operationDone){
            //TODO merge frees 2 nodes returns in-mem merged node
            BPtreeNode *merged = BPTNode_merge(node->children[ptoc_idx-1], 
                    node->children[ptoc_idx], node, ptoc_idx -1);
            p->children[NextChildIDX(p,kv)] = merged;
            operationDone = true;
            node = merged;
        }

        //if left sibling inexistent, merge with right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            //TODO merge frees 2 nodes returns in-mem merged node
            BPtreeNode *merged = BPTNode_merge(node->children[ptoc_idx], 
                    node->children[ptoc_idx+1], node, ptoc_idx);
            p->children[NextChildIDX(p,kv)] = merged;
            operationDone = true;
            node = merged;
        }

        //if root of the tree is empty, change root
        if(!operationDone){
            //link grandparent to grandchild skipping the empty root
            //TODO: overwrite root page, linking to different root
            p->children[0] = node->children[0];
            BPtreeNode_free(node);
            rv.smallNode = false;
            return rv;
        }

        //if parent becomes too small after merge, smallnode = true
        rv.smallNode = isSmallNode(node, tree->degree/2 -1);
        //TODO: else write merged node
    }

    return rv;
}
void BPT_delete(BPtree *tree, KVpair *kv){
    shinBPT_deleteR(tree, tree->root->children[0], tree->root, kv);
}


