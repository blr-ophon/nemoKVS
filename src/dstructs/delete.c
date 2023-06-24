#include "delete.h"

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


//Inserts kv at 0 position (prepend) independent of whetever are the keys
//in the node. Only use is when a borrow from left to right occurs,
//when the borrowed kv is equal to the first kv of destination node.
BPtreeNode *BPtreeNode_prepend(BPtreeNode *node, KVpair *kv){
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

//pass extreme kv of src to extreme of dst. 
//TODO: split this into borrowRight and borrowLeft
BPtreeNode *BPTNode_borrow(PageTable *t, BPtreeNode *p, int pKV_idx, bool fromRight){
    int src_pid, dst_pid, dst_idx;

    if(fromRight){
        src_pid = p->childLinks[pKV_idx+1];
        dst_pid = p->childLinks[pKV_idx];
        dst_idx = pKV_idx;
    }else{
        src_pid = p->childLinks[pKV_idx];
        dst_pid = p->childLinks[pKV_idx+1];
        dst_idx = pKV_idx+1;
    }
    BPtreeNode *src = nodeRead(t, src_pid);
    BPtreeNode *dst = nodeRead(t, dst_pid);

    //remove last (or first) kv from src 
    
    KVpair *delKV;                          //deleted key
    int delChild_pid;                       //child of the deleted key
    BPtreeNode *deleted = NULL;
    if(fromRight){  //leftmost kv of src
        delKV = BPtreeNode_getKV(src, 0);
        delChild_pid = src->childLinks[0];
        deleted = BPtreeNode_shrink(src, 0);
    }else{          //rightmost kv of src
        delKV = BPtreeNode_getKV(src, src->nkeys-1);
        delChild_pid = src->childLinks[src->nkeys];
        deleted = BPtreeNode_shrink(src, src->nkeys);
    }

    //insert the borrowed kv in dst with its children
     
    //(***) special case for trees of degree <= 4
    int keylessChild_pid = 0;
    if(dst->nkeys == 0){
        keylessChild_pid = dst->childLinks[0];
    }//(***)

    //if the key passed to dst has child, update the first key of dst node
    KVpair *insrtKV = delKV;
    if(delChild_pid){
        if(fromRight){
            BPtreeNode *tmp = nodeRead(t, delChild_pid);
            insrtKV = BPtreeNode_getKV(tmp, 0);
            BPtreeNode_free(tmp);
        }else{
            BPtreeNode *tmp = nodeRead(t, dst->childLinks[0]);
            insrtKV = BPtreeNode_getKV(tmp, 0);
            BPtreeNode_free(tmp);
        }
    }
    if(dst->type == NT_INT) KVpair_removeVal(insrtKV);
    
    //insert in dst
    BPtreeNode *inserted;
    if(fromRight){
        inserted = BPtreeNode_insert(dst, insrtKV, NULL);
    }else{
        inserted = BPtreeNode_prepend(dst, insrtKV);
    }
    //insert child at the beginning of dst children
    if(fromRight){
        inserted->childLinks[inserted->nkeys] = delChild_pid;
    }else{
        inserted->childLinks[0] = delChild_pid;
    }
    //(***) special case for trees of degree <= 4
    if(keylessChild_pid){
        inserted->childLinks[!fromRight] = keylessChild_pid;
    }//(***)

    pager_free(t, src_pid);
    pager_free(t, dst_pid);
    int inserted_pid = nodeWrite(t, inserted);
    int deleted_pid = nodeWrite(t, deleted);

    //Update the value of the parent node through which the key is passed
    
    //get first leftmost kv to the right of p
    KVpair *pKV;
    if(fromRight){ 
        pKV = BPtree_getLeftmostKV(t, deleted_pid);
    }else{
        pKV = BPtree_getLeftmostKV(t, inserted_pid);
    }
    KVpair_removeVal(pKV);

    BPtreeNode *updated_p = BPTNode_swapKey(p, pKV_idx, pKV);

    //(***) special case for trees of degree <= 4
    if(inserted->nkeys == 1 && fromRight){
        //the key to the left of the updated one must also be updated
        //because the borrowed key became the first (only for degree <= 4)
        KVpair *pKV_2 = BPtreeNode_getKV(inserted, 0);
        updated_p = BPTNode_swapKey(updated_p, pKV_idx-1, pKV_2);
    }//(***)

    //link updated parent to shrinked and inserted node
    int srcSide = fromRight? 1 : -1;
    updated_p->childLinks[dst_idx] = inserted_pid;
    updated_p->childLinks[dst_idx + srcSide] = deleted_pid;

    KVpair_free(pKV);
    KVpair_free(delKV);
    if(insrtKV != delKV) KVpair_free(insrtKV);
    return updated_p;
}

//(C)***Free 2 nodes. Write one merged node. Return shrinked parent in-mem 
//Merges the inferior and superior child of a kv in a node. Reverse of split
//kv_idx is the kv index in parent where whose 2 children are trying to merge

BPtreeNode *BPTNode_merge(PageTable *t, BPtreeNode *node, int kv_idx){
    int inferior_pid = node->childLinks[kv_idx];
    int superior_pid = node->childLinks[kv_idx+1];
    BPtreeNode *inferior = nodeRead(t, inferior_pid);
    BPtreeNode *superior = nodeRead(t, superior_pid);
    bool internal = inferior->type == NT_INT;

    //create node from node1, first kv of parent and node2  
    BPtreeNode *merged = BPtreeNode_create(inferior->nkeys + internal + superior->nkeys, inferior->type);
    int merged_idx = 0; //used to insert kvs and children in 'merged' node
    //inferior
    for(int i = 0; i < inferior->nkeys; i++, merged_idx++){
        KVpair *tmpKV = BPtreeNode_getKV(inferior, i);
        BPtreeNode_appendKV(merged, merged_idx, tmpKV); 
        KVpair_free(tmpKV);
        merged->childLinks[merged_idx] = inferior->childLinks[i];
    }

    //parent
    //append parent kv to merged node if nodes are internal
    if(internal){
        //TODO: delete appended kv from p   (what??)
        KVpair *pKV = BPtree_getLeftmostKV(t, superior_pid);
        BPtreeNode_appendKV(merged, merged_idx, pKV); 
        merged->childLinks[merged_idx] = inferior->childLinks[inferior->nkeys];
        merged_idx++;
        KVpair_free(pKV);
    }

    //superior
    for(int i = 0; i < superior->nkeys; i++, merged_idx++){
        KVpair *tmpKV = BPtreeNode_getKV(superior, i);
        BPtreeNode_appendKV(merged, merged_idx, tmpKV); 
        KVpair_free(tmpKV);
        merged->childLinks[merged_idx] = superior->childLinks[i];
    }
    merged->childLinks[merged_idx] = superior->childLinks[superior->nkeys];

    //Free and write
    pager_free(t, inferior_pid);
    pager_free(t, superior_pid);
    int merged_pid = nodeWrite(t, merged);

    //parent loses one child and is shrinked
    BPtreeNode *shrinked = BPtreeNode_shrink(node, kv_idx+1);
    shrinked->childLinks[kv_idx] = merged_pid;    //TODO: probably wrong and with special case in 0
                                                
    BPtreeNode_free(inferior);
    BPtreeNode_free(superior);
    BPtreeNode_free(node);
    return shrinked;
    //NOTE: caller must link grandparent to shrinked
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

    BPtreeNode *newNode = BPtreeNode_create(node->nkeys, node->type);
    int i = 0;
    for(; i < node->nkeys; i++){
        if(i == kv_idx){
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


//***TODO: smallNode becomes pointer to the merged node
typedef struct{
    bool smallNode;         //returned when a node is too small and must borrow or merge
    KVpair *leftmostKV;     //returned when a kv pair must be updated in one or more parents
}ret_flags;

ret_flags shinBPT_deleteR(BPtree *tree, PageTable *t, uint64_t node_pid, uint64_t p_pid, KVpair *kv){
    BPtreeNode *node = nodeRead(t, node_pid);
    BPtreeNode *p = nodeRead(t, p_pid);
    int ptoc_idx = NextChildIDX(node,kv);

    ret_flags rv;
    rv.smallNode = 0;
    rv.leftmostKV = NULL;

    if(node->type == NT_EXT){
        /*
         * Delete single KV from node
         */
        //search kv pair and remove if it exists
        int ptonode_idx = NextChildIDX(p,kv);
        int deletedKV_idx;
        BPtreeNode *deleted = BPtreeNode_delete(node, kv, &deletedKV_idx); 

        if(!deleted){ //kv not found in node
            BPtreeNode_free(node);
            BPtreeNode_free(p);
            return rv; //all false
        }

        if(deletedKV_idx == 0) rv.leftmostKV = BPtreeNode_getKV(deleted, 0); 

        node = deleted;

        pager_free(t, node_pid);
        node_pid = nodeWrite(t, node);
        //link to parent 
        //p->childLinks[ptonode_idx] = deleted;
        linkUpdate(t, p_pid, ptonode_idx, node_pid);

        if(node->nkeys < tree->degree/2 - 1){
            rv.smallNode = true;
            BPtreeNode_free(node);
            BPtreeNode_free(p);
            return rv;
        }
    }else{
        /*
         * Traverse to next node
         */
        int nodetonext_idx = NextChildIDX(node, kv);
        int next_pid = node->childLinks[nodetonext_idx];
        rv = shinBPT_deleteR(tree, t, next_pid, node_pid, kv);
        if(rv.leftmostKV){
            //update node and p
            BPtreeNode_free(node);
            BPtreeNode_free(p);
            node = nodeRead(t, node_pid);
            p = nodeRead(t, p_pid);
            /*
             * swap keys to new leftmost
             */
            if(nodetonext_idx != 0){
                BPtreeNode *swapped = BPTNode_swapKey(node, nodetonext_idx-1, rv.leftmostKV);
                node = swapped;

                pager_free(t, node_pid);
                node_pid = nodeWrite(t, node);

                linkUpdate(t, p_pid, NextChildIDX(p,kv), node_pid);

                KVpair_free(rv.leftmostKV);
                rv.leftmostKV = NULL;           
            }//else: the node to be updated is higher on the tree. Let it return again
        }
    }


    if(rv.smallNode){
        //update node and p
        BPtreeNode_free(node);
        BPtreeNode_free(p);
        node = nodeRead(t, node_pid);
        p = nodeRead(t, p_pid);
        /*
         * Merge or split
         */
        bool operationDone = false;
        //int ptoc_idx = NextChildIDX(node,kv);

        //as parent. try giving from the left sibling to the node
        if(ptoc_idx != 0){  //left sibling does not exist / node is child 0 of p
            BPtreeNode *tmp = nodeRead(t, node->childLinks[ptoc_idx-1]);
            if(tmp->nkeys -1 >= tree->degree/2 -1){

                BPtreeNode *updated = BPTNode_borrow(t, node, ptoc_idx-1, 0);
                node = updated;

                pager_free(t, node_pid);
                int node_pid = nodeWrite(t, node);
                linkUpdate(t, p_pid, NextChildIDX(p, kv), node_pid);

                operationDone = true;

                if(ptoc_idx == 1){  //borrow occurs in leftmost KV of node
                    //1 because for borrow left in kv 0, child 1 asks for a kv in child 0
                    rv.leftmostKV = BPtree_getLeftmostKV(t, node_pid);
                }
            }
            BPtreeNode_free(tmp);
        }


        //if left sibling too smal or inexistent, try the right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            BPtreeNode *tmp = nodeRead(t, node->childLinks[ptoc_idx+1]);
            if(tmp->nkeys -1 >= tree->degree/2 -1){

                BPtreeNode *updated = BPTNode_borrow(t, node, ptoc_idx, 1);

                node = updated;

                pager_free(t, node_pid);
                int node_pid = nodeWrite(t, node);
                linkUpdate(t, p_pid, NextChildIDX(p, kv), node_pid);

                operationDone = true;

                if(ptoc_idx == 0){  //borrow occurs in leftmost KV of node 
                    //0 because for borrow right in kv 0, child 0 asks for a kv in child 1
                    //rv.leftmostKV = BPtreeNode_getKV(updated, 0);
                    rv.leftmostKV = BPtree_getLeftmostKV(t, node_pid);
                }
            }
            BPtreeNode_free(tmp);
        }

        //if right sibling too small or inexistent, merge with left sibling
        if(ptoc_idx != 0 && !operationDone){
            //TODO merge frees 2 nodes returns in-mem merged node
            //BPtreeNode *merged = BPTNode_merge(node->children[ptoc_idx-1], 
            //        node->children[ptoc_idx], node, ptoc_idx -1);
            BPtreeNode *merged = BPTNode_merge(t, node, ptoc_idx-1);
            node = merged;

            pager_free(t, node_pid);
            int node_pid = nodeWrite(t, node);
            linkUpdate(t, p_pid, NextChildIDX(p, kv), node_pid);

            operationDone = true;
        }

        //if left sibling inexistent, merge with right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            //TODO merge frees 2 nodes returns in-mem merged node
            BPtreeNode *merged = BPTNode_merge(t, node, ptoc_idx);
            node = merged;

            pager_free(t, node_pid);
            int node_pid = nodeWrite(t, node);
            linkUpdate(t, p_pid, NextChildIDX(p, kv), node_pid);

            operationDone = true;
        }

        //if root of the tree is empty, change root
        if(!operationDone){
            //link grandparent to grandchild skipping the empty root
            //TODO: overwrite root page, linking to different root
            BPtreeNode *root = nodeRead(t, node_pid);
            int newRoot = root->childLinks[0];
            BPtreeNode_free(root);
            linkUpdate(t, p_pid, 0, newRoot);
            rv.smallNode = false;

            BPtreeNode_free(node);
            BPtreeNode_free(p);
            pager_free(t, node_pid);
            return rv;
        }

        //if parent becomes too small after merge, smallnode = true
        rv.smallNode = isSmallNode(node, tree->degree/2 -1);
        //TODO: else write merged node
    }

    BPtreeNode_free(node);
    BPtreeNode_free(p);
    return rv;
}

void BPT_delete(PageTable *t, BPtree* tree, KVpair *kv){

    BPtreeNode *Mroot = nodeRead(t, tree->Mroot_id);
    int root_pid = Mroot->childLinks[0];

    shinBPT_deleteR(tree, t, root_pid, tree->Mroot_id, kv);
    BPtreeNode_free(Mroot);
}


