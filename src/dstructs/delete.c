#include "delete.h"


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

//TODO: split this into borrowRight and borrowLeft
/*
 * Pass a key from one child of p to its sibling. p_Kidx is the index of the key where the
 * borrow occurs.
 */
BPtreeNode *BPTNode_borrow(PageTable *t, BPtreeNode *p, int p_Kidx, bool fromRight){
    int src_Pidx, dst_Pidx, dst_Cidx;

    if(fromRight){
        src_Pidx = p->childLinks[p_Kidx+1];
        dst_Pidx = p->childLinks[p_Kidx];
        dst_Cidx = p_Kidx;
    }else{
        src_Pidx = p->childLinks[p_Kidx];
        dst_Pidx = p->childLinks[p_Kidx+1];
        dst_Cidx = p_Kidx+1;
    }
    BPtreeNode *src = nodeRead(t, src_Pidx);
    BPtreeNode *dst = nodeRead(t, dst_Pidx);

    //remove last (or first) kv from src 
    
    KVpair *delKV;                          //deleted key
    int delChild_Pidx;                       //child of the deleted key
    BPtreeNode *deleted = NULL;
    if(fromRight){  //leftmost kv of src
        delKV = BPtreeNode_getKV(src, 0);
        delChild_Pidx = src->childLinks[0];
        deleted = BPtreeNode_shrink(src, 0);
    }else{          //rightmost kv of src
        delKV = BPtreeNode_getKV(src, src->nkeys-1);
        delChild_Pidx = src->childLinks[src->nkeys];
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
    if(delChild_Pidx){
        if(fromRight){
            BPtreeNode *tmp = nodeRead(t, delChild_Pidx);
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
        inserted->childLinks[inserted->nkeys] = delChild_Pidx;
    }else{
        inserted->childLinks[0] = delChild_Pidx;
    }
    //(***) special case for trees of degree <= 4
    if(keylessChild_pid){
        inserted->childLinks[!fromRight] = keylessChild_pid;
    }//(***)

    pager_free(t, src_Pidx);
    pager_free(t, dst_Pidx);
    int inserted_Pidx = nodeWrite(t, inserted);
    int deleted_Pidx = nodeWrite(t, deleted);

    //Update the value of the parent node through which the key is passed
    
    //get first leftmost kv to the right of p
    KVpair *pKV;
    if(fromRight){ 
        pKV = BPtree_getLeftmostKV(t, deleted_Pidx);
    }else{
        pKV = BPtree_getLeftmostKV(t, inserted_Pidx);
    }
    KVpair_removeVal(pKV);

    BPtreeNode *updated_p = BPTNode_swapKey(p, p_Kidx, pKV);

    //(***) special case for trees of degree <= 4
    if(inserted->nkeys == 1 && fromRight){
        //the key to the left of the updated one must also be updated
        //because the borrowed key became the first (only for degree <= 4)
        KVpair *pKV_2 = BPtreeNode_getKV(inserted, 0);
        updated_p = BPTNode_swapKey(updated_p, p_Kidx-1, pKV_2);
    }//(***)

    //link updated parent to shrinked and inserted node
    int srcSide = fromRight? 1 : -1;
    updated_p->childLinks[dst_Cidx] = inserted_Pidx;
    updated_p->childLinks[dst_Cidx + srcSide] = deleted_Pidx;

    KVpair_free(pKV);
    KVpair_free(delKV);
    if(insrtKV != delKV) KVpair_free(insrtKV);
    return updated_p;
}

/*
 * Merges the inferior and superior child of a kv in a node. Reverse of split
 * merge_Kidx is the kv index in parent where whose 2 children are trying to merge
 * Frees 2 nodes. Writes merged node. Return shrinked parent as struct.
 */

BPtreeNode *BPTNode_merge(PageTable *t, BPtreeNode *node, int merge_Kidx){
    int inferior_Pidx = node->childLinks[merge_Kidx];
    int superior_Pidx = node->childLinks[merge_Kidx+1];
    BPtreeNode *inferior = nodeRead(t, inferior_Pidx);
    BPtreeNode *superior = nodeRead(t, superior_Pidx);
    bool internal = inferior->type == NT_INT;

    //create node from node1, first kv of parent and node2  
    BPtreeNode *merged = BPtreeNode_create(inferior->nkeys + internal + superior->nkeys, inferior->type);
    int merged_it = 0; //iterator to insert kvs and children in 'merged' node
    //inferior
    for(int i = 0; i < inferior->nkeys; i++, merged_it++){
        KVpair *tmpKV = BPtreeNode_getKV(inferior, i);
        BPtreeNode_appendKV(merged, merged_it, tmpKV); 
        KVpair_free(tmpKV);
        merged->childLinks[merged_it] = inferior->childLinks[i];
    }

    //parent
    //append parent kv to merged node if nodes are internal
    if(internal){
        //TODO: delete appended kv from p   (what??)
        KVpair *pKV = BPtree_getLeftmostKV(t, superior_Pidx);
        BPtreeNode_appendKV(merged, merged_it, pKV); 
        merged->childLinks[merged_it] = inferior->childLinks[inferior->nkeys];
        merged_it++;
        KVpair_free(pKV);
    }

    //superior
    for(int i = 0; i < superior->nkeys; i++, merged_it++){
        KVpair *tmpKV = BPtreeNode_getKV(superior, i);
        BPtreeNode_appendKV(merged, merged_it, tmpKV); 
        KVpair_free(tmpKV);
        merged->childLinks[merged_it] = superior->childLinks[i];
    }
    merged->childLinks[merged_it] = superior->childLinks[superior->nkeys];

    //Free and write
    pager_free(t, inferior_Pidx);
    pager_free(t, superior_Pidx);
    int merged_Pidx = nodeWrite(t, merged);

    //parent loses one child and is shrinked
    BPtreeNode *shrinked = BPtreeNode_shrink(node, merge_Kidx+1);
    shrinked->childLinks[merge_Kidx] = merged_Pidx;    //TODO: probably wrong and with special case in 0
                                                
    BPtreeNode_free(inferior);
    BPtreeNode_free(superior);
    //BPtreeNode_free(node);
    return shrinked;
    //NOTE: caller must link grandparent to shrinked
}

static bool isSmallNode(BPtreeNode *node, int minvalue){
    //TODO: use this to check page size instead of number of keys
    return node->nkeys < minvalue;
}


//***TODO: smallNode becomes pointer to the merged node
typedef struct{
    bool smallNode;         //returned when a node is too small and must borrow or merge
    KVpair *leftmostKV;     //returned when a kv pair must be updated in one or more parents
}ret_flags;

ret_flags BPT_deleteR(BPtree *tree, PageTable *t, uint64_t node_Pidx, uint64_t p_Pidx, KVpair *kv){
    BPtreeNode *node = nodeRead(t, node_Pidx);
    BPtreeNode *p = nodeRead(t, p_Pidx);
    int NDtoNext_Cidx = NextChildIDX(node,kv);
    //TODO: unupdated NDtoNext could be causing problems

    ret_flags rv;
    rv.smallNode = 0;
    rv.leftmostKV = NULL;

    if(node->type == NT_EXT){
        /*
         * Delete single KV from node
         */
        //search kv pair and remove if it exists
        int PtoND_Cidx = NextChildIDX(p,kv);
        int deletedKV_Kidx;
        BPtreeNode *deleted = BPtreeNode_delete(node, kv, &deletedKV_Kidx); 

        if(!deleted){ //kv not found in node
            BPtreeNode_free(node);
            BPtreeNode_free(p);
            return rv; //all false
        }

        if(deletedKV_Kidx == 0) rv.leftmostKV = BPtreeNode_getKV(deleted, 0); 

        node = deleted;

        pager_free(t, node_Pidx);
        node_Pidx = nodeWrite(t, node);
        //link to parent 
        linkUpdate(t, p_Pidx, PtoND_Cidx, node_Pidx);

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
        //NDtoNext_Cidx = NextChildIDX(node,kv);
        int next_Pidx = node->childLinks[NDtoNext_Cidx];
        rv = BPT_deleteR(tree, t, next_Pidx, node_Pidx, kv);
        if(rv.leftmostKV){
            //update node and p
            BPtreeNode_free(node);
            BPtreeNode_free(p);
            node = nodeRead(t, node_Pidx);
            p = nodeRead(t, p_Pidx);
            /*
             * swap keys to new leftmost
             */
            if(NDtoNext_Cidx != 0){
                BPtreeNode *swapped = BPTNode_swapKey(node, NDtoNext_Cidx-1, rv.leftmostKV);
                node = swapped;

                pager_free(t, node_Pidx);
                node_Pidx = nodeWrite(t, node);

                linkUpdate(t, p_Pidx, NextChildIDX(p,kv), node_Pidx);

                KVpair_free(rv.leftmostKV);
                rv.leftmostKV = NULL;           
            }//else: the node to be updated is higher on the tree. Let it return again
        }
    }


    if(rv.smallNode){
        //update node and p
        BPtreeNode_free(node);
        BPtreeNode_free(p);
        node = nodeRead(t, node_Pidx);
        p = nodeRead(t, p_Pidx);
        /*
         * Merge or split
         */
        bool operationDone = false;

        //as parent. try giving from the left sibling to the node
        if(NDtoNext_Cidx != 0){  //left sibling does not exist / node is child 0 of p
            BPtreeNode *tmp = nodeRead(t, node->childLinks[NDtoNext_Cidx-1]);
            if(tmp->nkeys -1 >= tree->degree/2 -1){

                BPtreeNode *updated = BPTNode_borrow(t, node, NDtoNext_Cidx-1, 0);
                node = updated;

                pager_free(t, node_Pidx);
                int node_Pidx = nodeWrite(t, node);
                linkUpdate(t, p_Pidx, NextChildIDX(p, kv), node_Pidx);

                operationDone = true;

                if(NDtoNext_Cidx == 1){  //borrow occurs in leftmost KV of node
                    //1 because for borrow left in kv 0, child 1 asks for a kv in child 0
                    rv.leftmostKV = BPtree_getLeftmostKV(t, node_Pidx);
                }
            }
            BPtreeNode_free(tmp);
        }


        //if left sibling too smal or inexistent, try the right sibling
        if(NDtoNext_Cidx != node->nkeys && !operationDone){
            BPtreeNode *tmp = nodeRead(t, node->childLinks[NDtoNext_Cidx+1]);
            if(tmp->nkeys -1 >= tree->degree/2 -1){

                BPtreeNode *updated = BPTNode_borrow(t, node, NDtoNext_Cidx, 1);

                node = updated;

                pager_free(t, node_Pidx);
                int node_Pidx = nodeWrite(t, node);
                linkUpdate(t, p_Pidx, NextChildIDX(p, kv), node_Pidx);

                operationDone = true;

                if(NDtoNext_Cidx == 0){  //borrow occurs in leftmost KV of node 
                    //0 because for borrow right in kv 0, child 0 asks for a kv in child 1
                    //rv.leftmostKV = BPtreeNode_getKV(updated, 0);
                    rv.leftmostKV = BPtree_getLeftmostKV(t, node_Pidx);
                }
            }
            BPtreeNode_free(tmp);
        }

        //if right sibling too small or inexistent, merge with left sibling
        if(NDtoNext_Cidx != 0 && !operationDone){
            //TODO merge frees 2 nodes returns in-mem merged node
            //BPtreeNode *merged = BPTNode_merge(node->children[ptoc_idx-1], 
            //        node->children[ptoc_idx], node, ptoc_idx -1);
            BPtreeNode *merged = BPTNode_merge(t, node, NDtoNext_Cidx-1);
            node = merged;

            pager_free(t, node_Pidx);
            int node_Pidx = nodeWrite(t, node);
            linkUpdate(t, p_Pidx, NextChildIDX(p, kv), node_Pidx);

            operationDone = true;
        }

        //if left sibling inexistent, merge with right sibling
        if(NDtoNext_Cidx != node->nkeys && !operationDone){
            //TODO merge frees 2 nodes returns in-mem merged node
            BPtreeNode *merged = BPTNode_merge(t, node, NDtoNext_Cidx);
            node = merged;

            pager_free(t, node_Pidx);
            int node_Pidx = nodeWrite(t, node);
            linkUpdate(t, p_Pidx, NextChildIDX(p, kv), node_Pidx);

            operationDone = true;
        }

        //if root of the tree is empty, change root
        if(!operationDone){
            //link grandparent to grandchild skipping the empty root
            //TODO: overwrite root page, linking to different root
            BPtreeNode *root = nodeRead(t, node_Pidx);
            int newRoot = root->childLinks[0];
            BPtreeNode_free(root);
            linkUpdate(t, p_Pidx, 0, newRoot);
            rv.smallNode = false;

            BPtreeNode_free(node);
            BPtreeNode_free(p);
            pager_free(t, node_Pidx);
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
    int root_Pidx = Mroot->childLinks[0];

    BPT_deleteR(tree, t, root_Pidx, tree->Mroot_id, kv);
    BPtreeNode_free(Mroot);
}


