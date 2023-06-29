#include "bptree.h"

static bool isSmallNode(BPtreeNode *node, int minvalue){
    //TODO: use this to check page size instead of number of keys
    return node->nkeys < minvalue;
}


//***TODO: smallNode becomes pointer to the merged node
typedef struct{
    bool smallNode;         //returned when a node is too small and must borrow or merge
    KVpair *leftmostKV;     //returned when a kv pair must be updated in one or more parents
}del_rv;

del_rv BPT_deleteR(BPtree *tree, PageTable *t, uint64_t node_Pidx, uint64_t p_Pidx, KVpair *kv){
    BPtreeNode *node = nodeRead(t, node_Pidx);
    BPtreeNode *p = nodeRead(t, p_Pidx);
    int NDtoNext_Cidx = NextChildIDX(node,kv);
    //TODO: unupdated NDtoNext could be causing problems

    del_rv rv;
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
                    KVpair_free(rv.leftmostKV);
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
                    KVpair_free(rv.leftmostKV);
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

    del_rv rv = BPT_deleteR(tree, t, root_Pidx, tree->Mroot_id, kv);
    if(rv.leftmostKV) KVpair_free(rv.leftmostKV); 

    BPtreeNode_free(Mroot);
}


