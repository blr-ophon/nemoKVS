#include "delete.h"

//pass extreme kv of src to extreme of dst. 
BPtreeNode *shinBPT_borrow(BPtreeNode *dst, BPtreeNode *src, int dst_idx, bool fromRight, BPtreeNode *p){
    BPtreeNode *updated = NULL;
    //TODO: UPDATE THE VALUE OF THE PARENT NODE THROUGH WHICH THE KEY IS PASSED
    
    //remove last key from src 
    KVpair *delKV;
    BPtreeNode *delChild;
    BPtreeNode *deleted = NULL;
    if(fromRight){
        delKV = BPtreeNode_getKV(src, 0);
        delChild = src->children[0];
    }else{
        delKV = BPtreeNode_getKV(src, src->nkeys-1);
        delChild = src->children[src->nkeys];
        deleted = BPtreeNode_delete(src, delKV, NULL);
    }

    //insert kv at the beginning of dst kvs
    BPtreeNode *inserted = BPtreeNode_insert(dst, delKV, NULL);
    //insert child at the beginning of dst children
    if(fromRight){
        inserted->children[inserted->nkeys+1] = delChild;
    }else{
        inserted->children[0] = delChild;
    }
    
    //update parent if borrowed from left to right
    //link parent to shrinked and inserted node
    int srcSide = fromRight? 1 : -1;
    p->children[dst_idx] = inserted;
    p->children[dst_idx + srcSide] = deleted;
    return updated;
}

//create a merged node with the nodes of inferior followed by superior. Reverse of split
BPtreeNode *shinBPT_mergeINT(BPtreeNode *inferior, BPtreeNode *superior, BPtreeNode *p, int idx_from_p){
    //create node from node1, first kv of parent and node2  
    BPtreeNode *merged = BPtreeNode_create(inferior->nkeys + 1 + superior->nkeys);
    int merged_idx = 0;
    //inferior
    for(int i = 0; i < inferior->nkeys; i++, merged_idx++){
        KVpair *tmpKV = BPtreeNode_getKV(inferior, i);
        BPtreeNode_appendKV(merged, merged_idx, tmpKV); 
        KVpair_free(tmpKV);
        merged->children[merged_idx] = inferior->children[i];
    }
    //parent
    KVpair *pKV = BPtreeNode_getKV(p, idx_from_p);
    BPtreeNode_appendKV(merged, merged_idx++, pKV); 
    KVpair_free(pKV);
    merged->children[merged_idx] = p->children[idx_from_p];
    //superior
    for(int i = 0; i < superior->nkeys; i++, merged_idx++){
        KVpair *tmpKV = BPtreeNode_getKV(superior, i);
        BPtreeNode_appendKV(merged, merged_idx, tmpKV); 
        KVpair_free(tmpKV);
        merged->children[merged_idx] = superior->children[i];
    }
    merged->children[merged_idx] = superior->children[superior->nkeys];

    //parent loses one child and is shrinked
    BPtreeNode *shrinked = BPtreeNode_shrink(p, idx_from_p);
    shrinked->children[idx_from_p] = merged;    //TODO: probably wrong and with special case in 0
                                                //
    BPtreeNode_free(inferior);
    BPtreeNode_free(superior);
    BPtreeNode_free(p);
    return shrinked;
    //NOTE: callee must link grandparent to shrinked
}

static bool isSmallNode(BPtreeNode *node, int minvalue){
    //TODO: use this to check page size instead of number of keys
    return node->nkeys >= minvalue;
}


bool shinBPT_deleteR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv){
    bool smallNode = false;

    if(node->type == NT_EXT){
        //search kv pair and remove if it exists
        BPtreeNode *deleted = BPtreeNode_delete(node, kv, NULL); 
        if(!deleted) return false; //kv not found in node
        //link to parent 
        p->children[NextChildIDX(p,kv)] = deleted;
        node = deleted;
        if(node->nkeys < tree->degree/2 - 1){
            smallNode = true;
            return smallNode;
        }
    }else{
        BPtreeNode *next = node->children[NextChildIDX(node, kv)];
        smallNode = shinBPT_deleteR(tree, next, node, kv);
    }


    if(smallNode){
        bool operationDone = false;
        int ptoc_idx = NextChildIDX(node,kv);

        //as parent. try giving from the left sibling to the node
        if(ptoc_idx != 0){  //left sibling does not exist
            if(isSmallNode(node->children[ptoc_idx-1], tree->degree/2 -1)){

                BPtreeNode *updated = shinBPT_borrow(node->children[ptoc_idx], 
                        node->children[ptoc_idx-1], ptoc_idx, 0, node);
                p->children[NextChildIDX(p,kv)] = updated;
                operationDone = true;
                node = updated;
            }
        }


        //if left sibling too smal or inexistent, try the right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            if(isSmallNode(node->children[ptoc_idx+1], tree->degree/2 -1)){

                BPtreeNode *updated = shinBPT_borrow(node->children[ptoc_idx], 
                        node->children[ptoc_idx+1], ptoc_idx, 1, node);
                p->children[NextChildIDX(p,kv)] = updated;
                operationDone = true;
                node = updated;
            }
        }

        //if right sibling too small or inexistent, merge with left sibling
        if(ptoc_idx != 0 && !operationDone){
            BPtreeNode *merged = shinBPT_mergeINT(node->children[ptoc_idx-1], 
                    node->children[ptoc_idx], node, ptoc_idx);
            p->children[NextChildIDX(p,kv)] = merged;
            operationDone = true;
            node = merged;
        }

        //if left sibling inexistent, merge with right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            BPtreeNode *merged = shinBPT_mergeINT(node->children[ptoc_idx], 
                    node->children[ptoc_idx+1], node, ptoc_idx);
            p->children[NextChildIDX(p,kv)] = merged;
            operationDone = true;
            node = merged;
        }

        assert(operationDone);
        
        //if parent becomes too small after merge, smallnode = true
        smallNode = isSmallNode(node, tree->degree/2 -1);
    }

    return smallNode;
}
void BPT_delete(BPtree *tree, KVpair *kv){
    shinBPT_deleteR(tree, tree->root->children[0], tree->root, kv);
}

/*
typedef struct{
    BPtreeNode *delNode;        //Node where deletion occurs
    BPtreeNode *p;              //his parent
    bool smallNode;
    bool WantImmediateLeft;     //deleted node to parent
    bool WantImmediateRight;     //deleted node to parent
    bool InvalidLeft;           //searched left to parent 
    bool InvalidRight;          //searched right to parent 
}delFlags;

//ptoc_flags are flags passed down the node to the deleted
delFlags trueBPT_deleteR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv, delFlags *ptoc_flags){
    delFlags flags;
    memset(&flags, 0, sizeof(delFlags));
    if(ptoc_flags){
        flags = *ptoc_flags;
    }

    if(node->type == NT_EXT){
        //search kv pair and remove if it exists
        BPtreeNode *deleted = BPtreeNode_delete(node, kv); 
        if(!deleted) return flags; //kv not found in node
        //link to parent 
        p->children[NextChildIDX(p,kv)] = deleted;
        node = deleted;
        if(node->nkeys < tree->degree/2 - 1){
            flags.smallNode = 1;
        }
        flags.delNode = node;
        flags.p = p;

    }else{
        //traverse next children
        BPtreeNode *next = node->children[NextChildIDX(node, kv)];
        flags = trueBPT_deleteR(tree, next, node, kv, &flags);
    }


    if(flags.smallNode){ 
        if(flags.WantImmediateLeft){
            ////////////////////////////////////////////////////////////////////// 
            //traverse to immediate left (iterative)
            //if no left sibling is found. return to parent with this flag
            BPtreeNode *imleft = NULL;
            int child_idx = NextChildIDX(node, kv);
            if(child_idx == 0){
                return flags;
            }else{
                BPtree_getRightmost(node->children[child_idx-1]);
                
            }
            //if we are in root node or if the immediate left sibling is too small
            if(imleft->nkeys < tree->degree/2 + 1){
                //invalid left node
                flags.InvalidLeft = true;
                //traverse again to the deleted key with this flag to tell it to
                //look for a right sibling
                BPtreeNode *next = node->children[NextChildIDX(node, kv)];
                flags = trueBPT_deleteR(tree, next, node, kv, &flags);
            }else{
                //get the rightmost kv of the immediate left sibling
                KVpair *rightmostkv = BPtreeNode_getKV(imleft, imleft->nkeys-1);
                //append kv to the small node
                BPtreeNode *inserted = BPtreeNode_insert(flags.delNode, rightmostkv, NULL);
                flags.p->children[NextChildIDX(p, kv)] = inserted;
                KVpair_free(rightmostkv);
            }
            ////////////////////////////////////////////////////////////////////// 
            
            
           
            if(flags.InvalidLeft){
                flags.WantImmediateLeft = 0;
                flags.WantImmediateRight = 1;
            }else{
                //a kv is returned. Traverse to deleted node (iterative) 
                //and append it
            }

        }else if(flags.WantImmediateRight){ //TODO: later
            //traverse to immediate right
            if(flags.InvalidRight){
                //SPECIAL CASE: merge with left sibling
            }else{
                //a kv is returned. Traverse to deleted node (iterative) 
                //and append it
            }

        }else{
            //still in external node
        }
    }
    return flags;
}


//DO NOT PASS MASTER ROOT TO THIS
BPtreeNode *BPtree_getRightmost(BPtreeNode *root){
    BPtreeNode *tmp = root;
    while(root->type != NT_EXT){
        tmp = tmp->children[tmp->nkeys]; 
    }

    return tmp;
}
*/
