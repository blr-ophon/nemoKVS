#include "delete.h"

//TODO test what happens when using degree 4 and internal node becomes empty

//used to update the parent node when a borrow occurs
KVpair *BPtree_getLeftmostKV(BPtreeNode *node){
    if(!node) return NULL;
    BPtreeNode *tmp = node;
    while(tmp->type != NT_EXT){
        tmp = tmp->children[0];
    }
    return BPtreeNode_getKV(tmp, 0);
}

//pass extreme kv of src to extreme of dst. 
BPtreeNode *shinBPT_borrow(BPtreeNode *dst, BPtreeNode *src, int dst_idx, bool fromRight, BPtreeNode *p){
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
    }

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
    
    BPtreeNode *inserted = BPtreeNode_insert(dst, insrtKV, NULL);
    //insert child at the beginning of dst children
    if(fromRight){
        inserted->children[inserted->nkeys+1] = delChild;
    }else{
        inserted->children[0] = delChild;
    }
    //(***) special case for trees of degree <= 4
    if(keylessChild){
        inserted->children[!fromRight] = keylessChild;
    }


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
    //(delete old kv, insert new kv);
    KVpair_removeVal(pKV);
    BPtreeNode *deleted_p = BPtreeNode_shrink(p, kv_idx);
    BPtreeNode *updated_p = BPtreeNode_insert(deleted_p, pKV, NULL);

    //link updated parent to shrinked and inserted node
    int srcSide = fromRight? 1 : -1;
    updated_p->children[dst_idx] = inserted;
    updated_p->children[dst_idx + srcSide] = deleted;


    KVpair_free(pKV);
    KVpair_free(delKV);
    if(insrtKV != delKV) KVpair_free(insrtKV);
    return updated_p;
}

//Merges the inferior and superior child of a kv in a node. Reverse of split
//kv_idx is the kv index in parent where whose 2 children are trying to merge
BPtreeNode *shinBPT_mergeINT(BPtreeNode *inferior, BPtreeNode *superior, BPtreeNode *node, int kv_idx){
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
        //TODO: delete appended kv from p
        KVpair *pKV = BPtreeNode_getKV(node, kv_idx);
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
    BPtreeNode *shrinked = BPtreeNode_shrink(node, kv_idx);
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
        if(ptoc_idx != 0){  //left sibling does not exist / node is child 0 of p
            //if(!isSmallNode(node->children[ptoc_idx-1], tree->degree/2 -1)){
            if(node->children[ptoc_idx-1]->nkeys -1 >= tree->degree/2 -1){

                BPtreeNode *updated = shinBPT_borrow(node->children[ptoc_idx], 
                        node->children[ptoc_idx-1], ptoc_idx, 0, node);
                p->children[NextChildIDX(p,kv)] = updated;
                operationDone = true;
                node = updated;
            }
        }


        //if left sibling too smal or inexistent, try the right sibling
        if(ptoc_idx != node->nkeys && !operationDone){
            //if(!isSmallNode(node->children[ptoc_idx+1], tree->degree/2 -1)){
            if(node->children[ptoc_idx+1]->nkeys -1 >= tree->degree/2 -1){
                BPtreeNode *updated = shinBPT_borrow(node->children[ptoc_idx], 
                        node->children[ptoc_idx+1], ptoc_idx, 1, node);
                p->children[NextChildIDX(p,kv)] = updated;
                operationDone = true;
                node = updated;
            }
        }

        //if right sibling too small or inexistent, merge with left sibling
        if(ptoc_idx != 0 && !operationDone && p != tree->root){
            BPtreeNode *merged = shinBPT_mergeINT(node->children[ptoc_idx-1], 
                    node->children[ptoc_idx], node, ptoc_idx -1);
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

        //if root of the tree is empty, change root
        if(!operationDone){
            //link grandparent to grandchild skipping the empty root
            p->children[0] = node->children[0];
            BPtreeNode_free(node);
            return false;
        }

        //if parent becomes too small after merge, smallnode = true
        smallNode = isSmallNode(node, tree->degree/2 -1);
    }

    return smallNode;
}
void BPT_delete(BPtree *tree, KVpair *kv){
    shinBPT_deleteR(tree, tree->root->children[0], tree->root, kv);
}

