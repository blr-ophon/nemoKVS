#include "nodes.h"
#include "bptree.h"

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
        inserted = BPTNode_prepend(dst, insrtKV);
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
        KVpair_free(pKV_2);
    }//(***)

    //link updated parent to shrinked and inserted node
    int srcSide = fromRight? 1 : -1;
    updated_p->childLinks[dst_Cidx] = inserted_Pidx;
    updated_p->childLinks[dst_Cidx + srcSide] = deleted_Pidx;

    BPtreeNode_free(deleted);
    BPtreeNode_free(inserted);

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

