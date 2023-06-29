#include "nodes.h"

/*
 * Splits a node into 2 children and one parent. The children are written on disk and the 
 * parent is returned to merge with previous parent using mergeSplitted function
 */
BPtreeNode *BPtreeNode_split(PageTable *t, BPtreeNode *node){
    int i;
    bool internal = (node->type == NT_INT);
    bool odd = (node->nkeys % 2 != 0);

    //first half
    BPtreeNode *Lnode = BPtreeNode_create(node->nkeys/2, node->type);
    for(i = 0; i < (node->nkeys)/2; i++){
        //insert data from old to new node
        KVpair *tmpKV = BPtreeNode_getKV(node, i);
        BPtreeNode_appendKV(Lnode, i, tmpKV);
        KVpair_free(tmpKV);
        Lnode->childLinks[i] = node->childLinks[i];
    }
    Lnode->childLinks[i] = node->childLinks[i];

    //create parent node 
    BPtreeNode *p = BPtreeNode_create(1, NT_INT);
    KVpair *p_kv = BPtreeNode_getKV(node, (node->nkeys)/2);
    KVpair_removeVal(p_kv);
    BPtreeNode_appendKV(p, 0, p_kv);
    KVpair_free(p_kv);

    //second half
    BPtreeNode *Rnode = BPtreeNode_create(node->nkeys/2 + odd - internal, node->type);
    i+= internal;   
    int RNode_it = 0;
    for(; i < node->nkeys; i++){
        //insert data from old to new node
        KVpair *tmpKV = BPtreeNode_getKV(node, i);
        BPtreeNode_appendKV(Rnode, RNode_it, tmpKV);
        KVpair_free(tmpKV);
        Rnode->childLinks[RNode_it] = node->childLinks[i];
        RNode_it++;
    }
    Rnode->childLinks[RNode_it] = node->childLinks[i];

    //link parent node to 2 children
    int lnode_Pidx = nodeWrite(t, Lnode);
    int rnode_Pidx = nodeWrite(t, Rnode);
    p->childLinks[0] = lnode_Pidx;
    p->childLinks[1] = rnode_Pidx;
    BPtreeNode_free(Lnode);
    BPtreeNode_free(Rnode);
    
    //destroy node
    BPtreeNode_free(node);
    return p;
}

/*
 * Merges a single kv internal node returned from BPtreeNode_split() with its parent.
 * parent to splitted idx is necessary because the internal node may have repeated keys, to which the
 * splitted may be inserted before in the sequence
 *
 * Same as insert, but inserts at an specific place. TODO: insert becomes useless for 
 * internal nodes, reduce its code.
 *
 * TODO: can substitute prepend and insert in delete.c to reduce code
 */
BPtreeNode *BPtreeNode_mergeSplitted(BPtreeNode *node, BPtreeNode *splitted, int ptospl_Cidx){
    KVpair **KVs = malloc(node->nkeys * sizeof(void*));
    for(int i = 0; i < node->nkeys; i++){
        KVs[i] = BPtreeNode_getKV(node, i);
    }
    KVpair *newKV = BPtreeNode_getKV(splitted, 0);

    BPtreeNode *merged = BPtreeNode_create(node->nkeys+1, NT_INT);

    //append keys
    int node_it = 0;       //iterator for node
    for(int i = 0; i < merged->nkeys; i++){
        if(i == ptospl_Cidx){    //In this operation, the idx from node of the splitted child
                                //is equal to the kv idx where it will be inserted
            BPtreeNode_appendKV(merged, i, newKV);
        }else{
            BPtreeNode_appendKV(merged, i, KVs[node_it++]);
        }
    }

    //append children 
    node_it = 0;
    for(int i = 0; i < merged->nkeys +1; i++){
        if(i == ptospl_Cidx){
            i++;
        }
        merged->childLinks[i] = node->childLinks[node_it++];
    }
    merged->childLinks[ptospl_Cidx] = splitted->childLinks[0];
    merged->childLinks[ptospl_Cidx+1] = splitted->childLinks[1];
    
    //free memory and return
    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);
    KVpair_free(newKV);
    BPtreeNode_free(node);
    BPtreeNode_free(splitted);
    return merged;
}

