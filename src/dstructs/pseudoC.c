#include "bptree.h"
#include "bptree.c"

typedef struct{
    KVpair *kv;
    bool smallNode;
    bool WantImmediateLeft;     //deleted node to parent
    bool WantImmediateRight;     //deleted node to parent
    bool InvalidLeft;           //searched left to parent 
    bool InvalidRight;          //searched right to parent 
}delFlags;


delFlags trueBPT_deleteR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv){
    delFlags flags;
    memset(&flags, 0, sizeof(delFlags));

    if(node->type == NT_EXT){
        //search kv pair and remove if it exists
        BPtreeNode *deleted = BPtreeNode_delete(node, kv); 
        if(!deleted) return flags; //kv not found in node
        //link to parent 
        p->children[NextChildIDX(p,kv)] = deleted;
        node = deleted;
        if(node->nkeys < tree->degree/2){
            flags.smallNode = 1;
        }

    }else{
        //traverse next children
        BPtreeNode *next = node->children[NextChildIDX(node, kv)];
        flags = trueBPT_deleteR(tree, next, node, kv);
    }


    if(flags.smallNode){ 
        if(flags.WantImmediateLeft){
            //traverse to immediate left (iterative)
            //if no left sibling is found. return to parent with this flag
            //if we are in root node or if the immediate sibling is too small
            //  invalidate left
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

void trueBPT_delete(BPtree *tree, KVpair *kv){
}
