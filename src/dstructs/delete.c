#include "bptree.h"
#include "bptree.c"

typedef struct{
    BPtreeNode *delNode;        //Node where deletion occurs
    BPtreeNode *p;              //his parent
    bool smallNode;
    bool WantImmediateLeft;     //deleted node to parent
    bool WantImmediateRight;     //deleted node to parent
    bool InvalidLeft;           //searched left to parent 
    bool InvalidRight;          //searched right to parent 
}delFlags;

BPtreeNode *BPtree_getRightmost(BPtreeNode *root);

//ptoc_flags are flags passed down the node to the deleted
delFlags trueBPT_deleteR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv, delFlags *ptoc_flags){
    delFlags flags = *ptoc_flags;
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
            if(imleft->nkeys < tree->degree/2){
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

void trueBPT_delete(BPtree *tree, KVpair *kv){
}

//DO NOT PASS MASTER ROOT TO THIS
BPtreeNode *BPtree_getRightmost(BPtreeNode *root){
    BPtreeNode *tmp = root;
    while(root->type != NT_EXT){
        tmp = tmp->children[tmp->nkeys]; 
    }

    return tmp;
}
