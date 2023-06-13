#include "bptree.h"

//TODO: when in external memory, the conditions for spliting and merging are the size of the page
//instead of the number of keys
//TODO: make an optimization table with the number of reads and writes from nodes for each function,
//then try to reduce them.


void BPtree_print(PageTable *t, BPtree *tree){
    if(!tree){
        printf("> Empty tree\n");
        return;
    }
    printf("\n====== TREE ======\n");
    printf("Degree: %d\n", tree->degree); 
    printf("Root:");
    BPtreeNode *Mroot = nodeRead(t, tree->Mroot_id);
    BPtreeNode_print(Mroot);
}

//TODO: append master root record to the datafile
BPtree *BPtree_create(PageTable *t, uint8_t degree){
    BPtree *rv = calloc(1, sizeof(BPtree));
    rv->degree = degree;

    //master root
    BPtreeNode *Mroot = BPtreeNode_create(0);
    Mroot->type = NT_ROOT;
    Mroot->nkeys = 0;
    Mroot->childLinks[0] = 0;       //tree root
    nodeOverwrite(t, 1, Mroot);
    rv->Mroot_id = 1;

    return rv;
}

//TODO: free becomes deletion from file
//static void BPtree_free_rec(BPtreeNode *node){
//    if(node == NULL) return;
//
//    for(int i = 0; i < node->nkeys; i++){
//        BPtree_free_rec(node->children[i]);
//    }
//    BPtreeNode_free(node);
//}

//TODO: free becomes deletion from file
//void BPtree_free(BPtree *ptr){
//    BPtree_free_rec(ptr->root);
//    free(ptr);
//}

//returns ID of the next child. Used to traverse the tree
int NextChildIDX(BPtreeNode *node, KVpair *kv){
    //TODO: minimize use of this function in insert and delete
    //iterate through all kvs of node until kv is inferior to one of them
    for(int i = 0; i < node->nkeys; i++){
        KVpair *crntKV = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, crntKV) < 0){
            return i;
        }
        KVpair_free(crntKV);
    }

    //kv superior to all node kvs
    return node->nkeys;
}


//static bool BPtree_insertR(BPtree *tree, BPtreeNode *node, BPtreeNode *p, KVpair *kv){
static BPtreeNode *BPtree_insertR(BPtree *tree, PageTable *t, uint64_t node_pid, uint64_t p_pid, KVpair *kv){
    //TODO: free all bptreeNodes
    //TODO: remove tree struct, splitting based on node size
    //TODO: see when nodes should be written
    //TODO: write only if no mergeSplitted occur
    //
    BPtreeNode *node = nodeRead(t, node_pid);
    BPtreeNode *p = nodeRead(t, p_pid);
    BPtreeNode *spl = NULL;

    if(node->type == NT_EXT){
        //insert kv
        BPtreeNode *inserted = BPtreeNode_insert(node, kv, NULL);
        inserted->type = NT_EXT;
        node = inserted;
    }else{
        //traverse next children
        uint64_t next_pid = node->childLinks[NextChildIDX(node, kv)];
        spl = BPtree_insertR(tree, t, next_pid, node_pid, kv);
    }

    if(spl){
        //merge node with it`s child that splitted
        BPtreeNode *merged = BPtreeNode_mergeSplitted(node, spl);
        node = merged;
    }

    //if insert or merging makes node full
    if(node->nkeys >= tree->degree){
        //split
        BPtreeNode *splitted = BPtreeNode_split(t, node);
        spl = splitted;
    }else{
        uint64_t newNode = nodeWrite(t, node);
        //update p to link to node_pid
        int child_id = NextChildIDX(p, kv);
        linkUpdate(t, p_pid, child_id, newNode);
        //free old node
        node_free(t, node_pid);
        spl = NULL;
    }

    return spl;
}

void BPtree_insert(PageTable *t, BPtree *tree, KVpair *kv){
    BPtreeNode *masterRoot = nodeRead(t, tree->Mroot_id);
    uint64_t root_id =  masterRoot->childLinks[0];

    if(!root_id){
        //if master root points to no root, create one
        BPtreeNode *root = BPtreeNode_create(1);
        root->type = NT_EXT;
        BPtreeNode_appendKV(root, 0,  kv);
        root_id = nodeWrite(t, root);
        linkUpdate(t, tree->Mroot_id, 0, root_id);
        return;
    }

    BPtree_insertR(tree, t, root_id, tree->Mroot_id, kv);
}

//returns node pointer and the id of the key in idx
BPtreeNode *BPtree_search(PageTable *t, BPtree *tree, KVpair *kv, int *idx){
    //traverse until reach an external node
    BPtreeNode *mroot = nodeRead(t, tree->Mroot_id);
    if(!mroot->childLinks[0]) return NULL;
    
    BPtreeNode *tmp = nodeRead(t, mroot->childLinks[0]);
    while(tmp->type != NT_EXT){
        int child_id = NextChildIDX(tmp, kv);
        tmp = nodeRead(t, tmp->childLinks[child_id]);
    }

    //find of the key in the node
    int rv = BPtreeNode_search(tmp, kv);
    if(rv < 0) return NULL;
    if(idx) *idx = rv;

    return tmp;
}
