#include "bptree.h"

//TODO: when in external memory, the conditions for spliting and merging are the size of the page
//instead of the number of keys
//TODO: make an optimization table with the number of reads and writes from nodes for each function,
//then try to reduce them.


void BPtree_print(BPtree *tree){
    if(!tree){
        printf("> Empty tree\n");
        return;
    }
    printf("\n====== TREE ======\n");
    printf("Degree: %d\n", tree->degree); 
    printf("Root:");
    BPtreeNode_print(tree->root);
}

//TODO: append master root record to the datafile
BPtree *BPtree_create(uint8_t degree){
    BPtree *rv = calloc(1, sizeof(BPtree));
    rv->degree = degree;

    //master root
    rv->root = BPtreeNode_create(0);
    rv->root->type = NT_ROOT;
    rv->root->nkeys = 0;
    rv->root->childLinks[0] = 0;       //tree root

    return rv;
}

//TODO: free becomes deletion from file
static void BPtree_free_rec(BPtreeNode *node){
    if(node == NULL) return;

    for(int i = 0; i < node->nkeys; i++){
        BPtree_free_rec(node->children[i]);
    }
    BPtreeNode_free(node);
}

//TODO: free becomes deletion from file
void BPtree_free(BPtree *ptr){
    BPtree_free_rec(ptr->root);
    free(ptr);
}

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
static bool BPtree_insertR(BPtree *tree, PageTable *t, uint64_t node_pid, uint64_t p_pid, KVpair *kv){
    //TODO: free all bptreeNodes
    //TODO: see when nodes should be written
    BPtreeNode *node = nodeRead(t, node_pid);
    BPtreeNode *p = nodeRead(t, p_pid);
    bool split = false;

    if(node->type == NT_EXT){
        //insert kv
        BPtreeNode *inserted = BPtreeNode_insert(node, kv, NULL);
        inserted->type = NT_EXT;
        //TODO: write only if no mergeSplitted occur
        int inserted_pid = nodeWrite(t, inserted);
        node = inserted;
        //link parent to new node with inserted value
        p->childLinks[NextChildIDX(p, kv)] = inserted_pid; //(***)

    }else{
        //traverse next children
        uint64_t next_pid = node->childLinks[NextChildIDX(node, kv)];
        split = BPtree_insertR(tree, t, next_pid, node_pid, kv);
    }

    if(split){
        //merge node with it`s child that splitted
        uint64_t splittedChild_pid = node->childLinks[NextChildIDX(node,kv)];
        BPtreeNode *splittedChild = nodeRead(t, splittedChild_pid);
        BPtreeNode *merged = BPtreeNode_mergeSplitted(node, splittedChild);
        int merged_pid = nodeWrite(t, merged);
        node = merged;
        //link parent to new merged node
        p->childLinks[NextChildIDX(p, kv)] = merged_pid; //(***)
    }

    //if insert or merging makes node full
    if(node->nkeys >= tree->degree){
        //split
        BPtreeNode *splitted = BPtreeNode_split(t, node);
        int splitted_pid = nodeWrite(t, splitted);
        //link parent to new splitted node
        p->childLinks[NextChildIDX(p, kv)] = splitted_pid; //(***)
        split = true;
    }else{
        split = false;
    }

    return split;
}

void BPtree_insert(BPtree *tree, KVpair *kv){
    if(!tree->root->children[0]){
        //Empty tree. Create first node and insert KV
        tree->root->children[0] = BPtreeNode_create(1);
        tree->root->children[0]->type = NT_EXT;
        BPtreeNode_appendKV(tree->root->children[0], 0,  kv);
        
        //tree->root->children[0] = BPtreeNode_insert(tree->root->children[0], kv);
        return;
    }

    BPtree_insertR(tree, tree->root->children[0], tree->root, kv);
}

//returns node pointer and the id of the key in idx
BPtreeNode *BPtree_search(BPtree *tree, KVpair *kv, int *idx){
    //traverse until reach an external node
    BPtreeNode *tmp = tree->root->children[0];
    if(!tmp) return NULL;
    while(tmp->type != NT_EXT){
        tmp = tmp->children[NextChildIDX(tmp, kv)];
    }
    int rv = BPtreeNode_search(tmp, kv);

    if(rv < 0) return NULL;

    if(idx) *idx = rv;
    return tmp;
}

bool BPtree_deleteR(BPtreeNode *node, BPtreeNode *p, KVpair *kv){
    bool emptyChild = false;
    if(node->type == NT_EXT){
        //search kv pair and remove if it exists
        BPtreeNode *deleted = BPtreeNode_delete(node, kv, NULL); 
        if(!deleted) return false; //kv not found in node
        //link to parent 
        p->children[NextChildIDX(p,kv)] = deleted;
        node = deleted;

    }else{
        //traverse next children
        BPtreeNode *next = node->children[NextChildIDX(node, kv)];
        emptyChild = BPtree_deleteR(next, node, kv);
    }

    if(emptyChild){ //shrink internal node by removing one of its kvs 
        //find the empty child
        int child_idx = NextChildIDX(node,kv);
        BPtreeNode *shrinked = BPtreeNode_shrink(node, child_idx);  
        //link to parent 
        p->children[NextChildIDX(p, kv)] = shrinked;
        node = shrinked;
    }

    //if deleting or shrinking makes node empty 
    if(node->nkeys == 0){ //split
        //free node and unlink from parent
        BPtreeNode_free(node);
        p->children[NextChildIDX(p, kv)] = NULL;
        emptyChild = true;
    }else{
        emptyChild = false;
    }

    return emptyChild;
}

void BPtree_delete(BPtree *tree, KVpair *kv){
    if(!tree) return;
    if(!tree->root) return;
    if(!tree->root->children[0]) return;

    BPtreeNode *mroot = tree->root;
    BPtree_deleteR(mroot->children[0], mroot, kv);
}

