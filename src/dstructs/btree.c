#include "btree.h"

////////////////////////////////////////////////////////////////////////////////
/// TREE
////////////////////////////////////////////////////////////////////////////////

Btree *Btree_create(uint8_t degree){
    Btree *btree = (Btree*) calloc(1, sizeof(Btree)); 
    btree->degree = degree;
    return btree;
}

//returns node pointer and the id of the key in idx
BtreeNode *Btree_search(Btree *btree, uint32_t key, int *idx){
    BtreeNode *tmp = btree->root;
    while(tmp != NULL){
        for(int i = 0; i < tmp->size; i++){
            //key found
            if(key == tmp->keys[i]){
                *idx = i;
                return tmp;
            }

            //key is bigger than every node key
            if(i == tmp->size){ 
                //go to highest child
                tmp = tmp->children[i+1];
                break;
            }

            //key is smaller than node key i
            if(key < tmp->keys[i]){ 
                //go to left child
                tmp = tmp->children[i];
                break;
            }
            //key is between nodekey i and i+1
            if(key > tmp->keys[i] && key < tmp->keys[i+1]){ 
                //go to right child
                tmp = tmp->children[i+1];
                break;
            }
        }
    }
    return tmp;
}

void Btree_insert(Btree *btree, BtreeNode *root, uint32_t key, void *val){
    BtreeNode *tmp = root;
    BtreeNode *p;   //parent node
                    
    //case 1: empty tree 
    if(!tmp){
        return;
    }

    //traverse until leaf node is found
    int i;
    while(tmp != NULL){
        p = tmp;
        for(i = 0; i < tmp->size; i++){
            //key is bigger than every node key
            if(i == tmp->size){ 
                //go to highest child
                tmp = tmp->children[i+1];
                break;
            }

            //key is smaller than node key i
            if(key < tmp->keys[i]){ 
                //go to left child
                tmp = tmp->children[i];
                break;
            }
            //key is between nodekey i and i+1
            if(key >= tmp->keys[i] && key < tmp->keys[i+1]){ 
                //go to right child
                tmp = tmp->children[i+1];
                break;
            }
        }
    }

    //case 2: leaf node with empty space
    if(p->size < btree->degree){
        BtreeNode_insert(p, key, val);
        
    }else{ 
        //case 3: leaf node full 
        //split and insert 
        //TODO: analyze what happens when parent becomes full after split
        BtreeNode_split(&p); 
        Btree_insert(btree, p, key, val);
    }
}

static void Btree_free_rec(BtreeNode *node){
    if(node == NULL) return;

    for(int i = 0; i < node->size; i++){
        Btree_free_rec(node->children[i]);
    }
    BtreeNode_free(node);
}

void Btree_free(Btree *btree){
    Btree_free_rec(btree->root);
    free(btree);
}

////////////////////////////////////////////////////////////////////////////////
/// NODE
////////////////////////////////////////////////////////////////////////////////


BtreeNode *BtreeNode_create(uint8_t n){
    BtreeNode *btnode = (BtreeNode*) calloc(1, sizeof(BtreeNode));
    btnode->children = calloc(n, sizeof(void*));
    btnode->keys = calloc(n, sizeof(uint32_t));
    return btnode;
}

void BtreeNode_insert(BtreeNode *node, uint32_t key, void *val){
    for(int i = 0; i <- node->size; i ++){

    }
}

//NOTES
//whenever there is a split, a single parent node rises and merges to parent.
//The parent can never be full, its max size is degree-1.
//As soon as it becomes full, it splits and is merged to it's parent

//splits node and returns it's pointer. returned node must be merged with parent node
void BtreeNode_split(BtreeNode **node){
    //splits an m-node into 2 (m/2)-nodes 
    //must keep children
}

//To be used with split. Merges splited 'node' with its parent
void BtreeNode_merge(BtreeNode *node, BtreeNode *p){
    /*
     * (insert all keys and children of 'node' into 'p')
     * if(mergednode->size >= max){
     *      split node
     * }
     */
}

void BtreeNode_free(BtreeNode *node){
    free(node->children);
    free(node->keys);
    //TODO free record
    free(node);
}

