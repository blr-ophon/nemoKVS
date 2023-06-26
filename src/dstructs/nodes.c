#include "nodes.h"
#include "pager.h"

#define BTREE_PAGE_SIZE     4096
#define BTREE_MAX_KEY_SIZE  1000
#define BTREE_MAX_VAL_SIZE  3000


/*
 * Prints a node struct
 */
void BPtreeNode_print(BPtreeNode *node){
    //--- as is
    printf("\n==== NODE ====\n");
    if(!node){
        printf("> Empty node\n");
        return;
    }
    printf("> %p\n", (void*) node);
    printf("type: ");
    switch(node->type){
        case NT_INT:
            printf("Internal\n");
            break;
        case NT_EXT:
            printf("External\n");
            break;
        case NT_ROOT:
            printf("Root\n");
            break;
        default:
            printf("????\n");
            break;
    }
    printf("No. of keys: %u\n", node->nkeys);
    printf("Size of data: %lu\n", node->dataSize);
    printf("Child links: ");
    for(uint32_t i = 0; i < node->nkeys+1; i++){
        printf("%lx | ", node->childLinks[i]);
    }
    printf("\nKey offsets: ");
    for(uint32_t i = 0; i < node->nkeys; i++){
        printf("%#x(%d) | ", node->keyOffsets[i], node->keyOffsets[i]);
    }
    printf("\nKV pairs dump: ");
    for(uint32_t i = 0; i < node->dataSize; i++){
        if(i % 16 == 0){
            printf("\n");
        }
        printf("%x ", node->key_values[i]);
        //printf("%x(%c) ", node->key_values[i], node->key_values[i]);
    }
    printf("\n\n");
}


BPtreeNode *BPtreeNode_create(uint8_t nkeys, int type){
    BPtreeNode *rv = (BPtreeNode*) calloc(1, sizeof(BPtreeNode));
    if(nkeys){
        rv->childLinks = calloc(nkeys+1, sizeof(uint64_t));
        rv->keyOffsets = calloc(nkeys, sizeof(uint16_t));
        rv->keyOffsets[0] = 0;
        rv->key_values = NULL;              //key_values have variable size
    }else{  //used for master root
        rv->childLinks = calloc(1, sizeof(uint64_t));
    }
    rv->nkeys = nkeys;
    rv->type = type;
    return rv;
}


void BPtreeNode_free(BPtreeNode *node){
    free(node->childLinks);
    free(node->keyOffsets);
    free(node->key_values);
    free(node);
}


/*
 * Returns index of the specified KV pair in the node
 */
int BPtreeNode_search(BPtreeNode* node, KVpair *kv){
    for(int i = 0; i < node->nkeys; i++){
        KVpair *crntKV = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, crntKV) == 0){
            return i;
        }
        KVpair_free(crntKV);
    }

    //kv not found in node
    return -1;
}

/*
 * Insert a KV pair in the node based on KVpair_compare
 * Returns node and the position of the key in Kidx
 */
BPtreeNode *BPtreeNode_insert(BPtreeNode *node, KVpair *kv, int *ret_Kidx){
    if(!node){
        node = BPtreeNode_create(0, node->type);
    }

    //create a copy of the node with inserted value
    BPtreeNode *newNode = BPtreeNode_create(node->nkeys + 1, node->type);

    if(node->nkeys == 0){
        //This causes problems with borrow in trees of degree 4.
        //The single children can go either to the left or right of
        //the inserted node. The borrow function handles this.
        BPtreeNode_appendKV(newNode, 0, kv);
        BPtreeNode_free(node);
        return newNode;
    }

    //read all kvs and save the position of kv
    int newKVpos = 0;   //is kv is not bigger than any kv of node, it is before all, in 0
    KVpair **nodeKVs = calloc(node->nkeys, sizeof(KVpair*));
    for(int i = 0; i < node->nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, nodeKVs[i]) >= 0){
            newKVpos = i+1;
        }
    }
    if(ret_Kidx) *ret_Kidx = newKVpos;

    newNode->childLinks[0] = node->childLinks[0];
    int Kidx_it = 0; //increased every time a kv pair from node is appended
    for(int i = 0; i < newNode->nkeys; i++){
        if(i == newKVpos){ //append kv
            BPtreeNode_appendKV(newNode, i, kv);
            if(i == 0){ //only case where the empty node comes before the inserted kv
                newNode->childLinks[i] = 0;
            }else{
                newNode->childLinks[i+1] = 0;
            }
            continue;
        }
        //append old kvs
        BPtreeNode_appendKV(newNode, i, nodeKVs[Kidx_it]);
        newNode->childLinks[i+1] = node->childLinks[Kidx_it+1];
        Kidx_it++;
    }

    for(int i = 0; i < node->nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);

    BPtreeNode_free(node);
    return newNode;
}

/*
 *  Delete kv from an internal node based on the position of a null child
 *  provided in del_Cidx. Used when a node's child becomes empty.
 *  Frees old node and returns shrinked node.
 */
BPtreeNode *BPtreeNode_shrink(BPtreeNode *node, int del_Cidx){
    //indexes of node and child marked for deletion
    int del_Kidx;
    if(del_Cidx == 0){
        del_Kidx = 0;
    }else{
        del_Kidx = del_Cidx -1;
    }

    int nkeys = node->nkeys;
    BPtreeNode *newNode = BPtreeNode_create(nkeys -1, node->type);

    KVpair **nodeKVs = calloc(nkeys, sizeof(KVpair*));
    for(int i = 0; i < nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
    }

    int i = 0;          //index for traversing the old node
    int nn_i = 0;       //index for insertion in new node
    for(; i < node->nkeys; i++ ){
        //append or skip kv
        if(i != del_Kidx){
            BPtreeNode_appendKV(newNode, nn_i, nodeKVs[i]);
            nn_i++;
        }
    }

    i = 0; nn_i = 0;
    for(; i < node->nkeys+1; i++ ){
        //append or skip child 
        if(i != del_Cidx){
            newNode->childLinks[nn_i] = node->childLinks[i];
            nn_i++;
        }
    }

    for(int i = 0; i < nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }
    free(nodeKVs);
    BPtreeNode_free(node);
    return newNode;
}

/*
 * Deletes a KV of an external node ignoring children. Use shrink for internal.
 * Frees node and return new node with deleted kv. Kidx is filled with the index
 * of the deleted kv
 */
BPtreeNode *BPtreeNode_delete(BPtreeNode *node, KVpair *kv, int *ret_Kidx){
    if(!node) return NULL;
    if(node->nkeys == 0) return NULL;

    //create a smaller copy of the node 
    int nkeys = node->nkeys;
    BPtreeNode *newNode = BPtreeNode_create(nkeys -1, node->type);

    //Find index of the kv to be deleted
    int del_Kidx = -1;   
    KVpair **nodeKVs = calloc(nkeys, sizeof(KVpair*));
    for(int i = 0; i < nkeys; i++){
        nodeKVs[i] = BPtreeNode_getKV(node, i);
        if(KVpair_compare(kv, nodeKVs[i]) == 0){
            del_Kidx = i;
        }
    }
    if(ret_Kidx) *ret_Kidx = del_Kidx;

    if(del_Kidx != -1){ 
        //append all old kvs to new node except the deleted one
        int Kidx_it = 0;    //iterator
        int i = 0;
        if(del_Kidx == 0) i = 1;

        for(; i < node->nkeys; i++){
            if(i == del_Kidx){
                continue;
            }
            BPtreeNode_appendKV(newNode, Kidx_it, nodeKVs[i]);
            Kidx_it++;
        }
        BPtreeNode_free(node);
    }else{
        //no kv found
        BPtreeNode_free(newNode);
        newNode = NULL;
    }

    for(int i = 0; i < nkeys; i++){
        KVpair_free(nodeKVs[i]);
    }

    free(nodeKVs);
    return newNode;
}


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
    BPtreeNode_free(node);
    BPtreeNode_free(splitted);
    return merged;
}


KVpair *BPtreeNode_getKV(BPtreeNode *node, int Kidx){
    if(Kidx + 1 > node->nkeys){ 
        return NULL;
    }
    assert(node->keyOffsets);
    return KVpair_decode(&node->key_values[node->keyOffsets[Kidx]]);
}

/*
 * Appends a kv to a node, ignoring comparisons and childLinks
 */
void BPtreeNode_appendKV(BPtreeNode *node, int Kidx, KVpair *kv){
    //assert(node->type == NT_EXT);
    //expects keyoffsets[idx] to be previously filled by previous node
    //- keyOffsets[0] is set to 0 in node creation
    if(Kidx >= node->nkeys){
        //TODO: set errno
        return;
    }
    uint16_t offset = node->keyOffsets[Kidx]; 
    size_t kv_size = KVpair_getSize(kv);

    node->dataSize += kv_size;
    node->key_values = realloc(node->key_values, node->dataSize);

    uint8_t *bytestream = KVpair_encode(kv);
    memcpy(&node->key_values[offset], bytestream, kv_size);

    //appends offset for the next key
    if(Kidx + 1 <= node->nkeys){
        node->keyOffsets[Kidx+1] = node->keyOffsets[Kidx] + kv_size;
    }

    free(bytestream);
}

/*
 * Returns size of a node in bytes
 */
int BPtreeNode_getSize(BPtreeNode *node){
    //header
    int size = 2*sizeof(uint16_t) + sizeof(size_t);
    //children
    size += (node->nkeys+1) * sizeof(uint64_t);
    //offsets
    size += (node->nkeys) * sizeof(uint16_t);
    //kv pairs
    size += node->dataSize;
    return size;
}

/*
 * Encodes node struct to bytestream.
 */
uint8_t *BPtreeNode_encode(BPtreeNode *node){
    int size = BPtreeNode_getSize(node);
    uint8_t *bytestream = malloc(size);
    
    uint8_t *offset = bytestream;
    
    //header
    memcpy(offset, &node->type, sizeof(uint16_t));
    offset += 2;
    memcpy(offset, &node->nkeys, sizeof(uint16_t));
    offset += 2;
    memcpy(offset, &node->dataSize, sizeof(uint64_t));
    offset += 8;

    //children
    for(int i = 0; i < node->nkeys + 1; i++){
        memcpy(offset, &node->childLinks[i], sizeof(uint64_t));
        offset += 8;
    }

    //offsets
    for(int i = 0; i < node->nkeys; i++){
        memcpy(offset, &node->keyOffsets[i], sizeof(uint16_t));
        offset += 2;
    }

    //data
    if(node->nkeys){
        memcpy(offset, node->key_values, node->dataSize);
    }

    return bytestream;
}

/*
 * Decodes bytestream to a node struct.
 */
BPtreeNode *BPtreeNode_decode(uint8_t *bytestream){
    //header
    uint16_t nkeys;
    memcpy(&nkeys, &bytestream[2], 2);
    
    BPtreeNode *node = BPtreeNode_create(nkeys, 0);
    node->nkeys = nkeys;

    memcpy(&node->type, &bytestream[0], 2);
    int offset = 4;
    memcpy(&node->dataSize, &bytestream[offset], sizeof(uint64_t));
    offset += 8;

    //children
    for(int i = 0; i < node->nkeys + 1; i++){
        memcpy(&node->childLinks[i], &bytestream[offset], sizeof(uint64_t));
        offset += 8;
    }

    //offsets
    for(int i = 0; i < node->nkeys; i++){
        memcpy(&node->keyOffsets[i], &bytestream[offset], sizeof(uint16_t));
        offset += 2;
    }

    //data
    if(node->nkeys){
        node->key_values = malloc(node->dataSize);
        memcpy(node->key_values, &bytestream[offset],  node->dataSize);
    }
    
    return node;
}

/*
 * Writes node to some page and returns the page index
 */
int nodeWrite(PageTable *table, BPtreeNode *node){
    //allocate a page
    int page_n = pager_alloc(table);

    //encodes node
    uint8_t *bytestream = BPtreeNode_encode(node);

    //write to said page
    int size = BPtreeNode_getSize(node);
    uint8_t *page = table->entries[page_n];
    memcpy(page, bytestream, size);

    free(bytestream);
    return page_n;
}

/*
 * Reads page and returns a node struct
 */
BPtreeNode *nodeRead(PageTable *table, uint64_t Pidx){
    //receives a page address, decodes page to a BPtreeNode
    uint8_t *bytestream = table->entries[Pidx];
    BPtreeNode *node = BPtreeNode_decode(bytestream);
    return node;
}

/*
 * Writes a node over a specified page. 
 * Used in link updates
 */
void nodeOverwrite(PageTable *table, uint64_t Pidx, BPtreeNode *node){
    //encodes node
    uint8_t *bytestream = BPtreeNode_encode(node);

    //write to said page
    int size = BPtreeNode_getSize(node);
    uint8_t *page = table->entries[Pidx];
    memcpy(page, bytestream, size);

    free(bytestream);
}

void node_free(PageTable *t, uint64_t node_Pidx){
    pager_free(t, node_Pidx);    
}

/*
 * updates childLink[child_id] to newLink and overwrites node
 * node_pid is the node to be updated. Child_id is the childLink to be updated
 * and newLink is the new value of the child.
 */
void linkUpdate(PageTable *t, uint64_t Pidx, int Cidx, uint64_t newLink){
    BPtreeNode *node = nodeRead(t, Pidx);
    node->childLinks[Cidx] = newLink;
    nodeOverwrite(t, Pidx, node);
    BPtreeNode_free(node);
}
