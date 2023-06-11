#include "pager.h"

#define PAGE_SIZE 100


//write anywhere and return place
int pageAppend(PageTable *table, BPtreeNode *node){
    //encodes node
    uint8_t *bytestream = BPtreeNode_encode(node);
    
    //allocate a page
    int page_n = pager_alloc(table);

    //write to said page
    pageWrite(table, page_n, bytestream, BPtreeNode_getSize(node));

    //return page id
    return page_n;
}

//write to specific location
void pageWrite(PageTable *table, int page_n, uint8_t *bytestream, size_t size){
    //write bytestream to the map of entry n 
    char *map = table->entries[page_n].mmAddr;
    memcpy(map, bytestream, size);
}

BPtreeNode *pageRead(PageTable *table, int page_n){
    //receives a page address, decodes page to a BPtreeNode
    uint8_t *bytestream = (uint8_t*) table->entries[page_n].mmAddr;
    BPtreeNode *node = BPtreeNode_decode(bytestream);
    //returns node
    return node;
    
    //x->children[0]->children[0] becomes:
    //pageRead(pageRead(x->chilren[0])->children[0])
    //or:
    //p  = pageRead(x->children[0])
    //c = pageRead(p->children[0])
    //
    //-- this at least makes explicit the number of page decodings/readins being
    //done
}

//called at the start to map entire file to pages
PageTable *pager_init(int fd, int len){
    //mmap file
    struct stat sb;
    if(fstat(fd,&sb) < 0){
        perror("Couldn't get file size.\n");
    }
    char *mapped_file = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    
    //create table
    PageTable *table = malloc(sizeof(PageTable));

    table->fd = fd;
    table->entries = malloc(len *sizeof(PageEntry));
    for(int i = 0; i < len; i ++){
        table->entries[i].used = true;
        table->entries[i].mmAddr = &mapped_file[PAGE_SIZE*i];
    }

    return table;
}

//if pager_alloc doesnt find an empty entry, increase it
void pager_expand(PageTable *table){
    //realloc to create new entry
    //TODO: incorrect use of realloc
    table->entries = realloc(table->entries, table->len +1); 

    //TODO: mmap new entry
    //struct stat sb;
    //if(fstat(table->fd,&sb) < 0){
    //    perror("Couldn't get file size.\n");
    //}
    //char *mapped_file = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, table->fd, 0);


    table->entries[table->len].used = true;
}

int pager_alloc(PageTable *table){
    //looks for an empty entry in table and returns it
    for(size_t i = 0; i < table->len; i++){
        if(!table->entries[i].used){
            return i;
        }
    }
    pager_expand(table);
    return table->len;
}

void pager_free(PageTable *table, int page){
    table->entries[page].used = false;
    table->entries[page].mmAddr = 0;
}
