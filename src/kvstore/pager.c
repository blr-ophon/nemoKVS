#include "pager.h"

#define PAGE_SIZE 100

//write to some page and return its index
int pageWrite(PageTable *table, BPtreeNode *node){
    //encodes node
    uint8_t *bytestream = BPtreeNode_encode(node);
    
    //allocate a page
    int page_n = pager_alloc(table);

    //write to said page
    int size = BPtreeNode_getSize(node);
    uint8_t *map = table->entries[page_n];
    memcpy(map, bytestream, size);

    //return page id
    return page_n;
}

BPtreeNode *pageRead(PageTable *table, int page_n){
    //receives a page address, decodes page to a BPtreeNode
    uint8_t *bytestream = table->entries[page_n];
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

//load page 0 from file to a pager table
PageTable *pager_init(int fd){
    //mmap file
    struct stat sb;
    if(fstat(fd,&sb) < 0){
        perror("Couldn't get file size.\n");
    }
    uint8_t *mapped_file = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    
    //create table
    PageTable *table = malloc(sizeof(PageTable));

    //map page 0 of file (table page)
    table->pageMap = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    memcpy(&table->len, table->pageMap, 4);

    table->fd = fd;
    table->entries = malloc(table->len *sizeof(void*));

    for(uint32_t i = 0; i < table->len; i ++){
        table->entries[i] = &mapped_file[PAGE_SIZE*i];
    }

    return table;
}

//if pager_alloc doesnt find an empty entry, increase it
void pager_expand(PageTable *table){
    //update size
    table->len ++;
    memcpy(&table->pageMap[0], &table->len, sizeof(uint32_t));

    //append an empty page to table fd
    off_t newPg_offset = lseek(table->fd, 0, SEEK_END);
    const uint8_t zeroes[PAGE_SIZE] = {0};
    write(table->fd, zeroes, sizeof(zeroes));
    lseek(table->fd, 0, SEEK_SET);

    //mmap new page
    table->entries = realloc(table->entries, table->len * sizeof(void*));
    table->entries[table->len-1] = mmap(NULL, PAGE_SIZE, PROT_READ, MAP_PRIVATE, table->fd, newPg_offset);
    
    //update page map
    table->pageMap[table->len-1 +4] = 1;
}

int pager_alloc(PageTable *table){
    //looks for an empty entry in table and returns it
    for(size_t i = 0; i < table->len; i++){
        if(!table->pageMap[i +4]){
            return i;
        }
    }
    pager_expand(table);
    return table->len -1;
}

void pager_free(PageTable *table, int page_n){
    table->pageMap[page_n +4] = 0;
    //TODO: unmap entry
    table->entries[page_n] = 0;
}
