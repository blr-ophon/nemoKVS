#include <assert.h>
#include "pager.h"


//load page 0 from file to a pager table
PageTable *pager_init(int fd){
    //mmap file 
    struct stat sb;
    if(fstat(fd,&sb) < 0){
        perror("Couldn't get file size.\n");
    }
    if(sb.st_size % getpagesize() != 0){
        fprintf(stderr, "WARNING: File not aligned\n");
    }
    uint8_t *mapped_file = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(mapped_file == MAP_FAILED){
        perror("mmap");
        exit(1);
    }
    
    //create table
    PageTable *table = malloc(sizeof(PageTable));

    //give page 0 (table page) map to table 
    table->pageMap = &mapped_file[0];
    memcpy(&table->len, table->pageMap, 4);

    table->fd = fd;
    table->entries = malloc(table->len *sizeof(void*));

    //give other pages to entries
    for(uint32_t i = 1; i < table->len; i ++){
        table->entries[i] = &mapped_file[getpagesize()*i];
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
    uint8_t *emptyPage = calloc(1, getpagesize());
    write(table->fd, emptyPage, getpagesize());
    lseek(table->fd, 0, SEEK_SET);
    free(emptyPage);

    //mmap new page
    table->entries = realloc(table->entries, table->len * sizeof(void*));
    uint8_t *newMap = mmap(NULL, getpagesize(), PROT_READ | PROT_WRITE, MAP_SHARED, table->fd, newPg_offset);
    if(newMap == MAP_FAILED){
        perror("mmap (new)");
        exit(1);
    }
    table->entries[table->len-1] = newMap;
    
    //update page map
    table->pageMap[table->len-1 +4] = 1;
}

int pager_alloc(PageTable *table){
    //looks for an empty entry in table and returns it
    for(size_t i = 0; i < table->len; i++){
        if(!table->pageMap[i +4]){
            table->pageMap[i +4] = 1;
            return i;
        }
    }
    pager_expand(table);
    table->pageMap[table->len-1 +4] = 1;
    return table->len -1;
}

void pager_free(PageTable *table, int page_n){
    assert(page_n >= 2);
    table->pageMap[page_n +4] = 0;
    //TODO: unmap entry
}
