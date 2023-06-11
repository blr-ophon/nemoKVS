#ifndef PAGER_H
#define PAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "nodes.h"

typedef struct{
    bool used;
    char *mmAddr;       //necessary when multiple mappings are present
}PageEntry;

typedef struct{
    int fd;
    size_t len;
    PageEntry *entries;
}PageTable;


int pageAppend(PageTable *table, BPtreeNode *node);
void pageWrite(PageTable *table, int page_n, uint8_t *bytestream, size_t size);
BPtreeNode *pageRead(PageTable *table, int page_n);

PageTable *pager_init(int fd, int len);
void pager_expand(PageTable *table);
int pager_alloc(PageTable *table);
void pager_free(PageTable *table, int page);


#endif
