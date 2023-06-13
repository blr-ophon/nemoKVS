#ifndef PAGER_H
#define PAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"

typedef struct{
    int fd;
    uint32_t len;             //len is pageMap[1-4]
    uint8_t **entries;      //maps for all pages
    uint8_t *pageMap;       //map for the page table
}PageTable;

PageTable *pager_init(int fd);
void pager_expand(PageTable *table);
int pager_alloc(PageTable *table);
void pager_free(PageTable *table, int page);

#endif
