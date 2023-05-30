#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdint.h>
#include <stdio.h>
#include <pthread.h>

//Record file strucure:
//Header - key - value

struct database;
struct meta;

typedef struct{
    uint32_t checksum; 
    uint32_t timestamp; 
    uint32_t expiry; 
    uint32_t Ksize; 
    uint32_t Vsize; 
}RecHeader;

typedef struct record{
    RecHeader header;
    char *key; 
    uint8_t *value;
}Record;


Record *Record_create(char *key, uint8_t *val, size_t n);
void Record_free(Record *rec);

int Record_store(struct database *db, Record *rec, struct meta *meta);
Record *Record_load(struct database *db, uint32_t offset);

int ferror_check(FILE *f, int rv);

#endif
