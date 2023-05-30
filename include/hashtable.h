#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

//TODO: change filename to indexlist

//Index file strucure:
//Key length - key - metadata

struct record;

typedef struct meta{
    uint32_t Timestamp; 
    uint32_t RecordSize;  
    uint32_t RecordPos;    
    uint32_t FileID;        
}Meta;


typedef struct ht_entry{
    char *key;
    Meta val; 
}HT_entry;


typedef struct htable{
    size_t size;
    int count;
    HT_entry **entries; //array of pointers to entries
}HTable;

////////////////////////////////////////////////////////////////////////////////

//append metadata to file
void Indexfile_append(FILE *f, struct record *rec, Meta *metadata);

//load all metadata from file
HTable *Indexfile_load(FILE *f);

////////////////////////////////////////////////////////////////////////////////


//hash function

uint32_t ht_djb2(char *key, uint32_t table_size);

uint32_t ht_hash(char *key, int attempts, size_t table_size);

//search/insert/delete/print

Meta *ht_retrieveVal(HTable *table, char *key);

HT_entry *ht_search(HTable *table, char *key);

void ht_insert(HTable *table, char *key, Meta *val);

void ht_delete(HTable *table, char *key);

void ht_printTable(HTable *table);

//Create/free

HT_entry *ht_createEntry(char *key, Meta *val);

HTable *ht_createTable(size_t size);

void ht_freeEntry(HT_entry *entry);

void ht_freeTable(HTable *table);

#endif
