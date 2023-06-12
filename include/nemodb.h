#ifndef NEMODB_h
#define NEMODB_h

#include "common.h"
#include "bptree.h"
#include "dbtests.h"
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

typedef struct{
    //pthread_rwlock_t rwlock;
    FILE* writer;
    FILE* reader;
    int id;
    int offset;
}Datafile;

typedef struct database{
    char *name;
    char *path;

    Datafile datafile;
    Datafile indexfile;
}Database;


void DB_create(char *name, int node_len);
Database *DB_load(char *dbname);

void DB_free(Database *db);
void DB_destroy(Database *db);

int DB_Insert(Database *db, char *key, uint8_t *data, size_t size);
KVpair *DB_Read(Database *db, char *key);
void DB_delete(char *key);

#endif
