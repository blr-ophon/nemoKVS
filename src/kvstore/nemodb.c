#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include "nemodb.h"

#define MAX_PATHNAME 64
#define MAX_KEYNAME 64
#define DB_BASEDIR "./databases"

//API usage (cli interface):
//  -nemodb is executed and has a cli interface
//  -the interface allows normal database operations
//  -the database server is started via interface
//
//API usage (c library):
//  -User starts a connection with database through a given port
//  -the server validates the user through TLS
//  -After validation, CRUD operations are allowed
//  -server is concurrent
//
//Structures:
//  -Record and Metadata are independent of each other except when
//      metadata is generated in record insertion
//  -The database uses metadata to fill its hashtable and to append
//      new entries after a new record insertion
//  -The database uses Record in CRUD operations
//  -Both DB and Metadata have access to hashtable functions
//      
//TODO: check errno for all system calls
//TODO: DB functions error handling
//      -define errno constants
//      -all db calls must return 0
//      -one function prints the interpretation of errors


int main(void){
    BPtree *tree = BPtree_create(7);
    BPtree_print(tree);

    KVpair * kv1 = KVpair_create(1, 10, "1", "test-VAL1");
    KVpair * kv2 = KVpair_create(1, 10, "2", "test-VAL2");
    KVpair * kv3 = KVpair_create(1, 10, "3", "test-VAL3");
    KVpair * kv4 = KVpair_create(1, 10, "4", "test-VAL4");
    KVpair * kv5 = KVpair_create(1, 10, "5", "test-VAL5");
    KVpair * kv6 = KVpair_create(1, 10, "6", "test-VAL6");
    KVpair * kv7 = KVpair_create(1, 10, "7", "test-VAL7");
    KVpair * kv8 = KVpair_create(1, 10, "8", "test-VAL8");
    BPtree_insert(tree, kv5);
    BPtree_insert(tree, kv2);
    BPtree_insert(tree, kv4);
    BPtree_insert(tree, kv1);
    BPtree_insert(tree, kv6);
    BPtree_insert(tree, kv3);

    BPtreeNode_print(tree->root->children[0]);
    //DB_create("test_db");
    //Database *db = DB_load("test_db");
    //if(!db){
    //    printf("database not found");
    //    return -1;
    //}

    //uint8_t data1[7] = {1,2,3,4,5,6,7};
    //DB_Insert(db, "testkey1", data1, 7);

    //uint8_t data2[7] = {7,6,5,4,3,2,1};
    //DB_Insert(db, "testkey2", data2, 7);


    //Record *rec = DB_Read(db, "testkey1");
    //for(uint32_t i = 0; i < rec->header.Vsize; i++){
    //    printf("%d ", rec->value[i]);
    //}
    //printf("\n"); 

    //rec = DB_Read(db, "testkey2");
    //for(uint32_t i = 0; i < rec->header.Vsize; i++){
    //    printf("%d ", rec->value[i]);
    //}
    //printf("\n"); 


    //Record_free(rec);
    //DB_free(db);
    return 0;
}

//create database directory and struct
void DB_create(char *name){
    struct stat st = {0};

    //create base directory
    char *basedir = DB_BASEDIR;
    if(stat(basedir, &st) < 0){ 
        mkdir(basedir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  //or 0700
    }
    
    //create database directory (basedir + / + name)
    char dbfolder[MAX_PATHNAME] = {0};
    snprintf(dbfolder, MAX_PATHNAME, "%s/%s", basedir, name);

    if(stat(dbfolder, &st) < 0){ 
        mkdir(dbfolder, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  //or 0700
    }

    //create database file
    char dbfile[2*MAX_PATHNAME] = {0};
    char metafile[2*MAX_PATHNAME] = {0};
    snprintf(dbfile, MAX_PATHNAME, "%s/%s%s", dbfolder, name, ".dat");
    snprintf(metafile, MAX_PATHNAME, "%s/%s%s", dbfolder, name, ".kv");

    //TODO: ask if user wishes to overwrite if it's the same name
    FILE *dbf = fopen(dbfile, "w");
    FILE *metaf = fopen(metafile, "w");
    fclose(dbf);
    fclose(metaf);
}

Database *DB_load(char *dbname){
    struct stat st = {0};

    //basedir + / + name + / + name.dat
    char *basedir = DB_BASEDIR;

    char dbfolder[MAX_PATHNAME] = {0};
    snprintf(dbfolder, MAX_PATHNAME, "%s/%s", basedir, dbname);

    char dbfile[2*MAX_PATHNAME] = {0};
    char metafile[2*MAX_PATHNAME] = {0};
    snprintf(dbfile, MAX_PATHNAME, "%s/%s%s", dbfolder, dbname, ".dat");
    snprintf(metafile, MAX_PATHNAME, "%s/%s%s", dbfolder, dbname, ".kv");

    if(stat(dbfolder, &st) < 0){ 
        //database not found
        return NULL;
    }

    Database *database = malloc(sizeof(Database));
    database->name = strdup(dbname);
    database->path = strdup(dbfolder);  

    //LOAD HASHTABLE
    
    database->indexfile.id = 0;  //TODO
    database->indexfile.offset = 0;  //TODO
    database->indexfile.reader = fopen(metafile, "rb");
    //ferror_check(database->indexfile.reader);
    database->indexfile.writer = fopen(metafile, "ab");
    //TODO: check fopen errno
    database->keyDir = Indexfile_load(database->indexfile.reader);

    //LOAD DATAFILE
    
    database->datafile.id = 0;  //TODO
    database->datafile.offset = 0;  //TODO
    database->datafile.reader = fopen(dbfile, "rb");
    database->datafile.writer = fopen(dbfile, "ab");
    //TODO: check fopen errno

    return database;
}

void DB_merge(Database *db){
    //create new temporary indexfile
    //Load indexfile to hashtable
    //Store from hashtable to temp indexfile
    //Change temp indexfile name to overwrite previous one
    //(what about datafile)
    //Updata db datafile handlers
}

void DB_free(Database *db){
    ht_freeTable(db->keyDir);
    fclose(db->datafile.reader);
    fclose(db->datafile.writer);

    free(db->name);
    free(db->path);
    free(db);
}

void DB_destroy(Database *db){
    // Delete database file
    char command[256];
    snprintf(command, sizeof(command), "rm -rf %s", db->path);
    if(system(command) == 0){
        printf("'%s' database deleted\n", db->name);
    }else{
        printf("Failed to delete '%s' database\n", db->name);
    }

    DB_free(db);
}

int DB_Insert(Database *db, char *key, uint8_t *data, size_t size){
    if(ht_search(db->keyDir, key)){
        //TODO: delete previous entry in hashmap and indexfile
        printf("Record found for given key\n");
        return -1;
    }
    
    //create and store record file
    Record *rec = Record_create(key, data, size);         
    Meta metadata;  //to be filled by Record_store
    if(Record_store(db, rec, &metadata) < 0){
        printf("Record store failure\n");
        return -1;
    }

    //append to indexfile 
    Indexfile_append(db->indexfile.writer, rec, &metadata);

    //store in hashmap
    ht_insert(db->keyDir, key, &metadata);
    fflush(db->datafile.writer);
    fflush(db->indexfile.writer);

    Record_free(rec);
    return 0;
}

Record *DB_Read(Database *db, char *key){
    fflush(db->datafile.reader);

    Meta *metadata = ht_retrieveVal(db->keyDir, key);
    printf("Metadata(%u(id) | %u(pos) | %u(size) | %u(ts))\n",
            metadata->FileID,
            metadata->RecordPos,
            metadata->RecordSize,
            metadata->Timestamp
          );


    Record *rec = Record_load(db, metadata->RecordPos);

    return rec;
}
