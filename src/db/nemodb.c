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
//TODO: somewhere, malloc was being used incorrectly and causing random
//segfaults (bptree, kvpair and node). Look for this.(all mallocs were in
//kvpair.c except for 1 that was in tree or node, gotta look past commits 
//to confirm where)

int main(void){
    //BPtree *tree = BPtree_create(4);
    //BPtree_print(tree);

    //DBtests_all(tree, 10);
    DB_create("test_db");
    Database *db = DB_load("test_db");
    if(!db){
        printf("database not found\n");
        return -1;
    }
    printf("Database loaded\n");

    //tree struct is kinda useless, degree wont be a thing once
    //node pages are used instead and Mroot id is always 1. create
    //a MROOT_PAGE_ID 1
    BPtree *tree = BPtree_create(db->table, 6);

    DBtests_all(db->table, tree, 6);

    free(tree);
    DB_free(db);

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
    snprintf(dbfile, MAX_PATHNAME, "%s/%s%s", dbfolder, name, ".dat");

    //TODO: ask if user wishes to overwrite if it's the same name
    int fd = open(dbfile, O_WRONLY | O_CREAT | O_TRUNC);
    chmod(dbfile, S_IRUSR | S_IWUSR);
    
    //create page table 
    uint8_t *emptyPages = calloc(2, getpagesize());

    uint32_t page_num = 2;  //page 0 and 1
    memcpy(&emptyPages[0], &page_num, sizeof(uint32_t));
    emptyPages[0 +4] = 1;        //page table
    emptyPages[1 +4] = 1;        //master root
                                 
    //write page table 
    lseek(fd, 0*getpagesize(), SEEK_SET);
    write(fd, emptyPages, 2*getpagesize());
    free(emptyPages);

    /*
    //create master root 
    BPtree *tree = BPtree_create(node_len);
    int node_size = BPtreeNode_getSize(tree->root);
    uint8_t *bytestream = BPtreeNode_encode(tree->root);

    //write master root 
    lseek(fd, 1*getpagesize(), SEEK_SET);
    write(fd, bytestream, node_size);
    */

    close(fd);
}

Database *DB_load(char *dbname){
    struct stat st = {0};

    //basedir + / + name + / + name.dat
    char *basedir = DB_BASEDIR;

    char dbfolder[MAX_PATHNAME] = {0};
    snprintf(dbfolder, MAX_PATHNAME, "%s/%s", basedir, dbname);

    char dbfile[2*MAX_PATHNAME] = {0};
    snprintf(dbfile, MAX_PATHNAME, "%s/%s%s", dbfolder, dbname, ".dat");

    if(stat(dbfolder, &st) < 0){ 
        //database not found
        return NULL;
    }

    Database *database = malloc(sizeof(Database));
    database->name = strdup(dbname);
    database->path = strdup(dbfolder);  

    //LOAD PAGER
    //map page 0 to pager
    int fd = open(dbfile, O_RDWR);
    database->table = pager_init(fd);

    return database;
}

void DB_free(Database *db){
    //TODO: free table / unmap maps
    free(db->table->entries);
    free(db->table);

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
    //create kvpair with key and data

    //insert kvpair in bptree
}

KVpair *DB_Read(Database *db, char *key){
}
