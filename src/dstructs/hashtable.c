#include "hashtable.h"
#include "kvstore.h"

#define MAX_NAME 128
#define TABLE_SIZE 128      //TODO: remove this 
                            
//TODO: resize function to use when more than 3 attempts are made 
//to insert an item. Duplicate the size but keep the same hashes for
//the previous values

void Indexfile_append(FILE *f, Record *rec, Meta *metadata){
    //move cursor
    fseek(f, 0, SEEK_END);

    //append key lenght
    uint32_t K_len = rec->header.Ksize;
    int rv = fwrite(&K_len, sizeof(uint32_t), 1, f);
    ferror_check(f, rv);

    //append key 
    rv = fwrite(rec->key, sizeof(char), K_len, f);
    ferror_check(f, rv);

    //append metadata
    rv = fwrite(metadata, sizeof(Meta), 1, f);   
    ferror_check(f, rv);
    fflush(f);
}

HTable *Indexfile_load(FILE *f){
    HTable *table = ht_createTable(TABLE_SIZE);

    //test for empty indexfile
    fseek(f, 0, SEEK_END);
    uint32_t flength = ftell(f);
    if(flength < sizeof(Meta)){
        return table;
    }

    //read all entries from indexfile to hashtable
    rewind(f);
    for(;;){
        //read key length
        uint32_t K_len;
        int rv = fread(&K_len, sizeof(uint32_t), 1, f);
        if(feof(f)) break;
        ferror_check(f, rv);

        //read key 
        char *key = calloc(K_len+1, sizeof(char));
        rv = fread(key, sizeof(char), K_len, f);
        ferror_check(f, rv);

        //read metadata
        Meta *metadata = malloc(sizeof(Meta));
        rv = fread(metadata, sizeof(Meta), 1, f);
        ferror_check(f, rv);

        //insert in hash table
        ht_insert(table, key, metadata);
        free(key);
        free(metadata);
    }
    fflush(f);
    return table;

}

void ht_printTable(HTable *table){
    for(size_t i = 0; i < table->size; i++){
        if(table->entries[i]){
            printf("%s : %u\n", 
                    table->entries[i]->key,
                    table->entries[i]->val.RecordPos

                  );
        }
    }
}

uint32_t ht_djb2(char *key, uint32_t table_size){
    int len = strnlen(key, MAX_NAME);

    uint32_t hash = 5381;
    for(int i = 0; i < len; i++){
        hash = ((hash << 5) + hash) + key[i];
    }

    return hash % table_size;
}


uint32_t ht_hash(char *key, int attempts, size_t table_size){
    uint32_t h1 = ht_djb2(key, table_size);
    if(attempts == 0){
        return h1;
    }

    //double hashing
    uint32_t h2 = 1 + (ht_djb2(key, table_size- 2) % (table_size- 2));
    return (h1 + attempts * h2) % table_size;
}


void ht_insert(HTable *table, char *key, Meta *val){
    uint32_t hash = ht_hash(key, 0, table->size);
    HT_entry **entries = table->entries;

    if(entries[hash] == NULL){
        entries[hash] = ht_createEntry(key, val);

    //Collision
    }else{
        //hash while entry is filled
        int i = 0;
        do{
            if(strncmp(entries[hash]->key, key, MAX_NAME)){
                //ignore if trying to insert repeated entry
                return;
            }
            hash = ht_hash(key, ++i, table->size);
        }while(entries[hash]);

        entries[hash] = ht_createEntry(key, val);
    }
}

void ht_delete(HTable *table, char *key){
    uint32_t hash = ht_hash(key, 0, table->size);
    HT_entry **entries = table->entries;

    if(entries[hash]){
        //entry is found
        if(strncmp(entries[hash]->key, key, MAX_NAME) == 0){
            //if key matches
            ht_freeEntry(entries[hash]);
            entries[hash]->key = NULL;
            //entries[hash]->val = NULL;

            
        //Collision
        }else{
            //hash while key doesn't match
            int i = 0;
            do{
                hash = ht_hash(key, ++i, table->size);
                if(entries[hash] == NULL){
                    //no entry found
                    return;
                }
            }while(strncmp(entries[hash]->key, key, MAX_NAME));

            ht_freeEntry(entries[hash]);
            entries[hash]->key = NULL;
            //entries[hash]->val = NULL;
        }
    }
}

HT_entry *ht_search(HTable *table, char *key){
    uint32_t hash = ht_hash(key, 0, table->size);
    HT_entry **entries = table->entries;
    

    if(entries[hash]){
        //entry is found
        if(strncmp(entries[hash]->key, key, MAX_NAME) == 0){
            //if key matches
            return entries[hash];
            
        //Collision
        }else{
            //hash while key doesn't match
            int i = 0;
            do{
                hash = ht_hash(key, ++i, table->size);
                if(entries[hash] == NULL){
                    //no entry found
                    return NULL;
                }
            }while(strncmp(entries[hash]->key, key, MAX_NAME));

            return entries[hash];
        }
    }

    return NULL;
}

Meta *ht_retrieveVal(HTable *table, char *key){
    HT_entry *entry = ht_search(table, key);
    return &entry->val;
}


HT_entry *ht_createEntry(char *key, Meta *val){
    HT_entry *entry = malloc(sizeof(HT_entry));
    entry->key = strdup(key);
    memcpy(&entry->val, val, sizeof(Meta));
    return entry;
}


HTable *ht_createTable(size_t size){
    HTable *table = malloc(sizeof(HTable)); 

    table->entries = calloc(size, sizeof(HT_entry*));
    table->size = size;
    table->count = 0;

    return table;
}

void ht_freeEntry(HT_entry *entry){
    free(entry->key);
}

void ht_freeTable(HTable *table){
    //free all entries
    for(size_t i = 0; i < table->size; i++){
        if(table->entries[i]){
            ht_freeEntry(table->entries[i]);
        }
    }
    free(table->entries);
    free(table);
}


