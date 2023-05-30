#include "kvstore.h"
#include "hashtable.h"
#include "nemodb.h"

int ferror_check(FILE *f, int rv){
    if (rv ==  0) {
        if (feof(f)) {
            fprintf(stderr, "Error: Reached end of file.\n");
        } else if (ferror(f)) {
            perror("Error reading from file");
        } else {
            fprintf(stderr, "Unknown error occurred during file read.\n");
        }
        fclose(f);
        return -1;
    }
    return 0;
}

Record *Record_create(char *key, uint8_t *val, size_t n){
    Record *rec = (Record*) malloc(sizeof(Record));
    rec->key = strdup(key);
    rec->value = malloc(n);
    memcpy(rec->value, val, n);
    
    //HEADER
    rec->header.Ksize = strlen(key);
    rec->header.Vsize = n;
    //TODO
    rec->header.checksum = 0; 
    rec->header.timestamp = 0; 
    rec->header.expiry = 0; 

    return rec;
}

void Record_free(Record *rec){
    if(rec->key)    free(rec->key);
    if(rec->value)  free(rec->value);
    if(rec)         free(rec);
}

//store record in datafile, meta receives info on how the record was stored
int Record_store(Database *db, Record *rec, Meta *meta){
    //APPEND RECORD TO DATAFILE
    
    //set cursor
    size_t size = sizeof(Record) + rec->header.Ksize + rec->header.Vsize;
    fseek(db->datafile.writer, 0, SEEK_END);
    uint32_t fpos = ftell(db->datafile.writer);

    //write header, key and value in order
    int rv = fwrite(&rec->header, sizeof(RecHeader), 1, db->datafile.writer);
    if(ferror_check(db->datafile.writer, rv) < 0){
        return -1;
    }

    rv = fwrite(rec->key, sizeof(char), rec->header.Ksize, db->datafile.writer);
    if(ferror_check(db->datafile.writer, rv) < 0){
        return -1;
    }

    rv = fwrite(rec->value, sizeof(uint8_t), rec->header.Vsize, db->datafile.writer);
    if(ferror_check(db->datafile.writer, rv) < 0){
        return -1;
    }

    //create metadata
    meta->FileID = db->datafile.id;
    meta->RecordSize = size;
    meta->RecordPos = fpos;
    meta->Timestamp = rec->header.timestamp;        
    return 0;
}


//load record from datafile
Record *Record_load(Database *db, uint32_t offset){
    Record *rec = (Record*) calloc(1, sizeof(Record));

    //set cursor
    fseek(db->datafile.reader, offset, SEEK_SET);
    printf("FTELL: %li\n", ftell(db->datafile.reader));

    //read header
    int rv = fread(&rec->header, sizeof(RecHeader), 1, db->datafile.reader);
    if(ferror_check(db->datafile.reader, rv) < 0){
        Record_free(rec);
        return NULL;
    }
    printf("Header(%u(cs) | %u(ex) | %u(ts) | %u(ksize) | %u(vsize))\n",
            rec->header.checksum,
            rec->header.expiry,
            rec->header.timestamp,
            rec->header.Ksize,
            rec->header.Vsize
          );

    //read key
    rec->key = calloc(1, rec->header.Ksize + 1);
    rv = fread(rec->key, sizeof(char), rec->header.Ksize, db->datafile.reader);
    if(ferror_check(db->datafile.reader, rv) < 0){
        Record_free(rec);
        return NULL;
    }

    //read value
    rec->value = calloc(1, rec->header.Vsize);
    rv = fread(rec->value, sizeof(uint8_t), rec->header.Vsize, db->datafile.reader);
    if(ferror_check(db->datafile.reader, rv) < 0){
        Record_free(rec);
        return NULL;
    }
    return rec;
}
