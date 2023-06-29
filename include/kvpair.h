#ifndef KVPAIR_H
#define KVPAIR_H

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


//NODE
//| type | nkeys |  children  |   offsets  | key-values
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...
//
//KEY-VALUE
//| klen | vlen |  key |  value  | 
//|  4B  |  4B  | klen |  vlen   | 

typedef struct{
    uint32_t klen;
    uint32_t vlen;
    char *key;
    uint8_t *val;
}KVpair;



void KVpair_print(KVpair *kv);

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, char *key, void *val);
void KVpair_free(KVpair *ptr);

KVpair *KVpair_decode(uint8_t *bytestream);     //convert byte array to KV pair
uint8_t *KVpair_encode(KVpair *kv);             //convert KV pair to byte array
void KVpair_removeVal(KVpair *kv);

int KVpair_compare(KVpair *kv1, KVpair *kv2);
size_t KVpair_getSize(KVpair *ptr);             

#endif
