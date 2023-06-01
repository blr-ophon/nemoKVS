#ifndef KVPAIR_H
#define KVPAIR_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct{
    uint32_t klen;
    uint32_t vlen;
    char *key;
    uint8_t *val;
}KVpair;


KVpair *KVpair_create(uint32_t klen, uint32_t vlen, char *key, void *val);
void KVpair_free(KVpair *ptr);

KVpair *KVpair_decode(uint8_t *bytestream);     //convert byte array to KV pair
uint8_t *KVpair_encode(KVpair *kv);             //convert KV pair to byte array
void KVpair_removeVal(KVpair *kv);

int KVpair_compare(KVpair *kv1, KVpair *kv2);
size_t KVpair_getSize(KVpair *ptr);             

#endif
