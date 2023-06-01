#include "kvpair.h"

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, char *key, void *val){
    KVpair *kv = malloc(sizeof(KVpair));

    kv->klen = klen;
    kv->vlen = vlen;

    kv->key = malloc(klen);
    strncpy(kv->key, key, klen);

    kv->val = malloc(vlen);
    memcpy(kv->val, val, vlen);

    return kv;
}

void KVpair_free(KVpair *ptr){
    if(ptr->key) free(ptr->key);
    if(ptr->val) free(ptr->val);
    if(ptr) free(ptr);
}

uint32_t KVpair_getKey(KVpair *ptr){
    return atol(ptr->key);
}

size_t KVpair_getSize(KVpair *ptr){
    return 2*sizeof(uint32_t) + ptr->klen + ptr->vlen;
}
