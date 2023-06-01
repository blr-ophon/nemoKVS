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



KVpair *KVpair_decode(uint8_t *bytestream){ 
    uint32_t klen;
    memcpy(&klen, bytestream, sizeof(uint32_t));
    bytestream += sizeof(uint32_t);

    uint32_t vlen;
    memcpy(&vlen, bytestream, sizeof(uint32_t));
    bytestream += sizeof(uint32_t);

    char *key = malloc(klen); 
    memcpy(key, bytestream, klen);
    bytestream += klen;

    uint8_t *val= malloc(vlen); 
    memcpy(val, bytestream, vlen);

    KVpair *kv = KVpair_create(klen, vlen, key, val);
    free(key);
    free(val);
    return kv;
}
                                           
uint8_t *KVpair_encode(KVpair *kv){
    size_t streamSize = KVpair_getSize(kv);
    uint8_t *bytestream = malloc(streamSize);

    memcpy(bytestream, &kv->klen, sizeof(uint32_t));
    memcpy(bytestream, &kv->vlen, sizeof(uint32_t));
    memcpy(bytestream, kv->key, kv->klen);
    memcpy(bytestream, kv->val, kv->vlen);

    return bytestream;
}

void KVpair_removeVal(KVpair *kv){
    kv->vlen = 0;
    if(kv->val) free(kv->val);
}



//positive if kv1 > kv2 | 0 if kv1 == kv2 | negative if kv1 < kv2
int KVpair_compare(KVpair *kv1, KVpair *kv2){
    return atol(kv1->key) - atol(kv2->key);
}

size_t KVpair_getSize(KVpair *ptr){
    return 2*sizeof(uint32_t) + ptr->klen + ptr->vlen;
}
