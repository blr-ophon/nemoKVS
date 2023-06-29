#include "kvpair.h"

void KVpair_print(KVpair *kv){
    if(!kv){
        printf("> Empty KV pair\n");
        return;
    }
    printf("\n= KVpair:\n");
    printf("\n---- KV pair ----\n");
    printf("%u (klen) %u (vlen)\n", kv->klen, kv->vlen);
    printf("KEY: %s\nVAL: ", kv->key);          //TODO: use safer function
    for(uint32_t i = 0; i < kv->vlen; i++){
        printf("%c" ,kv->val[i]);
    }
    printf("\n");
}

KVpair *KVpair_create(uint32_t klen, uint32_t vlen, char *key, void *val){
    KVpair *kv = calloc(1, sizeof(KVpair));

    kv->klen = klen;
    kv->vlen = vlen;

    kv->key = calloc(1, klen);
    strncpy(kv->key, key, klen);

    kv->val = calloc(1, vlen);
    memcpy(kv->val, val, vlen);

    return kv;
}

void KVpair_free(KVpair *ptr){
    if(ptr){
        if(ptr->key) free(ptr->key);
        if(ptr->val) free(ptr->val);
        free(ptr);
    }
}



KVpair *KVpair_decode(uint8_t *bytestream){ 
    if(!bytestream){
        exit(1);
    }
    uint8_t *offset = bytestream;

    uint32_t klen;
    memcpy(&klen, offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    uint32_t vlen;
    memcpy(&vlen, offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    char *key = calloc(1, klen); 
    memcpy(key, offset, klen);
    offset += klen;

    uint8_t *val= calloc(1, vlen); 
    memcpy(val, offset, vlen);

    KVpair *kv = KVpair_create(klen, vlen, key, val);
    free(key);
    free(val);
    return kv;
}
                                           
uint8_t *KVpair_encode(KVpair *kv){
    size_t streamSize = KVpair_getSize(kv);
    uint8_t *bytestream = calloc(1, streamSize);

    uint8_t *tmp = bytestream;

    memcpy(tmp, &kv->klen, sizeof(uint32_t));
    tmp += sizeof(uint32_t);
    memcpy(tmp, &kv->vlen, sizeof(uint32_t));
    tmp += sizeof(uint32_t);
    memcpy(tmp, kv->key, kv->klen);
    tmp += kv->klen;
    memcpy(tmp, kv->val, kv->vlen);

    return bytestream;
}

void KVpair_removeVal(KVpair *kv){
    kv->vlen = 0;
    if(kv->val) {
        free(kv->val);
        kv->val = NULL;
    }
}

//positive if kv1 > kv2 | 0 if kv1 == kv2 | negative if kv1 < kv2
int KVpair_compare(KVpair *kv1, KVpair *kv2){
    /*
     * Using atol in key directly without converting to null termina-
     * ted string causes heap corruption in atol
     */
    
    char *key1 = calloc(1, kv1->klen +1);
    char *key2 = calloc(1, kv2->klen +1);
    memcpy(key1, kv1->key, kv1->klen);
    memcpy(key2, kv2->key, kv2->klen);

    int rv = atol(key1) - atol(key2);
    free(key1);
    free(key2);
    return rv;
}

size_t KVpair_getSize(KVpair *ptr){
    return 2*sizeof(uint32_t) + ptr->klen + ptr->vlen;
}
