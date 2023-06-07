#include "dbtests.h"
#include <time.h>
#include <stdlib.h>

//ERROR: degree 4, n = 10
//16, 19, 16, 10, 19, 28, 1, 28, 25, 28

void DBtests_all(BPtree *tree, int n){
    DBtests_inorder(tree, n);
    DBtests_revorder(tree, n);
    DBtests_randorder(tree, n);
}

void DBtests_randorder(BPtree *tree, int n){
    KVpair **KVs = malloc(n*sizeof(void*));
    int i;
    srand(time(NULL));

    //create
    for(i = 0; i < n; i++){
        int key_num = rand() % n*3 +1;
        char key[7];
        snprintf(key, 7, "%d", key_num);
        char val[21];
        snprintf(val, 21, "test-VAL%d", key_num);
        KVs[i] = KVpair_create(strlen(key), strlen(val), key, val);
    }
    printf("%d RANDOM key-value pairs created for B+tree of degree %d\n",
            i, tree->degree);

    //insert
    for(i = 0; i < n; i++){
        BPtree_insert(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully inserted\n", i);

    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted\n", i);

    //try search
    int search_kv_n = 0;
    for(i = 0; i < n; i++){
        if(BPtree_search(tree, KVs[i], NULL)){
            search_kv_n++;
            printf("KV[%d] found\n", i);
        }
    }
    printf("Search complete\n");
    printf("%d key-value pairs found\n\n", search_kv_n);
    if(search_kv_n){
        printf("Keys: \n");
        for(i = 0; i < n; i++){
            printf("%s ", KVs[i]->key);
        }
        printf("\n");
    }



    //free
    for(int i = 0; i < n; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);

}

void DBtests_inorder(BPtree *tree, int n){
    KVpair **KVs = malloc(n*sizeof(void*));
    int i;

    //create
    for(i = 0; i < n; i++){
        char key[7];
        snprintf(key, 7, "%d", i);
        char val[21];
        snprintf(val, 21, "test-VAL%d", i);
        KVs[i] = KVpair_create(strlen(key), strlen(val), key, val);
    }
    printf("%d sequential key-value pairs created for B+tree of degree %d\n",
            i, tree->degree);

    //insert
    for(i = 0; i < n; i++){
        BPtree_insert(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully inserted (IN ORDER)\n", i);

    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted (IN ORDER)\n", i);

    //try search
    int search_kv_n = 0;
    for(i = 0; i < n; i++){
        if(BPtree_search(tree, KVs[i], NULL)){
            search_kv_n++;
        }
    }
    printf("Search complete\n");
    printf("%d key-value pairs found\n\n", search_kv_n);

    //free
    for(int i = 0; i < n; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);
}

void DBtests_revorder(BPtree *tree, int n){
    KVpair **KVs = malloc(n*sizeof(void*));
    int i;

    //create
    for(i = 0; i < n; i++){
        char key[7];
        snprintf(key, 7, "%d", i);
        char val[21];
        snprintf(val, 21, "test-VAL%d", i);
        KVs[i] = KVpair_create(strlen(key), strlen(val), key, val);
    }
    printf("%d sequential key-value pairs created for B+tree of degree %d\n",
            i, tree->degree);

    //insert
    for(i = 0; i < n; i++){
        BPtree_insert(tree, KVs[n-1 - i]);
    }
    printf("%d key-value pairs successfully inserted (IN REVERSE ORDER)\n", i);
    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[n-1 - i]);
    }
    printf("%d key-value pairs successfully deleted (IN REVERSE ORDER)\n", i);
    //try search
    int search_kv_n = 0;
    for(i = 0; i < n; i++){
        if(BPtree_search(tree, KVs[i], NULL)){
            search_kv_n++;
        }
    }
    printf("Search complete\n");
    printf("%d key-value pairs found\n\n", search_kv_n);


    //free
    for(int i = 0; i < n; i++){
        KVpair_free(KVs[i]);
    }
    free(KVs);
}
