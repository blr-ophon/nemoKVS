#include "dbtests.h"
#include <time.h>
#include <stdlib.h>

/*

//ERROR: degree 4, n = 10
//16, 19, 16, 10, 19, 28, 1, 28, 25, 28

void DBtests_custom(BPtree *tree){
    BPtreeNode *root = tree->root;
    int n = 10;
    KVpair **KVs = malloc(n*sizeof(void*));
    int i;
    KVs[0] = KVpair_create(2, 10, "16", "testVal-16");
    KVs[1] = KVpair_create(2, 10, "19", "testVal-19");
    KVs[2] = KVpair_create(2, 10, "16", "testVal-16");
    KVs[3] = KVpair_create(2, 10, "10", "testVal-10");
    KVs[4] = KVpair_create(2, 10, "19", "testVal-19");
    KVs[5] = KVpair_create(2, 10, "28", "testVal-28");
    KVs[6] = KVpair_create(1, 9, "1", "testVal-1");
    KVs[7] = KVpair_create(2, 10, "28", "testVal-28");
    KVs[8] = KVpair_create(2, 10, "25", "testVal-25");
    KVs[9] = KVpair_create(2, 10, "28", "testVal-28");

    printf("%d sequential key-value pairs created for B+tree of degree %d\n",
            10 , tree->degree);

    //insert
    for(i = 0; i < n; i++){
        BPtree_insert(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully inserted (IN ORDER)\n", i);
    DBtests_search(tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted (IN ORDER)\n", i);

    //try search
    if(DBtests_search(tree, KVs, n)){
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
    printf("\n");
}

int DBtests_search(BPtree *tree, KVpair **KVs, int n){
    int i;
    //try search
    int search_kv_n = 0;
    for(i = 0; i < n; i++){
        if(BPtree_search(tree, KVs[i], NULL)){
            search_kv_n++;
            //printf("KV[%d] found\n", i);
        }
    }
    printf("Search complete: ");
    printf("%d key-value pairs found\n", search_kv_n);

    return search_kv_n;
}

void DBtests_all(BPtree *tree, int n){
    //DBtests_custom(tree);
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
    DBtests_search(tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted\n", i);

    //try search
    if(DBtests_search(tree, KVs, n)){
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
    printf("\n");
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
    DBtests_search(tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted (IN ORDER)\n", i);

    //try search
    if(DBtests_search(tree, KVs, n)){
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
    printf("\n");
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

    DBtests_search(tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPtree_delete(tree, KVs[n-1 - i]);
    }
    printf("%d key-value pairs successfully deleted (IN REVERSE ORDER)\n", i);

    //search
    if(DBtests_search(tree, KVs, n)){
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
    printf("\n");
}
*/
