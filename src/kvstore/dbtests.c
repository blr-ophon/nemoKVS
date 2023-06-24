#include "dbtests.h"
#include <time.h>
#include <stdlib.h>


void DBtests_all(PageTable *t,BPtree *tree, int n){
    srand(time(NULL));
    /*
    DBtests_custom(t, tree);
    */
    DBtests_inorder(t, tree, n);
    DBtests_revorder(t, tree, n);
    DBtests_randorder(t, tree, n);
    for(int i = 0; i < 10000; i++){
        if(DBtests_randorder(t, tree, n) < 0){
            printf("> (%d)Found remaining keys\n", i);
            break;
        }
    }
}

//ERROR: degree 4, n = 10
//16, 19, 16, 10, 19, 28, 1, 28, 25, 28
//4,19,22,19,4,22,16,28,19,4 
//
//degree 6, n = 10
//4 - 7 - 13 - 7 - 16 - 16 - 4 - 10 - 1 - 10    OK

//degree 6, n = 30
//46 - 1 - 79 - 58 - 70 - 82 - 73 - 7 - 58 - 82 - 28 - 88 - 79 - 25 - 73 - 70 - 43 - 34 - 85 - 13 - 88 - 49 - 40 - 28 - 85 - 52 - 31 - 1 - 55 - 19 -
//{46, 1, 79, 58, 70, 82, 73, 7, 58, 82, 28, 88, 79, 25, 73, 70, 43, 34, 85, 13, 88, 49, 40, 28, 85, 52, 31, 1, 55, 19}
//

void DBtests_custom(PageTable *t, BPtree *tree){ 
    int n = 30;
    //char tests[10] = {1,2,3,4,5,6,7,8,9,10};
    char tests[30] = {46, 1, 79, 58, 70, 82, 73, 7, 58, 82, 28, 88, 79, 25, 73, 70, 43, 34, 85, 13, 88, 49, 40, 28, 85, 52, 31, 1, 55, 19};

    //Create kvs
    KVpair **KVs = malloc(n*sizeof(void*));
    int i;
    for(i = 0; i < n; i++){
        char key[32] = {0};
        char val[32] = {0};
        sprintf(key, "%d", tests[i]);
        sprintf(val, "testVal-%d", tests[i]);
        KVs[i] = KVpair_create(strlen(key), strlen(val), key, val);
    }

    printf("%d sequential key-value pairs created for B+tree of degree %d\n",
            n , tree->degree);

    //insert
    for(i = 0; i < n; i++){
        BPtree_insert(t, tree, KVs[i]);
    }
    printf("%d key-value pairs successfully inserted (IN ORDER)\n", i);
    DBtests_search(t, tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPT_delete(t, tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted (IN ORDER)\n", i);

    //try search
    if(DBtests_search(t, tree, KVs, n)){
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

int DBtests_search(PageTable *t, BPtree *tree, KVpair **KVs, int n){
    int i;
    //try search
    int search_kv_n = 0;
    for(i = 0; i < n; i++){
        if(BPtree_search(t,tree, KVs[i], NULL)){
            search_kv_n++;
            //printf("KV[%d] found\n", i);
        }
    }
    printf("Search complete: ");
    printf("%d key-value pairs found\n", search_kv_n);

    return search_kv_n;
}

int DBtests_randorder(PageTable *t, BPtree *tree, int n){
    KVpair **KVs = malloc(n*sizeof(void*));
    int i;

    //create
    for(i = 0; i < n; i++){
        int key_num = rand() % n*3 +1;
        printf("%d - ", key_num);
        char key[7];
        snprintf(key, 7, "%d", key_num);
        char val[21];
        snprintf(val, 21, "test-VAL%d", key_num);
        KVs[i] = KVpair_create(strlen(key), strlen(val), key, val);
    }
    printf("\n%d RANDOM key-value pairs created for B+tree of degree %d\n",
            i, tree->degree);

    //insert
    for(i = 0; i < n; i++){
        BPtree_insert(t, tree, KVs[i]);
    }
    printf("%d key-value pairs successfully inserted\n", i);
    DBtests_search(t, tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPT_delete(t, tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted\n", i);

    //try search. Check if there are remaining keys
    int rmn_keys = DBtests_search(t, tree, KVs, n);  
    if(rmn_keys){
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

    if(rmn_keys > 0){
        return -1;
    }
    return 0;
}


void DBtests_inorder(PageTable *t, BPtree *tree, int n){
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
        BPtree_insert(t, tree, KVs[i]);
    }
    printf("%d key-value pairs successfully inserted (IN ORDER)\n", i);
    DBtests_search(t, tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPT_delete(t, tree, KVs[i]);
    }
    printf("%d key-value pairs successfully deleted (IN ORDER)\n", i);

    //try search
    if(DBtests_search(t, tree, KVs, n)){
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

void DBtests_revorder(PageTable *t, BPtree *tree, int n){
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
        BPtree_insert(t, tree, KVs[n-1 - i]);
    }
    printf("%d key-value pairs successfully inserted (IN REVERSE ORDER)\n", i);

    DBtests_search(t, tree, KVs, n);

    //delete
    for(i = 0; i < n; i++){
        BPT_delete(t, tree, KVs[n-1 - i]);
    }
    printf("%d key-value pairs successfully deleted (IN REVERSE ORDER)\n", i);

    //search
    if(DBtests_search(t, tree, KVs, n)){
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
