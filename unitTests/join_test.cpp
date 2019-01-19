#include "../join.h"
#include "../list.h"
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

JobScheduler *jobScheduler;

TEST(dec_to_bin_TEST, test) {
    ASSERT_EQ(dec_to_bin(10),1010);
    ASSERT_EQ(dec_to_bin(123),1111011);
}

TEST(hashvalue_TEST, test){
    ASSERT_EQ(hashvalue(10,3),2);
    ASSERT_EQ(hashvalue(101011,3),3);
}

TEST(sort_hashtable_TEST, test){
    // init job scheduler
    jobScheduler = new JobScheduler();
    jobScheduler->Init(THREADS);

    int numofentries = 10;

    uint64_t col[numofentries] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    int *hist,*psum;
    Tuple *hash;
    // init arrays and sort
    hash = (Tuple*)malloc(numofentries*sizeof(Tuple));
    for (int i=0;i<numofentries;i++){
        hash[i].key = -1;
    }
    hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        hist[i]=0;
    psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(col,numofentries,&hash,&hist,&psum);

    ASSERT_EQ(hash[0].key,7);
    ASSERT_EQ(hash[0].payload,8);
    ASSERT_EQ(hash[9].key,6);
    ASSERT_EQ(hash[9].payload,7);

    free_memory(&hash,&hist,&psum);

    // Stop job scheduler
    jobScheduler->Stop();
    delete jobScheduler;
}

TEST(create_indexing_TEST, test){
    // init job scheduler
    jobScheduler = new JobScheduler();
    jobScheduler->Init(THREADS);

    int numofentries = 4;

    uint64_t col[numofentries] = { 1, 2, 3, 3};

    int *hist,*psum,*chain,*bucket;
    Tuple *hash;
    // init arrays and sort
    hash = (Tuple*)malloc(numofentries*sizeof(Tuple));
    for (int i=0;i<numofentries;i++){
        hash[i].key = -1;
    }
    hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        hist[i]=0;
    psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(col,numofentries,&hash,&hist,&psum);

    create_indexing(numofentries,hash,hist,&chain,&bucket);

    ASSERT_EQ(chain[0],-1);
    ASSERT_EQ(chain[3],2);

    ASSERT_EQ(bucket[14],0);

    free(chain);
    free(bucket);
}

TEST(getResults_TEST,test){
    // init job scheduler
    jobScheduler = new JobScheduler();
    jobScheduler->Init(THREADS);

    Tuple *A_Sorted,*B_Sorted;
    int *A_hist,*A_psum,*B_hist,*B_psum,*A_chain,*A_bucket,*B_chain,*B_bucket;
    list *l;
    int A_size = 10, B_size = 5;

    uint64_t A[A_size] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    uint64_t B[B_size] = { 1, 2, 3, 3, 5 };

    // init arrays and sort
    A_Sorted = (Tuple*)malloc(A_size*sizeof(Tuple));
    for (int i=0;i<A_size;i++){
        A_Sorted[i].key = -1;
    }
    A_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        A_hist[i]=0;
    A_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(A,A_size,&A_Sorted,&A_hist,&A_psum);

    B_Sorted = (Tuple*)malloc(B_size*sizeof(Tuple));
    for (int i=0;i<B_size;i++){
        B_Sorted[i].key = -1;
    }
    B_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        B_hist[i]=0;
    B_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(B,B_size,&B_Sorted,&B_hist,&B_psum);

    // Create indexing to the array with the least amount of entries
    create_indexing(B_size,B_Sorted,B_hist,&B_chain,&B_bucket);
    l = getResults(A_size,A_Sorted,A_hist,B_Sorted,B_chain,B_bucket,1);
    free(B_chain);
    free(B_bucket);

    free_memory(&A_Sorted,&A_hist,&A_psum);
    free_memory(&B_Sorted,&B_hist,&B_psum);

    ASSERT_EQ(l->head->tuples[0].rowId1,0);
    ASSERT_EQ(l->head->tuples[0].rowId2,0);
    ASSERT_EQ(l->head->tuples[1].rowId1,1);
    ASSERT_EQ(l->head->tuples[1].rowId2,1);
    ASSERT_EQ(l->head->tuples[2].rowId1,2);
    ASSERT_EQ(l->head->tuples[2].rowId2,3);
}

TEST(RadixHashJoin_TEST,test){
    int A_size = 10, B_size = 5;
    uint64_t A[A_size] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    uint64_t B[B_size] = { 1, 2, 3, 3, 5 };

    list *l = RadixHashJoin(A,A_size,B,B_size);
    ASSERT_EQ(l->head->tuples[0].rowId1,0);
    ASSERT_EQ(l->head->tuples[0].rowId2,0);
    ASSERT_EQ(l->head->tuples[1].rowId1,1);
    ASSERT_EQ(l->head->tuples[1].rowId2,1);
    ASSERT_EQ(l->head->tuples[2].rowId1,2);
    ASSERT_EQ(l->head->tuples[2].rowId2,3);
}



