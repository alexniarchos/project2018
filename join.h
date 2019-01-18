#include <iostream>
#include <stdlib.h>
#include <ctime>
#include <cstring>
#include <stdio.h>
#include <fstream>
#include <vector>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "job.h"

using namespace std;

#ifndef JOIN_H
#define JOIN_H

#define n_last_digits 3 //number of last digits for hash function 1
#define divisor 13 //hash function 2 mod value

// globals
extern int numofbuckets;
extern JobScheduler *jobScheduler;

struct ColStats{
    int l; // minimum value of collumn
    int u; // max value
    int f; // number of entries
    int d; // number of distinct values
};

class relation{
    public: 
        uint64_t **cols;
        ColStats **colStats;
        ColStats **tempcolStats;
        int numofcols;
        int numofentries;
};

uint64_t dec_to_bin(uint64_t decimal);
int hashvalue(uint64_t num,int divider);
int getnumofentries(char* file_name);
relation** init_relations(int *numofrels);
void sort_hashtable(uint64_t *col,int numofentries,Tuple** hash,int** hist,int** psum);
int hashfun2(int value);
void create_indexing(int numofentries,Tuple *table,int* hist, int** chain, int** bucket);
list* getResults(int numofentries,Tuple *A,int *A_hist,Tuple *B,int *chain, int *bucket,int biggestTable);
void free_memory(Tuple** hash,int** hist,int** psum);
list* RadixHashJoin(uint64_t* A, int A_size, uint64_t* B, int B_size);

#endif