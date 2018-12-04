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

using namespace std;

#ifndef JOIN_H
#define JOIN_H

#define n_last_digits 3 //number of last digits for hash function 1
#define divisor 13 //hash function 2 mod value
#define bufsize 40 //size of bytes for each listnode tuple array

// globals
extern int numofbuckets;

struct Tuple {
    int key;
    uint64_t payload;
};

struct result {
    int rowId1;
    int rowId2;
};

struct relation{
    uint64_t **cols;
    int numofcols;
    int numofentries;
};

class midResult{
    public:
        vector<int*> cols;
        int colSize;
        vector<int> relId;
};

class listnode{
    public:

    listnode *next;
    result *tuples;

    listnode();

    void add(int tupleCount,int num1,int num2);

    ~listnode(){free(tuples);}
};

class list{
    public:

    int tupleCount;
    listnode *head=NULL;

    list(){tupleCount = 0;}

    void add(int num1,int num2);

    void print();

    ~list();
};

uint64_t dec_to_bin(uint64_t decimal);
int hashvalue(uint64_t num,int divider);
int getnumofentries(char* file_name);
relation** init_relations(int *numofrels);
void sort_hashtable(uint64_t *col,int numofentries,Tuple** hash,int** hist,int** psum);
int hashfun2(int value);
void create_indexing(int numofentries,Tuple *table,int* hist, int** chain, int** bucket);
list* getResults(int numofentries,Tuple *A, Tuple *B,int *chain, int *bucket);
void free_memory(Tuple** hash,int** hist,int** psum);

#endif