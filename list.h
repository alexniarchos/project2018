#include <stdlib.h>

#ifndef LIST_H
#define LIST_H

#define bufsize 128000 //size of bytes for each listnode tuple array

struct result{
    int rowId1;
    int rowId2;
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
        listnode *tail=NULL;

    list(){tupleCount = 0;}

    void add(int num1,int num2);

    void print();

    ~list();
};

#endif