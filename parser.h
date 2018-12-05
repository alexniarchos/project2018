#include <cstdio>
#include "string.h"
#include <cstdlib>
#include <iostream>
#include <vector>

#ifndef PARSER_H
#define PARSER_H
using namespace std;
class SQLquery{
    
    void parserelations(char* stringrelations);
    void parsepredicates(char* stringpredicates);
    void parseviews(char* stringviews);

    public:
        vector<int*> views; //has 2 values ||    for example r0.c1 will be 0 1
        vector<int> relations;
        vector<int*> predicates; //has 5 values and if it is a filter the 5th value is -1|| for example r0.c1=r1.c2 values will be 0 1 0 1 2 and at this r0.c1>3000 0 1 1 3000 -1
        
        SQLquery(){};
        int parser(char* query);


};
#endif