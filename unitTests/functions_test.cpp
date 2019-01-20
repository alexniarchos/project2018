#include "../join.h"
#include "../list.h"
#include "../functions.h"
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

TEST(none_of_two_in_midresults_TEST,test){
    vector<midResult*> midresults;

    SQLquery *query = new SQLquery();
    char line[100] = "0 1 1|0.0=1.0&0.1=2.0&0.2>3499|1.2 0.1";
    query->parser(line);

    int numofrels;
    relation **rels = (relation**)malloc(2 * sizeof(relation*));
    rels[0] = new relation();
    rels[1] = new relation();
    rels[0]->numofentries = 10;
    rels[1]->numofentries = 5;
    rels[0]->numofcols = 2;
    rels[1]->numofcols = 2;
    rels[0]->cols = (uint64_t**)malloc(rels[0]->numofcols*sizeof(uint64_t*));
    rels[1]->cols = (uint64_t**)malloc(rels[1]->numofcols*sizeof(uint64_t*));
    rels[0]->cols[0] = (uint64_t*)malloc(rels[0]->numofentries*sizeof(uint64_t));
    rels[1]->cols[0] = (uint64_t*)malloc(rels[1]->numofentries*sizeof(uint64_t));
    for(int i=0;i<rels[0]->numofentries;i++){
        rels[0]->cols[0][i] = i;
    }

    for(int i=0;i<rels[1]->numofentries;i++){
        rels[1]->cols[0][i] = i;
    }
    none_of_two_in_midresults(query,0,rels,midresults);
    for(int i=0;i<midresults[0]->colSize;i++){
        // cout << midresults[0]->cols[0][i] << "," << midresults[0]->cols[1][i] << endl;
        ASSERT_EQ(midresults[0]->cols[0][i],midresults[0]->cols[1][i]);
    }
}