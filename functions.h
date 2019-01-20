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
#include "parser.h"
#include "join.h"
#include <string>

enum Case { InMidResult, NotInMidResult, NoneInMidResults, OneInMidResult, BothInSameMidResult, BothInDiffMidResult};

class midResult{
    public:
        vector<int*> cols;
        int colSize;
        vector<int> relId;
};

class statisticRelation{
    public:
        int rel_index;
        ColStats **colStats;
        int numofcols;
};

class Statistics{
    public:
        vector<int*> predicates; //calculated predicates
        vector<statisticRelation*> relations;
        uint64_t score;
        Statistics():score(0){}
        Statistics(const Statistics *oldstatistic){
            for(int i=0;i<oldstatistic->predicates.size();i++){
                predicates.push_back(oldstatistic->predicates[i]);
            }
            score=oldstatistic->score;
            for(int i=0;i<oldstatistic->relations.size();i++){
                statisticRelation* newrel=new statisticRelation();
                newrel->numofcols=oldstatistic->relations[i]->numofcols;
                newrel->rel_index=oldstatistic->relations[i]->rel_index;
                newrel->colStats=(ColStats**)malloc(oldstatistic->relations[i]->numofcols*sizeof(ColStats*));
                for(int j=0;j<oldstatistic->relations[i]->numofcols;j++){
                    newrel->colStats[j]=(ColStats*)malloc(sizeof(ColStats));
                    memcpy(newrel->colStats[j],oldstatistic->relations[i]->colStats[j],sizeof(ColStats));
                }
                relations.push_back(newrel);
            }
        }
        ~Statistics(){
            for(int i=0;i<predicates.size();i++){
                predicates.erase(predicates.begin());
            }
            for(int i=0;i<relations.size();i++){
                for(int j=0;j<relations[i]->numofcols;j++){
                    free(relations[i]->colStats[j]);
                }
                free(relations[i]->colStats);
                delete(relations[i]);
            }
            for(int i=0;i<relations.size();i++)
                relations.erase(relations.begin());
        }
};


void categoriser(SQLquery* query,relation **rels,vector<string*> &results,int numofrels);
int checkfilter(SQLquery* query);
void none_of_two_in_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults);
void both_in_diff_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults);
int checkmidresults(int rel_index,vector<midResult*> &midresults);
Case samerelCases(SQLquery* query,int index,vector<midResult*> &midresults);
Case diffrelCases(SQLquery* query,int index,vector<midResult*> midresults);
void generateResults(SQLquery* query,relation** rels,vector<midResult*> &midresults,vector<string*> &results);
void executefilters(SQLquery* query,relation **rels,vector<midResult*> &midresults);
void scansamerel(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults);
void scansamemidresults(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults);
void samerelation(SQLquery* query,relation **rels,int index,vector<midResult*> &midresults);
void diffrelationsamemidresult(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults);
void diffrelationoneonmidresult(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults);
void differentrelation(SQLquery* query,relation **rels,int index,vector<midResult*> &midresults);