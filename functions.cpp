#include "functions.h"

int checkfilter(SQLquery* query){
    for(int i=0;i<query->predicates.size();i++){
        if(query->predicates[i][4]==-1){
            return i;
        }
    }
    return -1;
}

void categoriser(SQLquery* query){
    int index;
    while((index=checkfilter(query))!=-1){
        query->predicates.erase(query->predicates.begin()+index);
        //execute filters
    }
    //build score array with size the number of non filter predicates at the beggining
    //while loop through non-filter-predicates
        //sort the predicates according to the relations used
        //execute predicate
            //situations
                //1)are at the same relation
                    //execute using scan)
                //2)belong to different relations
                    //2.1)none of 2 are in mid results
                        //execute using rhj and build midresult object
                    //2.2)one of 2 belongs to midresults array of objects
                        //execute using rhj and add the second relation column to the midresult object the other relation is
                    //2.3)2 of 2 belong to the same midresult object
                        //execute using scan and update the midresult object
                    //2.4)2 of 2 belong to different midresult objects
                        //execute using scan and merge the midresults objects
            //end of situations
        //end of execution
        //add after each iteration the new relations that have been used to the score array
    //endfor
}