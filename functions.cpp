#include "functions.h"
#include "join.h"

//execute using rhj and create new midresult object
void none_of_two_in_midresults(int r0,int c0, int r1,int c1,vector<midResult*> midresults,relation** rels){
    list *result=NULL;
    result = RadixHashJoin(rels[r0],c0,rels[r1],c1);
    midResult *midres = new midResult();
    midres->cols->push_back(vector<int>);
    midres->cols->push_back(vector<int>);
    // copy result into midResult object
    int sum=0,limit;
    listnode *temp = result->head;
    while(temp!=NULL){
        sum+=bufsize/sizeof(result);
        limit = bufsize/sizeof(result);
        if(sum > tupleCount){
            limit = tupleCount % (bufsize/sizeof(result));
        }
        for(int i=0;i<limit;i++){
            // cout << temp->tuples[i].rowId1 << " , " << temp->tuples[i].rowId2 << endl;
            midres->cols[0].push_back(temp->tuples[i].rowId1);
            midres->cols[1].push_back(temp->tuples[i].rowId2);
        }
        temp = temp->next;
    }
    midres->relId.push_back(r0);
    midres->relId.push_back(r1);
    midresults.push_back(midres);
}

//execute using scan and merge the midresults objects
void both_in_diff_midresults(int r0,int c0, int r1,int c1,vector<midResult*> midresults){

}

int checkfilter(SQLquery* query){
    for(int i=0;i<query->predicates.size();i++){
        if(query->predicates[i][4]==-1){
            return i;
        }
    }
    return -1;
}

void categoriser(SQLquery* query,relation **rels){
    int index;
    while((index=checkfilter(query))!=-1){
        int rel_index=query->predicates[index][0];
        int col_index=query->predicates[index][1];
        //checkmidresults(rel_index);
        if(query->predicates[index][2]==0){
            for(int i=0;i<rels[rel_index]->numofentries;i++){
                if(rels[rel_index]->cols[col_index][i]==query->predicates[index][3]){
                    //add to tempresults
                }
            }
        }
        else if(query->predicates[index][2]==1){
            for(int i=0;i<rels[rel_index]->numofentries;i++){
                if(rels[rel_index]->cols[col_index][i]>query->predicates[index][3]){
                    //add to tempresults
                }
            }
        }
        else{
            for(int i=0;i<rels[rel_index]->numofentries;i++){
                if(rels[rel_index]->cols[col_index][i]<query->predicates[index][3]){
                    //add to tempresults
                }
            }
        }
        //inform mid results
        query->predicates.erase(query->predicates.begin()+index);
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