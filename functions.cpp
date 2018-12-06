#include "functions.h"
#include "join.h"

//execute using rhj and create new midresult object
void none_of_two_in_midresults(int r0,int c0, int r1,int c1,vector<midResult*> *midresults,relation** rels){
    list *result=NULL;
    result = RadixHashJoin(rels[r0],c0,rels[r1],c1);
    midResult *midres = new midResult();
    midres->cols.push_back((int*)malloc(result->tupleCount*sizeof(int)));
    midres->cols.push_back((int*)malloc(result->tupleCount*sizeof(int)));
    // copy result into midResult object
    int sum=0,limit,counter=0;
    listnode *temp = result->head;
    while(temp!=NULL){
        sum+=bufsize/sizeof(result);
        limit = bufsize/sizeof(result);
        if(sum > result->tupleCount){
            limit = result->tupleCount % (bufsize/sizeof(result));
        }
        for(int i=0;i<limit;i++){
            // cout << temp->tuples[i].rowId1 << " , " << temp->tuples[i].rowId2 << endl;
            midres->cols[0][counter] = temp->tuples[i].rowId1;
            midres->cols[1][counter] = temp->tuples[i].rowId2;
            counter++;
        }
        temp = temp->next;
    }
    midres->relId.push_back(r0);
    midres->relId.push_back(r1);
    midres->colSize = result->tupleCount;
    midresults->push_back(midres);
}

//execute using scan and merge the midresults objects
void both_in_diff_midresults(int r0,int c0, int r1,int c1,vector<midResult*> *midresults,relation **rels){
    // find in which midresult belongs each relation
    int midresId_r0,midresId_r1;
    for(int i=0;i<midresults->size();i++){
        for(int j=0;j<midresults->at(i)->relId.size();j++){
            if(midresults->at(i)->relId[j] == r0){
                midresId_r0 = i;
            }
            if(midresults->at(i)->relId[j] == r1){
                midresId_r1 = i;
            }
        }
    }
    cout << "midresId_r0 = " << midresId_r0 << " midresId_r1 = " << midresId_r1 << endl;
    
}

int checkfilter(SQLquery* query){
    for(int i=0;i<query->predicates.size();i++){
        if(query->predicates[i][4]==-1){
            return i;
        }
    }
    return -1;
}
int checkmidresults(int rel_index,vector<midResult*> &midresults){
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j]==rel_index)
                return 1;
        }
    }
    return 0;
}
void executefilters(SQLquery* query,relation **rels,vector<midResult*> &midresults){
    int index;
    while((index=checkfilter(query))!=-1){
        int rel_index=query->predicates[index][0];
        int col_index=query->predicates[index][1];
        if(checkmidresults(rel_index,midresults)){
            for(int i=0;i<midresults.size();i++){
                for(int j=0;j<midresults[i]->relId.size();j++){
                    if(midresults[i]->relId[j]==rel_index){
                        int* tempresults=(int*)malloc(midresults[i]->colSize*sizeof(int));
                        int counter=0;
                        if(query->predicates[index][2]==0){
                            for(int in=0;in<midresults[i]->colSize;in++){
                                if(rels[rel_index]->cols[col_index][midresults[i]->cols[j][in]]==query->predicates[index][3]){
                                    tempresults[counter]=midresults[i]->cols[j][in];
                                    counter++;
                                }
                            }
                        }
                        else if(query->predicates[index][2]==1){
                            for(int in=0;in<midresults[i]->colSize;in++){
                                if(rels[rel_index]->cols[col_index][midresults[i]->cols[j][in]]>query->predicates[index][3]){
                                    tempresults[counter]=midresults[i]->cols[j][in];
                                    counter++;
                                }
                            }
                        }
                        else{
                            for(int in=0;in<midresults[i]->colSize;in++){
                                if(rels[rel_index]->cols[col_index][midresults[i]->cols[j][in]]<query->predicates[index][3]){
                                    tempresults[counter]=midresults[i]->cols[j][in];
                                    counter++;
                                }
                            }
                        }
                        midresults[i]->colSize=counter;
                        free(midresults[i]->cols[j]);
                        midresults[i]->cols[j]=tempresults;
                    }
                }
            }
        }
        else{
            int* tempresults=(int*)malloc(rels[rel_index]->numofentries*sizeof(int));
            int counter=0;
            if(query->predicates[index][2]==0){
                for(int i=0;i<rels[rel_index]->numofentries;i++){
                    if(rels[rel_index]->cols[col_index][i]==query->predicates[index][3]){
                        tempresults[counter]=i;
                        counter++;
                    }
                }
            }
            else if(query->predicates[index][2]==1){
                for(int i=0;i<rels[rel_index]->numofentries;i++){
                    if(rels[rel_index]->cols[col_index][i]>query->predicates[index][3]){
                        tempresults[counter]=i;
                        counter++;
                    }
                }
            }
            else{
                for(int i=0;i<rels[rel_index]->numofentries;i++){
                    if(rels[rel_index]->cols[col_index][i]<query->predicates[index][3]){
                        tempresults[counter]=i;
                        counter++;
                    }
                }
            }
            midResult* tempmidResult=new midResult();
            tempmidResult->colSize=counter;
            tempmidResult->relId.push_back(rel_index);
            tempmidResult->cols.push_back(tempresults);
        }
        query->predicates.erase(query->predicates.begin()+index);
    }
}

int sortpredicates(SQLquery* query,vector<midResult*> midresults,vector<int> &scoretable){
    for(int i=0;i<query->predicates.size();i++){
        scoretable.push_back(0);
        for(int c=0;c<2;c++){
            int flag=0;
            for(int j=0;j<midresults.size();j++){
                for(int z=0;z<midresults[j]->relId.size();z++){
                    if(c==0){
                        if(query->predicates[i][0]==midresults[j]->relId[z]){
                            scoretable[i]++;
                            flag=1;
                            break;
                        }
                    }
                    else{
                        if(query->predicates[i][3]==midresults[j]->relId[z]){
                            scoretable[i]++;
                            flag=1;
                            break;
                        }
                    }
                }
                if(flag)
                    break;
            }
        }
    }
    int oneflag=0;
    for(int i=0;i<scoretable.size();i++){
        if(scoretable[i]==2)
            return i;
        else if(scoretable[i]==1)
            oneflag=i;
    }
    return oneflag;
}

int checkcases(SQLquery* query,int index,vector<int> scoretable,vector<midResult*> midresults){
    if(scoretable[index]==0)
        return 1;
    else if(scoretable[index]==1)
        return 2;
    else if(scoretable[index]==2){
        int jkeeper1=-1;
        int jkeeper2=-1;
        for(int c=0;c<2;c++){
            int flag=0;
            for(int j=0;j<midresults.size();j++){
                for(int z=0;z<midresults[j]->relId.size();z++){
                    if(c==0){
                        if(query->predicates[index][0]==midresults[j]->relId[z]){
                            jkeeper1=1;
                            flag=1;
                            break;
                        }
                    }
                    else{
                        if(query->predicates[index][3]==midresults[j]->relId[z]){
                            jkeeper2=1;
                            flag=1;
                            break;
                        }
                    }
                }
                if(flag)
                    break;
            }
        }
        if(jkeeper1==jkeeper2)
            return 3;
        else
            return 4;
    }
}

void categoriser(SQLquery* query,relation **rels){
    vector<midResult*> midresults;
    executefilters(query,rels,midresults);
    cout << "midres size = " << midresults.size() << endl;
    int numofqueries=query->predicates.size();
    vector<int> scoretable;
    for(int i=0;i<numofqueries;i++){
        int index=sortpredicates(query,midresults,scoretable);
        if(query->predicates[index][0]==query->predicates[index][3]){ //1)are at the same relation
            //execute using scan)
        }
        else{   //2)belong to different relations
            int ret=checkcases(query,index,scoretable,midresults);
            cout << "Query: " << query->predicates[index][0] << "." << query->predicates[index][1] << " " << query->predicates[index][3] << "." << query->predicates[index][4] << endl;
            cout << "ret = " << ret << endl;
            if(ret==1){//2.1)none of 2 are in mid results
                //execute using rhj and build midresult object
                none_of_two_in_midresults(query->predicates[index][0],query->predicates[index][1], query->predicates[index][3],query->predicates[index][4], &midresults, rels);
            }
            if(ret==2){//2.2)one of 2 belongs to midresults array of objects
                //execute using rhj and add the second relation column to the midresult object the other relation is
            }
            if(ret==3){//2.3)2 of 2 belong to the same midresult object
                //execute using scan and update the midresult object
            }
            if(ret==4){//2.4)2 of 2 belong to different midresult objects
                //execute using rhj and merge the midresults objects
                both_in_diff_midresults(query->predicates[index][0],query->predicates[index][1], query->predicates[index][3],query->predicates[index][4], &midresults, rels);
            }   
        }
        query->predicates.erase(query->predicates.begin()+index);
        scoretable.clear();
    }
        
}