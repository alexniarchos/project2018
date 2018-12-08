#include "functions.h"
#include "join.h"

//execute using rhj and create new midresult object
void none_of_two_in_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults){
    int r0=query->predicates[index][0];
    int c0=query->predicates[index][1];
    int r1=query->predicates[index][3];
    int c1=query->predicates[index][4];
    list *result=NULL;
    result = RadixHashJoin(rels[r0]->cols[c0],rels[r0]->numofentries,rels[r1]->cols[c1],rels[r1]->numofentries);
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
    midresults.push_back(midres);
}

//execute using scan and merge the midresults objects
void both_in_diff_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults){
    int r0=query->predicates[index][0];
    int c0=query->predicates[index][1];
    int r1=query->predicates[index][3];
    int c1=query->predicates[index][4];
    // find in which midresult belongs each relation
    int midresPos_r0,midresPos_r1,relPos_r0,relPos_r1;
    list *result=NULL;
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j] == r0){
                midresPos_r0 = i;
                relPos_r0 = j;
            }
            if(midresults[i]->relId[j] == r1){
                midresPos_r1 = i;
                relPos_r1 = j;
            }
        }
    }
    cout << "midresPos_r0 = " << midresPos_r0 << " midresPos_r1 = " << midresPos_r1 << endl;
    // create input arrays for joining using row ids
    uint64_t *temp_r0 = (uint64_t*)malloc(midresults[midresPos_r0]->colSize*sizeof(uint64_t));
    uint64_t *temp_r1 = (uint64_t*)malloc(midresults[midresPos_r1]->colSize*sizeof(uint64_t));
    for(int i=0;i<midresults[midresPos_r0]->colSize;i++){
        temp_r0[i] = rels[r0]->cols[c0][midresults[midresPos_r0]->cols[relPos_r0][i]];
        // cout << "temp_r0[" << i << "] = " << temp_r0[i] << endl;
    }
    for(int i=0;i<midresults[midresPos_r1]->colSize;i++){
        temp_r1[i] = rels[r1]->cols[c1][midresults[midresPos_r1]->cols[relPos_r1][i]];
    }
    // join
    result = RadixHashJoin(temp_r0,midresults[midresPos_r0]->colSize,temp_r1,midresults[midresPos_r1]->colSize);
    // copy result into midResult object
    midResult *midres = new midResult();
    for(int j=0;j<midresults[midresPos_r0]->cols.size()+midresults[midresPos_r1]->cols.size();j++){
        midres->cols.push_back((int*)malloc(result->tupleCount*sizeof(int)));
    }
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
            for(int j=0;j<midresults[midresPos_r0]->cols.size();j++){
                midres->cols[j][counter] = midresults[midresPos_r0]->cols[j][temp->tuples[i].rowId1];
            }
            for(int j=0;j<midresults[midresPos_r1]->cols.size();j++){
                midres->cols[midresults[midresPos_r0]->cols.size()+j][counter] = midresults[midresPos_r1]->cols[j][temp->tuples[i].rowId2];
            }
            counter++;
        }
        temp = temp->next;
    }
    // update midres colsize and rel ids
    midres->colSize = result->tupleCount;
    for(int i=0;i<midresults[midresPos_r0]->relId.size();i++){
        midres->relId.push_back(midresults[midresPos_r0]->relId[i]);
    }
    for(int i=0;i<midresults[midresPos_r1]->relId.size();i++){
        midres->relId.push_back(midresults[midresPos_r1]->relId[i]);
    }
    cout << "midres->colSize = " << midres->colSize << endl;
    for(int i=0;i<midres->relId.size();i++){
        cout << "relid = " << midres->relId[i] << endl;
    }
    // delete old mid results and add the new one
    midresults.erase(midresults.begin()+midresPos_r0);
    // find second midres pos
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j] == r1){
                midresPos_r1 = i;
            }
        }
    }
    midresults.erase(midresults.begin()+midresPos_r1);
    midresults.push_back(midres);
    ofstream output;
    output.open("output.csv");
    for(int i=0;i<midres->colSize;i++){
        for(int j=0;j<midres->cols.size();j++){
            output << midres->cols[j][i] << ",";
        }
        output << endl;
    }
    output.close();
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
            midresults.push_back(tempmidResult);
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
                            jkeeper1=j;
                            flag=1;
                            break;
                        }
                    }
                    else{
                        if(query->predicates[index][3]==midresults[j]->relId[z]){
                            jkeeper2=j;
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

void scansamerel(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults){
    int rel_index=query->predicates[index][0];
    int col1=query->predicates[index][1];
    int col2=query->predicates[index][4];
    int* tempresults=(int*)malloc(rels[rel_index]->numofentries*sizeof(int));
    int counter=0;
    for(int i=0;i<rels[rel_index]->numofentries;i++){
        if(rels[rel_index]->cols[col1]==rels[rel_index]->cols[col2])
        {
            tempresults[counter]=i;
            counter++;
        }
    }
    midResult* tempmidResult=new midResult();
    tempmidResult->colSize=counter;
    tempmidResult->relId.push_back(rel_index);
    tempmidResult->cols.push_back(tempresults);
    midresults.push_back(tempmidResult);
}

void scansamemidresults(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults){
    int rel_index=query->predicates[index][0];
    int col1=query->predicates[index][1];
    int col2=query->predicates[index][4];
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j]==rel_index){
                int* tempresults=(int*)malloc(midresults[i]->colSize*sizeof(int));
                int counter=0;
                for(int in=0;in<midresults[i]->colSize;in++){
                    if(rels[rel_index]->cols[col1][midresults[i]->cols[j][in]]==rels[rel_index]->cols[col2][midresults[i]->cols[j][in]]){
                        tempresults[counter]=midresults[i]->cols[j][in];
                        counter++;
                    }
                }
                midresults[i]->colSize=counter;
                free(midresults[i]->cols[j]);
                midresults[i]->cols[j]=tempresults;
                return;
            }
        }
    }
}

void samerelation(SQLquery* query,relation **rels,int index,vector<int> scoretable,vector<midResult*> &midresults){
    int ret=checkcases(query,index,scoretable,midresults);
    if(ret==1)
        scansamerel(query,index,rels,midresults);
    else if(ret==3)
        scansamemidresults(query,index,rels,midresults);

}

void diffrelationsamemidresult(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults){
    int rel1=query->predicates[index][0];
    int col1=query->predicates[index][1];
    int rel2=query->predicates[index][3];
    int col2=query->predicates[index][4];
    int midresultindex;
    int midresultrel1,midresultrel2;
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j]==rel1){
                midresultindex=i;
                midresultrel1=j;
            }
            if(midresults[i]->relId[j]==rel2){
                midresultrel2=j;
            }
        }
    }
    int* tempresults1=(int*)malloc(midresults[midresultindex]->colSize*sizeof(int));
    int* tempresults2=(int*)malloc(midresults[midresultindex]->colSize*sizeof(int));
    int counter=0;
    for(int i=0;i<midresults[midresultindex]->colSize;i++){
        for(int j=0;j<midresults[midresultindex]->colSize;j++){
            if(rels[rel1]->cols[col1][midresults[midresultindex]->cols[midresultrel1][i]]==rels[rel2]->cols[col2][midresults[midresultindex]->cols[midresultrel2][j]]){
                tempresults1[counter]=midresults[midresultindex]->cols[midresultrel1][i];
                tempresults2[counter]=midresults[midresultindex]->cols[midresultrel2][j];
                counter++;
            }
        } 
    }
    midresults[midresultindex]->colSize=counter;
    free(midresults[midresultindex]->cols[midresultrel1]);
    free(midresults[midresultindex]->cols[midresultrel2]);
    midresults[midresultindex]->cols[midresultrel1]=tempresults1;
    midresults[midresultindex]->cols[midresultrel2]=tempresults2;
}

void differentrelation(SQLquery* query,relation **rels,int index,vector<int> scoretable,vector<midResult*> &midresults){
    int ret=checkcases(query,index,scoretable,midresults);
    cout << "Query: " << query->predicates[index][0] << "." << query->predicates[index][1] << query->predicates[index][2] << query->predicates[index][3] << "." << query->predicates[index][4] << endl;
    cout << "ret = " << ret << endl;
    if(ret==1){//2.1)none of 2 are in mid results
        //execute using rhj and build midresult object
        none_of_two_in_midresults(query,index,rels,midresults);
    }
    else if(ret==2){//2.2)one of 2 belongs to midresults array of objects
        //execute using rhj and add the second relation column to the midresult object the other relation is
    }
    else if(ret==3)//2 of 2 belong to the same midresult object
        diffrelationsamemidresult(query,index,rels,midresults);
    else if(ret==4){//2.4)2 of 2 belong to different midresult objects
        //execute using scan and merge the midresults objects
        both_in_diff_midresults(query,index,rels,midresults);
        
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
        if(query->predicates[index][0]==query->predicates[index][3]) //1)are at the same relation
            samerelation(query,rels,index,scoretable,midresults);  
        else   //2)belong to different relations
            differentrelation(query,rels,index,scoretable,midresults);
        query->predicates.erase(query->predicates.begin()+index);
        scoretable.clear();
    }
        
}