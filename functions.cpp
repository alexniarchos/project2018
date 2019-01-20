#include "functions.h"
#include <fstream>
#include <string>
#include <time.h>
#include <cmath>

void generateResults(SQLquery* query,relation** rels,vector<midResult*> &midresults,vector<string*> &results){
    // cout << "Generating results..." << endl;
    int rel,col,count=0;
    uint64_t sum=0;
    string* output=new string();
    // ofstream outfile; 
    // outfile.open("output",std::ofstream::out | std::ofstream::app);
    for(int i=0;i<query->views.size();i++){
        rel = query->views[i][0];
        col = query->views[i][1];
        // cout << rel << "." << col << endl;
        // cout << "midresults colsize = " << midresults[0]->colSize << endl;
        for(int j=0;j<midresults[0]->cols.size();j++){
            if(rel == midresults[0]->relId[j]){
                // cout << "position of rel in midresult = " << j << endl;
                sum=0;
                count=0;
                for(int k=0;k<midresults[0]->colSize;k++){
                    // cout << rels[rel]->cols[col][midresults[0]->cols[j][k]] << endl;
                    sum += rels[query->relations[rel]]->cols[col][midresults[0]->cols[j][k]];
                    count ++;
                }
                // cout << "sum = " << sum << " count = " << count << endl;
                if(sum == 0){
                    output->append("NULL ");
                }
                else{
                    char* csum=(char*)malloc(32*sizeof(char));
                    sprintf(csum,"%lu ",sum);
                    output->append(csum);
                    free(csum);
                }
                // cout << "--------------------------------------------" << endl;
            }
        }
    }
    output->erase(output->end()-1);
    output->append("\n");
    results.push_back(output);
    // outfile << output->c_str();
}

//execute using rhj and create new midresult object
void none_of_two_in_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults){
    int r0=query->predicates[index][0];
    int c0=query->predicates[index][1];
    int r1=query->predicates[index][3];
    int c1=query->predicates[index][4];

    int rels_index0 = query->relations[r0];
    int rels_index1 = query->relations[r1];
    list *result=NULL;
    result = RadixHashJoin(rels[rels_index0]->cols[c0],rels[rels_index0]->numofentries,rels[rels_index1]->cols[c1],rels[rels_index1]->numofentries);
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
    delete result;
}

//execute using RHJ and merge the midresults objects
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
    // cout << "midresPos_r0 = " << midresPos_r0 << " midresPos_r1 = " << midresPos_r1 << endl;
    // create input arrays for joining using row ids
    uint64_t *temp_r0 = (uint64_t*)malloc(midresults[midresPos_r0]->colSize*sizeof(uint64_t));
    uint64_t *temp_r1 = (uint64_t*)malloc(midresults[midresPos_r1]->colSize*sizeof(uint64_t));
    for(int i=0;i<midresults[midresPos_r0]->colSize;i++){
        temp_r0[i] = rels[query->relations[r0]]->cols[c0][midresults[midresPos_r0]->cols[relPos_r0][i]];
        // cout << "temp_r0[" << i << "] = " << temp_r0[i] << endl;
    }
    for(int i=0;i<midresults[midresPos_r1]->colSize;i++){
        temp_r1[i] = rels[query->relations[r1]]->cols[c1][midresults[midresPos_r1]->cols[relPos_r1][i]];
    }
    // join
    result = RadixHashJoin(temp_r0,midresults[midresPos_r0]->colSize,temp_r1,midresults[midresPos_r1]->colSize);
    // free temp arrays
    free(temp_r0);
    free(temp_r1);
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
    // free result
    delete result;
    for(int i=0;i<midresults[midresPos_r0]->relId.size();i++){
        midres->relId.push_back(midresults[midresPos_r0]->relId[i]);
    }
    for(int i=0;i<midresults[midresPos_r1]->relId.size();i++){
        midres->relId.push_back(midresults[midresPos_r1]->relId[i]);
    }
    // cout << "midres->colSize = " << midres->colSize << endl;
    // for(int i=0;i<midres->relId.size();i++){
    //     cout << "relid = " << midres->relId[i] << endl;
    // }
    // delete old mid results and add the new one
    for(int i=0;i<midresults[midresPos_r0]->cols.size();i++)
        free(midresults[midresPos_r0]->cols[i]);
    delete midresults[midresPos_r0];
    midresults.erase(midresults.begin()+midresPos_r0);
    // find second midres pos
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j] == r1){
                midresPos_r1 = i;
            }
        }
    }
    for(int i=0;i<midresults[midresPos_r1]->cols.size();i++)
        free(midresults[midresPos_r1]->cols[i]);
    delete midresults[midresPos_r1];
    midresults.erase(midresults.begin()+midresPos_r1);
    midresults.push_back(midres);
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

int sortfilters(SQLquery* query,relation **rels,int numofrels){
    //apomonwse filters
    for(int i=0;i<numofrels;i++){
        for(int j=0;j<rels[i]->numofcols;j++){
            rels[i]->tempcolStats[j]->l=rels[i]->colStats[j]->l;
            rels[i]->tempcolStats[j]->u=rels[i]->colStats[j]->u;
            rels[i]->tempcolStats[j]->f=rels[i]->colStats[j]->f;
            rels[i]->tempcolStats[j]->d=rels[i]->colStats[j]->d;
        }
    }
    for(int i=0;i<query->predicates.size();i++){
        if(query->predicates[i][4]==-1){
            int rel_index=query->predicates[i][0];
            int rels_index=query->relations[rel_index];
            int col=query->predicates[i][1];
            int k1=query->predicates[i][3];
            if(query->predicates[i][2]==0){
                rels[rels_index]->tempcolStats[col]->l=k1;
                rels[rels_index]->tempcolStats[col]->u=k1;
                int fA=rels[rels_index]->tempcolStats[col]->f;
                rels[rels_index]->tempcolStats[col]->f=rels[rels_index]->tempcolStats[col]->f/rels[rels_index]->tempcolStats[col]->d;
                rels[rels_index]->tempcolStats[col]->d=1;
                for(int j=0;j<rels[rels_index]->numofcols;j++){
                    if(j==col)
                        continue;
                    rels[rels_index]->tempcolStats[j]->d=rels[rels_index]->tempcolStats[j]->d*(1.0-pow((1.0-double(rels[rels_index]->tempcolStats[col]->f)/double(fA)),double(rels[rels_index]->tempcolStats[j]->f)/double(rels[rels_index]->tempcolStats[j]->d)));
                    rels[rels_index]->tempcolStats[j]->f=rels[rels_index]->tempcolStats[col]->f;
                }
            }
            if(query->predicates[i][2]==1){
                int rel_index=query->predicates[i][0];
                int rels_index=query->relations[rel_index];
                int col=query->predicates[i][1];
                int k1=query->predicates[i][3];
                if(k1<=rels[rels_index]->tempcolStats[col]->l){
                    free(query->predicates[i]);
                    query->predicates.erase(query->predicates.begin()+i);
                    continue;
                }
                int fA=rels[rels_index]->tempcolStats[col]->f;
                rels[rels_index]->tempcolStats[col]->f=(double(rels[rels_index]->tempcolStats[col]->u)-double(k1)/double(rels[rels_index]->tempcolStats[col]->u)-double(rels[rels_index]->tempcolStats[col]->l)*(double)fA);
                rels[rels_index]->tempcolStats[col]->d=(double(rels[rels_index]->tempcolStats[col]->u)-double(k1)/double(rels[rels_index]->tempcolStats[col]->u)-double(rels[rels_index]->tempcolStats[col]->l)*(double)rels[rels_index]->tempcolStats[col]->d);
                rels[rels_index]->tempcolStats[col]->l=k1;
                for(int j=0;j<rels[rels_index]->numofcols;j++){
                    if(j==col)
                        continue;
                    rels[rels_index]->tempcolStats[j]->d=rels[rels_index]->tempcolStats[j]->d*(1.0-pow((1.0-double(rels[rels_index]->tempcolStats[col]->f)/double(fA)),double(rels[rels_index]->tempcolStats[j]->f)/double(rels[rels_index]->tempcolStats[j]->d)));
                    rels[rels_index]->tempcolStats[j]->f=rels[rels_index]->tempcolStats[col]->f;
                }
            }
            if(query->predicates[i][2]==2){
                int rel_index=query->predicates[i][0];
                int rels_index=query->relations[rel_index];
                int col=query->predicates[i][1];
                int k2=query->predicates[i][3];
                if(k2>=rels[rels_index]->tempcolStats[col]->u){
                    free(query->predicates[i]);
                    query->predicates.erase(query->predicates.begin()+i);
                    continue;
                }
                int fA=rels[rels_index]->tempcolStats[col]->f;
                rels[rels_index]->tempcolStats[col]->f=(double(k2)-double(rels[rels_index]->tempcolStats[col]->u)/double(rels[rels_index]->tempcolStats[col]->u)-double(rels[rels_index]->tempcolStats[col]->l)*(double)fA);
                rels[rels_index]->tempcolStats[col]->d=(double(k2)-double(rels[rels_index]->tempcolStats[col]->u)/double(rels[rels_index]->tempcolStats[col]->u)-double(rels[rels_index]->tempcolStats[col]->l)*(double)rels[rels_index]->tempcolStats[col]->d);
                rels[rels_index]->tempcolStats[col]->u=k2;
                for(int j=0;j<rels[rels_index]->numofcols;j++){
                    if(j==col)
                        continue;
                    rels[rels_index]->tempcolStats[j]->d=rels[rels_index]->tempcolStats[j]->d*(1.0-pow((1.0-double(rels[rels_index]->tempcolStats[col]->f)/double(fA)),double(rels[rels_index]->tempcolStats[j]->f)/double(rels[rels_index]->tempcolStats[j]->d)));
                    rels[rels_index]->tempcolStats[j]->f=rels[rels_index]->tempcolStats[col]->f;
                }
            }
        }
    }
    
    //calculate
    
}

void executefilters(SQLquery* query,relation **rels,vector<midResult*> &midresults,int numofrels){
    int index;
    sortfilters(query,rels,numofrels);
    while((index=checkfilter(query))!=-1){
        int rel_index=query->predicates[index][0];
        int rels_index = query->relations[rel_index];
        int col_index=query->predicates[index][1];
        if(checkmidresults(rel_index,midresults)){
            for(int i=0;i<midresults.size();i++){
                for(int j=0;j<midresults[i]->relId.size();j++){
                    if(midresults[i]->relId[j]==rel_index){
                        int* tempresults=(int*)malloc(midresults[i]->colSize*sizeof(int));
                        int counter=0;
                        if(query->predicates[index][2]==0){
                            for(int in=0;in<midresults[i]->colSize;in++){
                                if(rels[rels_index]->cols[col_index][midresults[i]->cols[j][in]]==query->predicates[index][3]){
                                    tempresults[counter]=midresults[i]->cols[j][in];
                                    counter++;
                                }
                            }
                        }
                        else if(query->predicates[index][2]==1){
                            for(int in=0;in<midresults[i]->colSize;in++){
                                if(rels[rels_index]->cols[col_index][midresults[i]->cols[j][in]]>query->predicates[index][3]){
                                    tempresults[counter]=midresults[i]->cols[j][in];
                                    counter++;
                                }
                            }
                        }
                        else{
                            for(int in=0;in<midresults[i]->colSize;in++){
                                if(rels[rels_index]->cols[col_index][midresults[i]->cols[j][in]]<query->predicates[index][3]){
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
            int* tempresults=(int*)malloc(rels[rels_index]->numofentries*sizeof(int));
            int counter=0;
            if(query->predicates[index][2]==0){
                for(int i=0;i<rels[rels_index]->numofentries;i++){
                    if(rels[rels_index]->cols[col_index][i]==query->predicates[index][3]){
                        tempresults[counter]=i;
                        counter++;
                    }
                }
            }
            else if(query->predicates[index][2]==1){
                for(int i=0;i<rels[rels_index]->numofentries;i++){
                    if(rels[rels_index]->cols[col_index][i]>query->predicates[index][3]){
                        tempresults[counter]=i;
                        counter++;
                    }
                }
            }
            else{
                for(int i=0;i<rels[rels_index]->numofentries;i++){
                    if(rels[rels_index]->cols[col_index][i]<query->predicates[index][3]){
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
        free(query->predicates[index]);
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

Case diffrelCases(SQLquery* query,int index,vector<midResult*> midresults){
    int firstPos=-1;
    int secondPos=-1;
    bool foundFirstRel=false,foundSecondRel=false,flag=false;
    for(int c=0;c<2;c++){
        flag = false;
        for(int j=0;j<midresults.size();j++){
            for(int z=0;z<midresults[j]->relId.size();z++){
                // first relation
                if(c==0){
                    if(query->predicates[index][0]==midresults[j]->relId[z]){
                        firstPos = j;
                        foundFirstRel = true;
                        flag = true;
                        break;
                    }
                }
                // second relation
                else{
                    if(query->predicates[index][3]==midresults[j]->relId[z]){
                        secondPos = j;
                        foundSecondRel = true;
                        flag = true;
                        break;
                    }
                }
            }
            if(flag)
                break;
        }
    }
    // found both in midresults
    if(foundFirstRel && foundSecondRel){
        if(firstPos == secondPos){
            return BothInSameMidResult;
        }
        return BothInDiffMidResult;
    }
    // found only one
    else if(foundFirstRel || foundSecondRel){
        return OneInMidResult;
    }
    // found none
    else{
        return NoneInMidResults;
    }
}

Case samerelCases(SQLquery* query,int index,vector<midResult*> &midresults){
    // check if relation exists inside midresults
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j < midresults[i]->relId.size();j++){
            if(query->predicates[index][0] == midresults[i]->relId[j]){
                return InMidResult;
            }
        }
    }
    return NotInMidResult;
}

void scansamerel(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults){
    int rel_index=query->predicates[index][0];
    int col1=query->predicates[index][1];
    int col2=query->predicates[index][4];
    int* tempresults=(int*)malloc(rels[query->relations[rel_index]]->numofentries*sizeof(int));
    int counter=0;
    for(int i=0;i<rels[query->relations[rel_index]]->numofentries;i++){
        if(rels[query->relations[rel_index]]->cols[col1][i]==rels[query->relations[rel_index]]->cols[col2][i])
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
                int** tempresults=(int**)malloc(midresults[i]->cols.size()*sizeof(int*));
                for(int g=0;g<midresults[i]->cols.size();g++)
                    tempresults[g]=(int*)malloc(midresults[i]->colSize*sizeof(int));
                int counter=0;
                for(int in=0;in<midresults[i]->colSize;in++){
                    if(rels[query->relations[rel_index]]->cols[col1][midresults[i]->cols[j][in]]==rels[query->relations[rel_index]]->cols[col2][midresults[i]->cols[j][in]]){
                        for(int g=0;g<midresults[i]->cols.size();g++)
                            tempresults[g][counter]=midresults[i]->cols[g][in];
                        counter++;
                    }
                }
                midresults[i]->colSize=counter;
                for(int g=0;g<midresults[i]->cols.size();g++){
                    free(midresults[i]->cols[g]);
                    midresults[i]->cols[g]=tempresults[g];
                }
                free(tempresults);
                return;
            }
        }
    }
}

void samerelation(SQLquery* query,relation **rels,int index,vector<midResult*> &midresults){
    Case ret=samerelCases(query,index,midresults);
    // cout << "Query: " << query->predicates[index][0] << "." << query->predicates[index][1] << " = " << query->predicates[index][3] << "." << query->predicates[index][4] << endl;
    // cout << "same ret = " << ret << endl;
    if(ret == NotInMidResult)
        scansamerel(query,index,rels,midresults);
    else if(ret == InMidResult)
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
    int counter=0;
    int** tempresults=(int**)malloc(midresults[midresultindex]->cols.size()*sizeof(int*));
        for(int g=0;g<midresults[midresultindex]->cols.size();g++)
            tempresults[g]=(int*)malloc(midresults[midresultindex]->colSize*sizeof(int));
    for(int i=0;i<midresults[midresultindex]->colSize;i++){
        if(rels[query->relations[rel1]]->cols[col1][midresults[midresultindex]->cols[midresultrel1][i]]==rels[query->relations[rel2]]->cols[col2][midresults[midresultindex]->cols[midresultrel2][i]]){
            for(int g=0;g<midresults[midresultindex]->cols.size();g++)
                tempresults[g][counter]=midresults[midresultindex]->cols[g][i];
            counter++;
        }
    }
    midresults[midresultindex]->colSize=counter;
    for(int g=0;g<midresults[midresultindex]->cols.size();g++){
        free(midresults[midresultindex]->cols[g]);
        midresults[midresultindex]->cols[g]=tempresults[g];
    }
    free(tempresults);
}

void diffrelationoneonmidresult(SQLquery* query,int index,relation **rels,vector<midResult*> &midresults){
    int rel1=query->predicates[index][0];
    int col1=query->predicates[index][1];
    int rel2=query->predicates[index][3];
    int col2=query->predicates[index][4];
    int midresultindex,relinmidresult,colinmidresult,relnotinmidresult,colnotinmidresult,midresultrel;
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->relId.size();j++){
            if(midresults[i]->relId[j]==rel1){
                midresultindex=i;
                midresultrel=j;
                relinmidresult=rel1;
                colinmidresult=col1;
                relnotinmidresult=rel2;
                colnotinmidresult=col2;
            }
            if(midresults[i]->relId[j]==rel2){
                midresultindex=i;
                midresultrel=j;
                relinmidresult=rel2;
                colinmidresult=col2;
                relnotinmidresult=rel1;
                colnotinmidresult=col1;
            }
        }
    }
    uint64_t* rhjinput=(uint64_t*)malloc(midresults[midresultindex]->colSize*sizeof(uint64_t));
    for(int i=0;i<midresults[midresultindex]->colSize;i++)
        rhjinput[i]=rels[query->relations[relinmidresult]]->cols[colinmidresult][midresults[midresultindex]->cols[midresultrel][i]];
    list *result=NULL;
    time_t start,end;
    time(&start);
    result=RadixHashJoin(rhjinput,midresults[midresultindex]->colSize,rels[query->relations[relnotinmidresult]]->cols[colnotinmidresult],rels[query->relations[relnotinmidresult]]->numofentries);
    time(&end);
    // cout << "time spent by rhj: " << end-start << endl;
    free(rhjinput);
    int** tempresults=(int**)malloc((midresults[midresultindex]->cols.size()+1)*sizeof(int*));
    for(int g=0;g<(midresults[midresultindex]->cols.size()+1);g++)
        tempresults[g]=(int*)malloc(result->tupleCount*sizeof(int));
    int counter=0;
    listnode *temp = result->head;
    int sum=0,limit;
    while(temp!=NULL){
        sum+=bufsize/sizeof(result);
        limit = bufsize/sizeof(result);
        if(sum > result->tupleCount){
            limit = result->tupleCount % (bufsize/sizeof(result));
        }
        for(int i=0;i<limit;i++){
            for(int g=0;g<midresults[midresultindex]->cols.size();g++)
                tempresults[g][counter]=midresults[midresultindex]->cols[g][temp->tuples[i].rowId1];
            tempresults[midresults[midresultindex]->cols.size()][counter]=temp->tuples[i].rowId2;
            counter++;
        }
        temp = temp->next;
    }
    midresults[midresultindex]->colSize=counter;
    for(int g=0;g<midresults[midresultindex]->cols.size();g++){
        free(midresults[midresultindex]->cols[g]);
        midresults[midresultindex]->cols[g]=tempresults[g];
    }
    int size=midresults[midresultindex]->cols.size();
    midresults[midresultindex]->cols.push_back(tempresults[size]);
    midresults[midresultindex]->relId.push_back(relnotinmidresult);
    free(tempresults);
    delete result;
}

void differentrelation(SQLquery* query,relation **rels,int index,vector<midResult*> &midresults){
    Case ret=diffrelCases(query,index,midresults);
    // cout << "Query: " << query->predicates[index][0] << "." << query->predicates[index][1] << " = " << query->predicates[index][3] << "." << query->predicates[index][4] << endl;
    // cout << "ret = " << ret << endl;
    // cout << "index = " << index << endl;
    if(ret == NoneInMidResults){//2.1)none of 2 are in mid results
        //execute using rhj and build midresult object
        none_of_two_in_midresults(query,index,rels,midresults);
    }
    else if(ret == OneInMidResult)//2.2)one of 2 belongs to midresults array of objects //execute using rhj and add the second relation column to the midresult object the other relation is
        diffrelationoneonmidresult(query,index,rels,midresults);
    else if(ret == BothInSameMidResult)//2 of 2 belong to the same midresult object scan
        diffrelationsamemidresult(query,index,rels,midresults);
    else if(ret == BothInDiffMidResult){//2.4)2 of 2 belong to different midresult objects
        //execute using scan and merge the midresults objects
        both_in_diff_midresults(query,index,rels,midresults);
    }
}

vector<int>* queryOptimiser(SQLquery* query){
}

int determinetype(SQLquery* query,Statistics* statistic,int* predicate){
    int relindex1=predicate[0];
    int rels_index1=query->relations[relindex1];
    int relindex2=predicate[3];
    int rels_index2=query->relations[relindex2];
    if(rels_index1!=rels_index2){
        int flag1=0;
        int flag2=0;
        for(int i=0;i<statistic->relations.size();i++){
            if(predicate[0]==statistic->relations[i]->rel_index)
                flag1=1;
            if(predicate[3]==statistic->relations[i]->rel_index)
                flag2=1;
           
        }
        if(flag1==1 && flag2==1){
            return 0;
        }
        return 1;
    }
    else if(rels_index1==rels_index2){
        if(predicate[1]==predicate[4])
            return 2;
        else if(predicate[1]!=predicate[4])
            return 0;
    }
}

void calculate_filter(SQLquery* query,Statistics* statistic,int *predicate,relation** rels){
    int index1=-1;
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[0]){
            index1=i;
            break;
        }
    }
    if(index1==-1){
        index1=statistic->relations.size();
        int relindex=predicate[0];
        int rels_index=query->relations[relindex];
        statisticRelation* statisticRel=new statisticRelation();
        statisticRel->numofcols=rels[rels_index]->numofcols;
        statisticRel->colStats=(ColStats**)malloc(statisticRel->numofcols*sizeof(ColStats*));
        for(int i=0;i<statisticRel->numofcols;i++){
            statisticRel->colStats[i]=(ColStats*)malloc(sizeof(ColStats));
            memcpy(statisticRel->colStats[i],rels[rels_index]->colStats[i],sizeof(ColStats));
        }
        statisticRel->rel_index=predicate[0];
        statistic->relations.push_back(statisticRel);
    }
    int index2=-1;
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[3]){
            index2=i;
            break;
        }
    }
    if(index2==-1){
        index2=statistic->relations.size();
        int relindex=predicate[3];
        int rels_index=query->relations[relindex];
        statisticRelation* statisticRel=new statisticRelation();
        statisticRel->numofcols=rels[rels_index]->numofcols;
        statisticRel->colStats=(ColStats**)malloc(statisticRel->numofcols*sizeof(ColStats*));
        for(int i=0;i<statisticRel->numofcols;i++){
            statisticRel->colStats[i]=(ColStats*)malloc(sizeof(ColStats));
            memcpy(statisticRel->colStats[i],rels[rels_index]->colStats[i],sizeof(ColStats));
        }
        statisticRel->rel_index=predicate[3];
        statistic->relations.push_back(statisticRel);
    }
    int fA=statistic->relations[index1]->colStats[predicate[1]]->f;
    int min;
    if(statistic->relations[index1]->colStats[predicate[1]]->l>statistic->relations[index2]->colStats[predicate[4]]->l)
        min=statistic->relations[index1]->colStats[predicate[1]]->l;
    else
        min=statistic->relations[index2]->colStats[predicate[4]]->l;
    statistic->relations[index2]->colStats[predicate[4]]->l=statistic->relations[index1]->colStats[predicate[1]]->l=min;
    
    int max;
    if(statistic->relations[index1]->colStats[predicate[1]]->u<statistic->relations[index2]->colStats[predicate[4]]->u)
        max=statistic->relations[index1]->colStats[predicate[1]]->u;
    else
        max=statistic->relations[index2]->colStats[predicate[4]]->u;
    statistic->relations[index2]->colStats[predicate[4]]->u=statistic->relations[index1]->colStats[predicate[1]]->u=max;
    int n=max-min+1;
    statistic->relations[index2]->colStats[predicate[4]]->f=statistic->relations[index1]->colStats[predicate[1]]->f=statistic->relations[index1]->colStats[predicate[1]]->f/n;
    statistic->relations[index2]->colStats[predicate[4]]->d=statistic->relations[index1]->colStats[predicate[1]]->d=statistic->relations[index1]->colStats[predicate[1]]->d*(1.0-(double)pow((1.0-(double)statistic->relations[index1]->colStats[predicate[1]]->f/fA),(double)fA/(double)statistic->relations[index1]->colStats[predicate[1]]->d));

    for(int i=0;i<statistic->relations.size();i++){
        for(int j=0;j<statistic->relations[i]->numofcols;j++){
            if((index1==i && j==predicate[1]) || (index2==i && j==predicate[1]))
                continue;
            statistic->relations[i]->colStats[j]->d=statistic->relations[i]->colStats[j]->d*(1.0-(double)pow((1.0-(double)statistic->relations[index1]->colStats[predicate[1]]->f/fA),(double)statistic->relations[i]->colStats[j]->f/(double)statistic->relations[i]->colStats[j]->d));
            statistic->relations[i]->colStats[j]->f=statistic->relations[index1]->colStats[predicate[1]]->f;
        }
    }
    statistic->score+=statistic->relations[index1]->colStats[predicate[1]]->f;
    statistic->predicates.push_back(predicate);
}

void calculate_prejoin_filter(Statistics* statistic,int index1,int index2,int who_belongs,int who_not_belongs,int* predicate){
    int min;
    if(statistic->relations[index1]->colStats[predicate[1]]->l >statistic->relations[index2]->colStats[predicate[4]]->l)
        min=statistic->relations[index1]->colStats[predicate[1]]->l;
    else
        min=statistic->relations[index2]->colStats[predicate[4]]->l;

    int max;
    if(statistic->relations[index1]->colStats[predicate[1]]->u < statistic->relations[index2]->colStats[predicate[4]]->u)
        max=statistic->relations[index1]->colStats[predicate[1]]->u;
    else
        max=statistic->relations[index2]->colStats[predicate[4]]->u;
    int fA_belonged,fA_not_belonged,belong_index,not_belong_index;
    if(who_belongs==index1){
        fA_belonged=statistic->relations[index1]->colStats[predicate[1]]->f;
        belong_index=predicate[1];
        fA_not_belonged=statistic->relations[index2]->colStats[predicate[4]]->f;
        not_belong_index=predicate[4];
    }
    else{
        fA_not_belonged=statistic->relations[index1]->colStats[predicate[1]]->f;
        not_belong_index=predicate[1];
        fA_belonged=statistic->relations[index2]->colStats[predicate[4]]->f;
        belong_index=predicate[4];
    }
    statistic->relations[index1]->colStats[predicate[1]]->d=double(max-min)/double(statistic->relations[index1]->colStats[predicate[1]]->u-statistic->relations[index1]->colStats[predicate[1]]->l)*statistic->relations[index1]->colStats[predicate[1]]->d;
    statistic->relations[index1]->colStats[predicate[1]]->f=double(max-min)/double(statistic->relations[index1]->colStats[predicate[1]]->u-statistic->relations[index1]->colStats[predicate[1]]->l)*statistic->relations[index1]->colStats[predicate[1]]->f;
    statistic->relations[index2]->colStats[predicate[4]]->d=double(max-min)/double(statistic->relations[index2]->colStats[predicate[4]]->u-statistic->relations[index2]->colStats[predicate[4]]->l)*statistic->relations[index2]->colStats[predicate[4]]->d;
    statistic->relations[index2]->colStats[predicate[4]]->f=double(max-min)/double(statistic->relations[index2]->colStats[predicate[4]]->u-statistic->relations[index2]->colStats[predicate[4]]->l)*statistic->relations[index2]->colStats[predicate[4]]->f;
    statistic->relations[index1]->colStats[predicate[1]]->u=statistic->relations[index2]->colStats[predicate[4]]->u=max;
    statistic->relations[index1]->colStats[predicate[1]]->l=statistic->relations[index2]->colStats[predicate[4]]->l=min;
    int new_fA_belonged,new_fA_not_belonged;
    if(who_belongs==index1){
        new_fA_belonged=statistic->relations[index1]->colStats[predicate[1]]->f;
        new_fA_not_belonged=statistic->relations[index2]->colStats[predicate[4]]->f;
    }
    else{
        new_fA_not_belonged=statistic->relations[index1]->colStats[predicate[1]]->f;
        new_fA_belonged=statistic->relations[index2]->colStats[predicate[4]]->f;
    }

    for(int i=0;i<statistic->relations.size();i++){
        if(i==who_not_belongs){
            for(int j=0;j<statistic->relations[i]->numofcols;j++){
                if(j==not_belong_index)
                    continue;
                statistic->relations[i]->colStats[j]->d=statistic->relations[i]->colStats[j]->d*(1.0-pow((1.0-(double)new_fA_not_belonged/(double)fA_not_belonged),((double)statistic->relations[i]->colStats[j]->f/(double)statistic->relations[i]->colStats[j]->d)));
                statistic->relations[i]->colStats[j]->f=statistic->relations[who_not_belongs]->colStats[not_belong_index]->f;
            }
            continue;
        }
        for(int j=0;j<statistic->relations[i]->numofcols;j++){
            if(j==belong_index)
                continue;
            statistic->relations[i]->colStats[j]->d=statistic->relations[i]->colStats[j]->d*(1.0-pow((1.0-(double)new_fA_belonged/(double)fA_belonged),((double)statistic->relations[i]->colStats[j]->f/(double)statistic->relations[i]->colStats[j]->d)));
            statistic->relations[i]->colStats[j]->f=statistic->relations[who_belongs]->colStats[belong_index]->f;
        }
    }

}

void calculate_join(SQLquery* query,Statistics* statistic,int *predicate,relation** rels){
    int index1=-1;
    int who_belongs=-1;
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[0]){
            index1=i;
            who_belongs=index1;
            break;
        }
    }
    if(index1==-1){
        index1=statistic->relations.size();
        int relindex=predicate[0];
        int rels_index=query->relations[relindex];
        statisticRelation* statisticRel=new statisticRelation();
        statisticRel->numofcols=rels[rels_index]->numofcols;
        statisticRel->colStats=(ColStats**)malloc(statisticRel->numofcols*sizeof(ColStats*));
        for(int i=0;i<statisticRel->numofcols;i++){
            statisticRel->colStats[i]=(ColStats*)malloc(sizeof(ColStats));
            memcpy(statisticRel->colStats[i],rels[rels_index]->colStats[i],sizeof(ColStats));
        }
        statisticRel->rel_index=predicate[0];
        statistic->relations.push_back(statisticRel);
    }
    int index2=-1;
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[3]){
            index2=i;
            who_belongs=index2;
            break;
        }
    }
    if(index2==-1){
        index2=statistic->relations.size();
        int relindex=predicate[3];
        int rels_index=query->relations[relindex];
        statisticRelation* statisticRel=new statisticRelation();
        statisticRel->numofcols=rels[rels_index]->numofcols;
        statisticRel->colStats=(ColStats**)malloc(statisticRel->numofcols*sizeof(ColStats*));
        for(int i=0;i<statisticRel->numofcols;i++){
            statisticRel->colStats[i]=(ColStats*)malloc(sizeof(ColStats));
            memcpy(statisticRel->colStats[i],rels[rels_index]->colStats[i],sizeof(ColStats));
        }
        statisticRel->rel_index=predicate[3];
        statistic->relations.push_back(statisticRel);
    }
    int who_not_belongs;
    if(who_belongs==-1)
        who_belongs=index1;
    if(who_belongs==index1)
        who_not_belongs=index2;
    else
        who_not_belongs=index1;
    calculate_prejoin_filter(statistic,index1,index2,who_belongs,who_not_belongs,predicate);
    int n=statistic->relations[index1]->colStats[predicate[1]]->u-=statistic->relations[index1]->colStats[predicate[1]]->l+1;
    statistic->relations[index1]->colStats[predicate[1]]->f=statistic->relations[index2]->colStats[predicate[4]]->f=statistic->relations[index1]->colStats[predicate[1]]->f*statistic->relations[index2]->colStats[predicate[4]]->f/n;
     int dbelong,dnotbelong,belong_index,not_belong_index;
    if(who_belongs==index1){
        dbelong= statistic->relations[index1]->colStats[predicate[1]]->d;
        dnotbelong= statistic->relations[index2]->colStats[predicate[4]]->d;
        belong_index=predicate[1];
        not_belong_index=predicate[4];
    } 
    else{
        dnotbelong= statistic->relations[index1]->colStats[predicate[1]]->d;
        dbelong= statistic->relations[index2]->colStats[predicate[4]]->d;
        belong_index=predicate[4];
        not_belong_index=predicate[1];
    }
    statistic->relations[index1]->colStats[predicate[1]]->d=statistic->relations[index2]->colStats[predicate[4]]->d=statistic->relations[index1]->colStats[predicate[1]]->d*statistic->relations[index2]->colStats[predicate[4]]->d/n;
    for(int i=0;i<statistic->relations.size();i++){
        if(i==who_not_belongs){
            for(int j=0;j<statistic->relations[i]->numofcols;j++){
                if(j==not_belong_index)
                    continue;
                statistic->relations[i]->colStats[j]->d=statistic->relations[i]->colStats[j]->d*(1.0-pow(1.0-(double)statistic->relations[who_not_belongs]->colStats[not_belong_index]->d /(double)dnotbelong,(double) statistic->relations[i]->colStats[j]->f/(double) statistic->relations[i]->colStats[j]->d));
                statistic->relations[i]->colStats[j]->f=statistic->relations[who_belongs]->colStats[belong_index]->f;
            }
            continue;
        }
        for(int j=0;j<statistic->relations[i]->numofcols;j++){
                if(j==belong_index)
                    continue;
                statistic->relations[i]->colStats[j]->d=statistic->relations[i]->colStats[j]->d*(1.0-pow(1.0-(double)statistic->relations[who_belongs]->colStats[belong_index]->d /(double)dbelong,(double) statistic->relations[i]->colStats[j]->f/(double) statistic->relations[i]->colStats[j]->d));
                statistic->relations[i]->colStats[j]->f=statistic->relations[who_belongs]->colStats[belong_index]->f;
            }
    }
    statistic->score+=statistic->relations[index1]->colStats[predicate[1]]->f;
    statistic->predicates.push_back(predicate);
}

void calculate_autocorrelation(SQLquery* query,Statistics* statistic,int *predicate,relation** rels){
    int index1=-1;
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[0]){
            index1=i;
            break;
        }
    }
    if(index1==-1){
        index1=statistic->relations.size();
        int relindex=predicate[0];
        int rels_index=query->relations[relindex];
        statisticRelation* statisticRel=new statisticRelation();
        statisticRel->numofcols=rels[rels_index]->numofcols;
        statisticRel->colStats=(ColStats**)malloc(statisticRel->numofcols*sizeof(ColStats*));
        for(int i=0;i<statisticRel->numofcols;i++){
            statisticRel->colStats[i]=(ColStats*)malloc(sizeof(ColStats));
            memcpy(statisticRel->colStats[i],rels[rels_index]->colStats[i],sizeof(ColStats));
        }
        statisticRel->rel_index=predicate[0];
        statistic->relations.push_back(statisticRel);
    }
    int index2=-1;
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[3]){
            index2=i;
            break;
        }
    }
    if(index2==-1){
        index2=statistic->relations.size();
        int relindex=predicate[3];
        int rels_index=query->relations[relindex];
        statisticRelation* statisticRel=new statisticRelation();
        statisticRel->numofcols=rels[rels_index]->numofcols;
        statisticRel->colStats=(ColStats**)malloc(statisticRel->numofcols*sizeof(ColStats*));
        for(int i=0;i<statisticRel->numofcols;i++){
            statisticRel->colStats[i]=(ColStats*)malloc(sizeof(ColStats));
            memcpy(statisticRel->colStats[i],rels[rels_index]->colStats[i],sizeof(ColStats));
        }
        statisticRel->rel_index=predicate[3];
        statistic->relations.push_back(statisticRel);
    }

    int fA=statistic->relations[index1]->colStats[predicate[1]]->f;
    int n=statistic->relations[index1]->colStats[predicate[1]]->u-statistic->relations[index1]->colStats[predicate[1]]->l+1;
    statistic->relations[index1]->colStats[predicate[1]]->f=statistic->relations[index2]->colStats[predicate[4]]->f=statistic->relations[index2]->colStats[predicate[4]]->f*statistic->relations[index2]->colStats[predicate[4]]->f/n;
    for(int i=0;i<statistic->relations.size();i++){
        for(int j=0;j<statistic->relations[i]->numofcols;j++){
            if((index1==i && j==predicate[1]) || (index2==i && j==predicate[4]))
                continue;
            statistic->relations[i]->colStats[j]->f=statistic->relations[index1]->colStats[predicate[1]]->f;
        }
    }
    statistic->score+=statistic->relations[index1]->colStats[predicate[1]]->f;
    statistic->predicates.push_back(predicate);
}

int checkconnection(Statistics* statistic,int *predicate){
    for(int i=0;i<statistic->relations.size();i++){
        if(statistic->relations[i]->rel_index==predicate[0] || statistic->relations[i]->rel_index==predicate[3] )
            return 1;
    }
    return 0;
}
int check_if_is_calculated(int* predicate1,int* predicate2){
    for(int i=0;i<5;i++){
        if(predicate1[i]!=predicate2[i])
            return 0;
    }
    return 1;
}
void alltypesfinder(vector<Statistics*> &allstatistics,Statistics* statistic,int num,SQLquery* query,relation** rels){
    for(int i=0;i<num;i++){
        int flag=0;
        for(int j=0;j<statistic->predicates.size();j++){
            if(check_if_is_calculated(statistic->predicates[j],allstatistics[i]->predicates[0])){
                flag=1;
                break;
            }
        }
        if(!checkconnection(statistic,allstatistics[i]->predicates[0]) || flag==1)
            continue;
        
        Statistics* newstatistic=new Statistics(statistic);
        int ret=determinetype(query,newstatistic,allstatistics[i]->predicates[0]);
        if(ret==0){
            calculate_filter(query,newstatistic,allstatistics[i]->predicates[0],rels);
        }
        else if(ret==1){
            calculate_join(query,newstatistic,allstatistics[i]->predicates[0],rels);
        }
        else if(ret==2){
            calculate_autocorrelation(query,newstatistic,allstatistics[i]->predicates[0],rels);
        }
        allstatistics.push_back(newstatistic);
        alltypesfinder(allstatistics,newstatistic,num,query,rels);
    }
}

void statistics(SQLquery* query,relation** rels){
    vector<Statistics*> allstatistics;
    for(int i=0;i<query->predicates.size();i++){
        Statistics* newstatistic=new Statistics();
        int ret=determinetype(query,newstatistic,query->predicates[i]);
        if(ret==0){
            calculate_filter(query,newstatistic,query->predicates[i],rels);
        }
        else if(ret==1){
            calculate_join(query,newstatistic,query->predicates[i],rels);
        }
        else if(ret==2){
            calculate_autocorrelation(query,newstatistic,query->predicates[i],rels);
        }  
        allstatistics.push_back(newstatistic);
    }
    for(int i=0;i<query->predicates.size();i++){
        alltypesfinder(allstatistics,allstatistics[i],query->predicates.size(),query,rels);
    }
    u_int64_t min=2000000000;
    int minindex=-1;
    for(int i=0;i<allstatistics.size();i++){
        if(allstatistics[i]->predicates.size()==query->predicates.size()){
            if(allstatistics[i]->score<min){
                minindex=i;
                min=allstatistics[i]->score;
            }
        }
    }
    query->predicates.clear();
    for(int j=0;j<allstatistics[minindex]->predicates.size();j++){
        query->predicates.push_back(allstatistics[minindex]->predicates[j]);
    }
}

void categoriser(SQLquery* query,relation **rels,vector<string*> &results,int numofrels){
    vector<midResult*> midresults;
    executefilters(query,rels,midresults,numofrels);
    // if(midresults.size()!=0){
    //     cout << "midresults size after filters = " << midresults.size() << " num of columns = " << midresults[0]->cols.size() << " num of entries = " << midresults[0]->colSize << endl;
    //     cout << "RelId = ";
    //     for(int i=0;i<midresults[0]->relId.size();i++){
    //         cout << midresults[0]->relId[i] << " ";
    //     }
    //     cout << endl;
    // }
    int numofqueries=query->predicates.size();
    statistics(query,rels);
    for(int i=0;i<numofqueries;i++){
        int index = i;
        if(query->predicates[index][0]==query->predicates[index][3]){ //1)are at the same relation
            // cout << "Same" << endl;
            samerelation(query,rels,index,midresults);  
        }
        else{   //2)belong to different relations
            // cout << "Different" << endl;
            differentrelation(query,rels,index,midresults);
        }
    }
    for(int i=0; i<numofqueries; i++){
        free(query->predicates[0]);
        query->predicates.erase(query->predicates.begin());
    }
    generateResults(query,rels,midresults,results);
    for(int i=0;i<midresults.size();i++){
        for(int j=0;j<midresults[i]->cols.size();j++)
            free(midresults[i]->cols[j]);
        delete midresults[i];
    }
}