#include <iostream>
#include <stdlib.h>
#include <ctime>
#include <cstring>
#include <stdio.h>
#include <fstream>

using namespace std;

#define n_last_digits 3 //number of last digits for hash function 1
#define divisor 13 //hash function 2 mod value
#define bufsize 40 //size of bytes for each listnode tuple array

// globals
int numofbuckets;

struct Tuple {
    int key;
    int payload;
};

struct result {
    int rowId1;
    int rowId2;
};

struct relation{
    Tuple *tuples;
    int numofentries;
};

class listnode{
    public:

    listnode *next;
    result *tuples;

    listnode(){
        next = NULL;
        tuples = (result*)malloc(bufsize);
        memset(tuples,0,bufsize);
    }

    void add(int tupleCount,int num1,int num2){
        int index = tupleCount % (bufsize/sizeof(result));
        tuples[index].rowId1 = num1;
        tuples[index].rowId2 = num2;
    }

    ~listnode(){
        free(tuples);
    }
};

class list{
    public:

    int tupleCount;
    listnode *head=NULL;

    list(){
        tupleCount = 0;
    }

    void add(int num1,int num2){
        listnode *temp;
        if(head==NULL){//first add to list
            head = new listnode();
            temp=head;
        }
        else{
            temp=head;
            while(temp->next!=NULL){//get to the tail of the list
                temp = temp->next;
            }
            if(tupleCount % (bufsize/sizeof(result)) == 0){//buffer is full create new one
                temp->next = new listnode();
                temp = temp->next;
            }
        }

        temp->add(tupleCount,num1,num2);
        tupleCount++;
    }

    void print(){
        cout << "Printing result list..." << endl;
        int sum=0,limit;
        listnode *temp = head;
        while(temp!=NULL){
            sum+=bufsize/sizeof(result);
            limit = bufsize/sizeof(result);
            if(sum > tupleCount){
                limit = tupleCount % (bufsize/sizeof(result));
            }
            for(int i=0;i<limit;i++){
                cout << temp->tuples[i].rowId1 << " , " << temp->tuples[i].rowId2 << endl;
            }
            temp = temp->next;
            cout << "---end of buffer---" << endl;
        }
    }

    ~list(){
        // free listnodes
        listnode *temp = head,*next;
        while(temp!=NULL){
            next = temp->next;
            delete temp;
            temp = next;
        }
    }
};

int dec_to_bin(int decimal) {
    if (decimal == 0) 
        return 0;
    if (decimal == 1) 
        return 1;                  
    return (decimal % 2) + 10 * dec_to_bin(decimal / 2);
}
int hashvalue(int num,int divider) 
{ 
    int mod=1;
    for(int i=0;i<divider;i++)
        mod*=10;
    num=num%mod;
    int dec_value = 0; 
      
    int base = 1; 
      
    while (num) 
    { 
        int last_digit = num % 10; 
        num = num/10; 
          
        dec_value += last_digit*base; 
          
        base = base*2; 
    } 
      
    return dec_value; 
}

int getnumofentries(char* file_name)
{
    FILE* file=fopen(file_name,"r");
    char *line=NULL;
    size_t falsebuffer=0;
    int counter=0;
    while(-1!=getline(&line,&falsebuffer,file))
    {
        counter++;
        free(line);
        line=NULL;
        falsebuffer=0;
    }
    fclose(file);
    free(line);
    return counter;
}

void init_and_get_values(relation* rel,char* filename)
{
    rel->numofentries = getnumofentries((char*)filename);
    cout<<"Size of rel: "<< rel->numofentries << endl;
    rel->tuples=(Tuple*)malloc(rel->numofentries*sizeof(Tuple));

    FILE* file=fopen(filename,"r");
    char* line=NULL;
    size_t falsebuffer=0;
    for (int i=0;i<rel->numofentries;i++){
        getline(&line,&falsebuffer,file);
        char* token=strtok(line,",");
        rel->tuples[i].key=atoi(token);
        token=strtok(NULL,"\n");
        rel->tuples[i].payload=atoi(token);
        free(line);
        line=NULL;
        falsebuffer=0;
    }
    free(line);
    fclose(file);
}

void sort_hashtable(relation *rel,Tuple** hash,int** hist,int** psum){
    for(int i=0;i<rel->numofentries;i++){
        int temp_binary=dec_to_bin(rel->tuples[i].payload);
        int index=hashvalue(temp_binary,n_last_digits);
        (*hist)[index]++;
    }
    
    for(int i=0;i<numofbuckets;i++){
        if(i==0)
            (*psum)[i]=0;
        else
            (*psum)[i]=(*psum)[i-1]+(*hist)[i-1];
    }

    for(int i=0;i<rel->numofentries;i++){
        int temp_binary=dec_to_bin(rel->tuples[i].payload);
        int index=hashvalue(temp_binary,n_last_digits);
        int j=0;
        while(1){
            if((*hash)[(*psum)[index]+j].key==-1){    
                (*hash)[(*psum)[index]+j].key=rel->tuples[i].key;
                (*hash)[(*psum)[index]+j].payload=rel->tuples[i].payload;
                break;
            }
            j++;
        }
    }
}

int hashfun2(int value){
    return value % divisor;
}

void create_indexing(int numofentries,Tuple *table,int* hist, int** chain, int** bucket){
    // allocate chain and bucket
    *chain = (int*)malloc(numofentries*sizeof(int));
    for(int i=0;i<numofentries;i++){
        (*chain)[i]=-1;
    }
    *bucket = (int*)malloc(numofbuckets*divisor*sizeof(int));
    for(int i=0;i<numofbuckets*divisor;i++){
        (*bucket)[i]=-1;
    }

    // fill chain and bucket with indexes
    int sum=0,tempPos,tempVal;
    bool write_on_bucket=true;
    for(int i=0; i<numofbuckets; i++){
        for(int j=sum; j<sum+hist[i]; j++){
            write_on_bucket=true;
            tempPos = j;
            tempVal = hashfun2(table[j].payload);
            for(int k=j+1;k<sum+hist[i];k++){
                if(tempVal == hashfun2(table[k].payload) && (*chain)[k] != -1){//already found same hash function 2 values
                    write_on_bucket = false;
                    break;
                }
                if(tempVal == hashfun2(table[k].payload)){
                    (*chain)[k] = tempPos;
                    tempPos = k;
                }
            }
            if(write_on_bucket){
                (*bucket)[i*divisor+tempVal] = tempPos;//bucket array needs offset
            }
            
        }
        sum+=hist[i];
    }
}

list* getResults(relation *relA, Tuple *B,int *chain, int *bucket){
    int h1,h2,chainVal,chainPos;
    ofstream output;
    output.open("output.csv");
    list *l = new list();
    for(int i=0;i<relA->numofentries;i++){
        h1 = hashvalue(dec_to_bin(relA->tuples[i].payload),n_last_digits);
        h2 = hashfun2(relA->tuples[i].payload);
        chainPos = bucket[h1*divisor+h2];
        if(chainPos == -1){
            continue;
        }
        while(1){
            if(B[chainPos].payload == relA->tuples[i].payload){
                output << relA->tuples[i].key << "," << B[chainPos].key << endl;
                l->add(relA->tuples[i].key,B[chainPos].key);
            }
            if(chain[chainPos] == -1){
                break;
            }
            chainPos = chain[chainPos];
        }
    }
    output.close();
    return l;
}

void free_memory(Tuple** a,Tuple** hash,int** hist,int** psum)
{
    free(*a);
    free(*hash);
    free(*hist);
    free(*psum);
}
void create_csv()
{
    remove("a.csv");
    remove("b.csv");
    FILE* a=fopen("a.csv","w");
    FILE* b=fopen("b.csv","w");
    srand(time(NULL));
    int A_numofentries=rand()%1000 + 5;
    int B_numofentries=rand()%1000 + 5;
    for(int i=0;i<A_numofentries;i++)
        fprintf(a,"%d,%d\n",i,rand()%100);
    for(int i=0;i<B_numofentries;i++)
        fprintf(b,"%d,%d\n",i,rand()%50);
    fclose(a);
    fclose(b);
}

list* RadixHashJoin(relation *relA, relation *relB){
    Tuple *A_Sorted,*B_Sorted;
    int *A_hist,*A_psum,*B_hist,*B_psum,*A_chain,*A_bucket,*B_chain,*B_bucket;
    list *l;
    
    // init arrays and sort
    A_Sorted = (Tuple*)malloc(relA->numofentries*sizeof(Tuple));
    for (int i=0;i<relA->numofentries;i++){
        A_Sorted[i].key = -1;
    }
    A_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        A_hist[i]=0;
    A_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(relA,&A_Sorted,&A_hist,&A_psum);

    B_Sorted = (Tuple*)malloc(relB->numofentries*sizeof(Tuple));
    for (int i=0;i<relB->numofentries;i++){
        B_Sorted[i].key = -1;
    }
    B_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        B_hist[i]=0;
    B_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(relB,&B_Sorted,&B_hist,&B_psum);

    // Create indexing to the array with the least amount of entries
    if(relA->numofentries < relB->numofentries){
        create_indexing(relA->numofentries,A_Sorted,A_hist,&A_chain,&A_bucket);
        l = getResults(relB,A_Sorted,A_chain,A_bucket);
        free(A_chain);
        free(A_bucket);
    }else{
        create_indexing(relB->numofentries,B_Sorted,B_hist,&B_chain,&B_bucket);
        l = getResults(relA,B_Sorted,B_chain,B_bucket);
        free(B_chain);
        free(B_bucket);
    }

    free_memory(&relA->tuples,&A_Sorted,&A_hist,&A_psum);
    free_memory(&relB->tuples,&B_Sorted,&B_hist,&B_psum);
    return l;
}

int main(void){
    relation *relA,*relB;
    relA = (relation*)malloc(sizeof(relation));
    relB = (relation*)malloc(sizeof(relation));

    list *result;

    srand ( time(NULL) );
    // create input files
    create_csv();

    numofbuckets=1;
    for(int i=0;i<n_last_digits;i++){
        numofbuckets*=2;
    }

    // load file into relation
    init_and_get_values(relA,(char*)"a.csv");
    init_and_get_values(relB,(char*)"b.csv");

    result = RadixHashJoin(relA,relB);
    free(relA);
    free(relB);
    // print list
    result->print();
    delete result;
}