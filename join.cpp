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

using namespace std;

#define n_last_digits 3 //number of last digits for hash function 1
#define divisor 13 //hash function 2 mod value
#define bufsize 40 //size of bytes for each listnode tuple array

// globals
int numofbuckets;

struct Tuple {
    int key;
    uint64_t payload;
};

struct result {
    int rowId1;
    int rowId2;
};

struct relation{
    uint64_t **cols;
    int numofcols;
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

uint64_t dec_to_bin(uint64_t decimal) {
    if (decimal == 0) 
        return 0;
    if (decimal == 1) 
        return 1;                  
    return (decimal % 2) + 10 * dec_to_bin(decimal / 2);
}
int hashvalue(uint64_t num,int divider) 
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

relation** init_relations(int *numofrels){
    // read relation names from stdin
    string filename;
    relation **rels = NULL;
    std::vector<string> filenames;
    while (getline(cin, filename)) {
        if (filename == "Done") break;
        filenames.push_back(filename);
    }
    cout << "num of rels = " << filenames.size() << endl;
    *numofrels = filenames.size();
    rels = (relation**)malloc(filenames.size() * sizeof(relation*));
    for(int i=0;i<filenames.size();i++){
        // Obtain file size
        int fd = open(filenames[i].c_str(), O_RDONLY);
        if (fd==-1) {
            cout << "cannot open " << filenames[i].c_str() << endl;
            throw;
        }
        struct stat sb;
        if (fstat(fd,&sb)==-1)
            cout << "fstat\n";

        int length=sb.st_size;
        
        char* addr=static_cast<char*>(mmap(NULL,length,PROT_READ,MAP_PRIVATE,fd,0u));
        if (addr==MAP_FAILED) {
            cout << "cannot mmap " << filenames[i].c_str() << " of length " << length << endl;
            throw;
        }

        if (length<16) {
            cout << "relation file " << filenames[i].c_str() << " does not contain a valid header" << endl;
            throw;
        }
        rels[i] = (relation*)malloc(sizeof(relation));
        rels[i]->numofentries=*(uint64_t*)(addr);
        addr+=sizeof(uint64_t);
        rels[i]->numofcols=*(uint64_t*)(addr);
        cout << filenames[i].c_str() << " entries: " << rels[i]->numofentries << " cols: " << rels[i]->numofcols << endl;
        addr+=sizeof(uint64_t);
        rels[i]->cols = (uint64_t**)malloc(rels[i]->numofcols*sizeof(uint64_t*));
        for (int j=0;j<rels[i]->numofcols;j++) {
            rels[i]->cols[j] = (uint64_t*)(addr);
            addr+=rels[i]->numofentries*sizeof(uint64_t);
        }
    }
    return rels;
}

void sort_hashtable(uint64_t *col,int numofentries,Tuple** hash,int** hist,int** psum){
    for(int i=0;i<numofentries;i++){
        uint64_t temp_binary=dec_to_bin(col[i]);
        int index=hashvalue(temp_binary,n_last_digits);
        (*hist)[index]++;
    }
    
    for(int i=0;i<numofbuckets;i++){
        if(i==0)
            (*psum)[i]=0;
        else
            (*psum)[i]=(*psum)[i-1]+(*hist)[i-1];
    }

    for(int i=0;i<numofentries;i++){
        uint64_t temp_binary=dec_to_bin(col[i]);
        int index=hashvalue(temp_binary,n_last_digits);
        int j=0;
        while(1){
            if((*hash)[(*psum)[index]+j].key==-1){    
                (*hash)[(*psum)[index]+j].key=i;
                (*hash)[(*psum)[index]+j].payload=col[i];
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

list* getResults(int numofentries,Tuple *A, Tuple *B,int *chain, int *bucket){
    int h1,h2,chainVal,chainPos;
    ofstream output;
    output.open("output.csv");
    list *l = new list();
    for(int i=0;i<numofentries;i++){
        h1 = hashvalue(dec_to_bin(A[i].payload),n_last_digits);
        h2 = hashfun2(A[i].payload);
        chainPos = bucket[h1*divisor+h2];
        if(chainPos == -1){
            continue;
        }
        while(1){
            if(B[chainPos].payload == A[i].payload){
                output << A[i].key << "," << B[chainPos].key << endl;
                l->add(A[i].key,B[chainPos].key);
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

list* RadixHashJoin(relation *relA,int numofcolA, relation *relB,int numofcolB){
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

    sort_hashtable(relA->cols[numofcolA],relA->numofentries,&A_Sorted,&A_hist,&A_psum);

    B_Sorted = (Tuple*)malloc(relB->numofentries*sizeof(Tuple));
    for (int i=0;i<relB->numofentries;i++){
        B_Sorted[i].key = -1;
    }
    B_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        B_hist[i]=0;
    B_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(relB->cols[numofcolB],relB->numofentries,&B_Sorted,&B_hist,&B_psum);

    // Create indexing to the array with the least amount of entries
    if(relA->numofentries < relB->numofentries){
        create_indexing(relA->numofentries,A_Sorted,A_hist,&A_chain,&A_bucket);
        l = getResults(relB->numofentries,B_Sorted,A_Sorted,A_chain,A_bucket);
        free(A_chain);
        free(A_bucket);
    }else{
        create_indexing(relB->numofentries,B_Sorted,B_hist,&B_chain,&B_bucket);
        l = getResults(relA->numofentries,A_Sorted,B_Sorted,B_chain,B_bucket);
        free(B_chain);
        free(B_bucket);
    }

    // free_memory(&relA->tuples,&A_Sorted,&A_hist,&A_psum);
    // free_memory(&relB->tuples,&B_Sorted,&B_hist,&B_psum);
    return l;
}

int main(void){
    relation **rels = NULL;
    int numofrels;

    list *result;

    numofbuckets=1;
    for(int i=0;i<n_last_digits;i++){
        numofbuckets*=2;
    }

    // load file into relation
    rels = init_relations(&numofrels);
    cout << "-----------------------------------------------" << endl;
    for(int i=0;i<numofrels;i++){
        for (int j=0;j<rels[i]->numofcols;j++) {
            cout << "first number of column: " << j << " " << (uint64_t)*rels[i]->cols[j] << endl;
            cout << "last number of column: " << j << " " << (uint64_t)rels[i]->cols[j][rels[i]->numofentries-1] << endl;
        }
    }
    
    result = RadixHashJoin(rels[0],0,rels[1],0);
    // print list
    result->print();
    // delete result;
}