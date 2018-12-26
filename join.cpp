#include "join.h"

using namespace std;
int numofbuckets;
listnode::listnode(){
    next = NULL;
    tuples = (result*)malloc(bufsize);
    memset(tuples,0,bufsize);
}

void listnode::add(int tupleCount,int num1,int num2){
    int index = tupleCount % (bufsize/sizeof(result));
    tuples[index].rowId1 = num1;
    tuples[index].rowId2 = num2;
}

void list::add(int num1,int num2){
    listnode *temp;
    if(head==NULL){//first add to list
        head = new listnode();
        temp = head;
        tail = head;
    }
    else{   
        if(tupleCount % (bufsize/sizeof(result)) == 0){//buffer is full create a new node
            tail->next = new listnode();
            tail = tail->next;
        }
    }

    tail->add(tupleCount,num1,num2);
    tupleCount++;
}

void list::print(){
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

list::~list(){
    // free listnodes
    listnode *temp = head,*next;
    while(temp!=NULL){
        next = temp->next;
        delete temp;
        temp = next;
    }
}

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
    int sum=0,h2val=0,curChainPos=0;
    for(int i=0; i<numofbuckets; i++){
        for(int j=sum; j<sum+hist[i]; j++){
            h2val = hashfun2(table[j].payload);
            // empty bucket position
            if((*bucket)[i*divisor+h2val] == -1){
                (*bucket)[i*divisor+h2val] = j;
            }
            // bucket already pointing to chain position
            else{
                curChainPos = (*bucket)[i*divisor+h2val];
                (*chain)[j] = curChainPos;
                (*bucket)[i*divisor+h2val] = j;
            }
        }
        sum+=hist[i];
    }
}

list* getResults(int numofentries,Tuple *A, Tuple *B,int *chain, int *bucket, int biggestTable){
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
                if(biggestTable == 1){
                    output << A[i].key << "," << B[chainPos].key << endl;
                    l->add(A[i].key,B[chainPos].key);
                }
                else if(biggestTable == 2){
                    output << B[chainPos].key << "," << A[i].key << endl;
                    l->add(B[chainPos].key,A[i].key);
                }
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

void free_memory(Tuple** hash,int** hist,int** psum)
{
    free(*hash);
    free(*hist);
    free(*psum);
}

list* RadixHashJoin(uint64_t* A, int A_size, uint64_t* B, int B_size){
    Tuple *A_Sorted,*B_Sorted;
    int *A_hist,*A_psum,*B_hist,*B_psum,*A_chain,*A_bucket,*B_chain,*B_bucket;
    list *l;
    
    // init arrays and sort
    A_Sorted = (Tuple*)malloc(A_size*sizeof(Tuple));
    for (int i=0;i<A_size;i++){
        A_Sorted[i].key = -1;
    }
    A_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        A_hist[i]=0;
    A_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(A,A_size,&A_Sorted,&A_hist,&A_psum);

    B_Sorted = (Tuple*)malloc(B_size*sizeof(Tuple));
    for (int i=0;i<B_size;i++){
        B_Sorted[i].key = -1;
    }
    B_hist=(int*)malloc(numofbuckets*sizeof(int));
    for(int i=0;i<numofbuckets;i++)
        B_hist[i]=0;
    B_psum=(int*)malloc(numofbuckets*sizeof(int));

    sort_hashtable(B,B_size,&B_Sorted,&B_hist,&B_psum);

    // Create indexing to the array with the least amount of entries
    if(A_size < B_size){
        create_indexing(A_size,A_Sorted,A_hist,&A_chain,&A_bucket);
        l = getResults(B_size,B_Sorted,A_Sorted,A_chain,A_bucket,2);
        free(A_chain);
        free(A_bucket);
    }else{
        create_indexing(B_size,B_Sorted,B_hist,&B_chain,&B_bucket);
        l = getResults(A_size,A_Sorted,B_Sorted,B_chain,B_bucket,1);
        free(B_chain);
        free(B_bucket);
    }

    free_memory(&A_Sorted,&A_hist,&A_psum);
    free_memory(&B_Sorted,&B_hist,&B_psum);
    return l;
}