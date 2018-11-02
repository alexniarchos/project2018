#include <iostream>
#include <stdlib.h>
#include <ctime>
#include <cstring>
#include <stdio.h>
#include <fstream>

using namespace std;

#define divisor 3 //hash function 2 mod value
#define bufsize 40 //size of bytes for each listnode tuple array

struct Tuple {
    int key;
    int payload;
};

struct result {
    int rowId1;
    int rowId2;
};

class listnode{
    public:

    listnode *next;
    int *tuples;

    listnode(){
        next = NULL;
        tuples = (int*)malloc(bufsize);
    }

    void add(int tupleCount,int num1,int num2){
        int index = tupleCount % (bufsize/sizeof(int));
        cout << "index = " << index << endl;
        tuples[index] = num1;
        tuples[index+1] = num2;
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
            if(tupleCount % (bufsize/sizeof(int)) == 0){//buffer is full create new one
                temp->next = new listnode();
                temp = temp->next;
            }
        }

        temp->add(tupleCount,num1,num2);
        tupleCount+=2;//there are two ints in each node
    }

    void print(){
        cout << "Printing...\nSizeof(int) = " << sizeof(int) << endl;
        listnode *temp = head;
        while(temp!=NULL){
            cout << "---end of buffer---" << endl;
            for(int i=0;i<bufsize/sizeof(int) - 1;i+=2){
                cout << temp->tuples[i] << " , " << temp->tuples[i+1] << endl;
            }
            temp = temp->next;
        }
    }

    ~list(){
        // free listnodes
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

void init_and_get_values(int* n,int* numofentries,int* numofbuckets,Tuple** a,Tuple** hash,int** hist,int** psum,char* filename)
{   
    //get values and calculate sizes
    *n=3;
    // *numofentries=10;
    *numofbuckets=1;
    for(int i=0;i<*n;i++){
        *numofbuckets*=2;
    }
    //init
    *a=(Tuple*)malloc((*numofentries)*sizeof(Tuple));
    *hash=(Tuple*)malloc((*numofentries)*sizeof(Tuple));
    FILE* file=fopen(filename,"r");
    char* line=NULL;
    size_t falsebuffer=0;
    for (int i=0;i<(*numofentries);i++){
        getline(&line,&falsebuffer,file);
        char* token=strtok(line,",");
        (*a)[i].key=atoi(token);
        token=strtok(NULL,"\n");
        (*a)[i].payload=atoi(token);
        free(line);
        line=NULL;
        falsebuffer=0;
        (*hash)[i].key=-1;
    }
    free(line);
    fclose(file);


    *hist=(int*)malloc((*numofbuckets)*sizeof(int));
    for(int i=0;i<(*numofbuckets);i++)
        (*hist)[i]=0;
    *psum=(int*)malloc((*numofbuckets)*sizeof(int));
}

void sort_hashtable(int n,int numofentries,int numofbuckets,Tuple* a,Tuple** hash,int** hist,int** psum){
    for(int i=0;i<numofentries;i++){
        int temp_binary=dec_to_bin(a[i].payload);
        cout<<"rowId = "<< a[i].key << " payload = " << a[i].payload << endl;
        cout<<temp_binary<<endl;
        int index=hashvalue(temp_binary,n);
        cout<<index<<endl;
        (*hist)[index]++;
    }
    
    for(int i=0;i<numofbuckets;i++){
        if(i==0)
            (*psum)[i]=0;
        else
            (*psum)[i]=(*psum)[i-1]+(*hist)[i-1];
    }

    for(int i=0;i<numofentries;i++){
        int temp_binary=dec_to_bin(a[i].payload);
        int index=hashvalue(temp_binary,n);
        int j=0;
        while(1){
            if((*hash)[(*psum)[index]+j].key==-1){    
                (*hash)[(*psum)[index]+j].key=a[i].key;
                (*hash)[(*psum)[index]+j].payload=a[i].payload;
                break;
            }
            j++;
        }
    }
}

int hashfun2(int value){
    return value % divisor;
}

void create_indexing(int numofbuckets,int numofentries,Tuple *table,int* hist, int** chain, int** bucket){
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
            if(tempPos > 13){
                cout << write_on_bucket << " " << i*divisor+tempVal << " " << tempPos << endl;
            }
            if(write_on_bucket){
                (*bucket)[i*divisor+tempVal] = tempPos;//bucket array needs offset
            }
            
        }
        sum+=hist[i];
    }
    for(int i=0; i<numofentries; i++){
        cout << table[i].payload << "  ";
    }
    cout << endl;
    cout << "Printing chain" << endl;
    for(int i=0; i<numofentries; i++){
        cout << (*chain)[i] << "  ";
    }
    cout << endl;
    cout << "Printing bucket" << endl;
    for(int i=0; i<numofbuckets*divisor; i++){
        cout << (*bucket)[i] << "  ";
    }
    cout << endl;
}

void getResults(int n,Tuple *A,int A_numofentries, Tuple *B,int *chain, int *bucket){
    int h1,h2,chainVal,chainPos;
    ofstream output;
    output.open("output.csv");
    list *l = new list();
    for(int i=0;i<A_numofentries;i++){
        h1 = hashvalue(dec_to_bin(A[i].payload),n);
        h2 = hashfun2(A[i].payload);
        // cout << "h1 = " << h1 << " " << "h2 = " << h2 << endl;
        chainPos = bucket[h1*divisor+h2];
        if(chainPos == -1){
            continue;
        }
        // cout << chainPos << endl;
        // cout << "---------- " << A[i].key << " " << A[i].payload << endl;
        while(1){
            // cout << "comparing: " << B[chainPos].payload << " == " << A[i].payload << endl;
            if(B[chainPos].payload == A[i].payload){
                cout << A[i].key << "," << B[chainPos].key << endl;
                output << A[i].key << "," << B[chainPos].key << endl;
                l->add(A[i].key,B[chainPos].key);
            }
            // cout << "chainPos = " << chainPos << endl;
            if(chain[chainPos] == -1){
                break;
            }
            chainPos = chain[chainPos];
        }
    }
    // print list
    l->print();
    output.close();
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
int main(void){
    int n,A_numofentries,A_numofbuckets,B_numofentries,B_numofbuckets;
    Tuple *A,*A_Sorted,*B,*B_Sorted;
    int *A_hist,*A_psum,*B_hist,*B_psum,*A_chain,*A_bucket,*B_chain,*B_bucket;
    srand ( time(NULL) );
    create_csv();
    A_numofentries = getnumofentries((char*)"a.csv");
    cout<<A_numofentries<<endl;
    init_and_get_values(&n,&A_numofentries,&A_numofbuckets,&A,&A_Sorted,&A_hist,&A_psum,(char*)"a.csv");
    sort_hashtable(n,A_numofentries,A_numofbuckets,A,&A_Sorted,&A_hist,&A_psum);

    B_numofentries = getnumofentries((char*)"b.csv");
    cout<<B_numofentries<<endl; 
    init_and_get_values(&n,&B_numofentries,&B_numofbuckets,&B,&B_Sorted,&B_hist,&B_psum,(char*)"b.csv");
    sort_hashtable(n,B_numofentries,B_numofbuckets,B,&B_Sorted,&B_hist,&B_psum);
    cout<<endl<<endl;
    for(int i=0;i<A_numofentries;i++){
        cout<<"A rowId = "<< A_Sorted[i].key << " payload = " << A_Sorted[i].payload << endl;
    }
    cout<<endl<<endl;
    for(int i=0;i<B_numofentries;i++){
        cout<<"B rowId = "<< B_Sorted[i].key << " payload = " << B_Sorted[i].payload << endl;
    }

    // Create indexing to the array with the least amount of entries
    if(A_numofentries < B_numofentries){
        create_indexing(A_numofbuckets,A_numofentries,A_Sorted,A_hist,&A_chain,&A_bucket);
        for(int i=0;i<A_numofentries;i++){
            cout<<"A rowId = "<< A_Sorted[i].key << " payload = " << A_Sorted[i].payload << endl;
        }
        cout << "Printing hist" << endl;
        for(int i=0;i<A_numofbuckets;i++){
            cout << A_hist[i] << " ";
        }
        cout << endl;
        getResults(n,B_Sorted,B_numofentries,A_Sorted,A_chain,A_bucket);
    }else{
        create_indexing(B_numofbuckets,B_numofentries,B_Sorted,B_hist,&B_chain,&B_bucket);
        for(int i=0;i<B_numofentries;i++){
            cout<<"B rowId = "<< B_Sorted[i].key << " payload = " << B_Sorted[i].payload << endl;
        }
        cout << "Printing hist" << endl;
        for(int i=0;i<B_numofbuckets;i++){
            cout << B_hist[i] << " ";
        }
        cout << endl;
        getResults(n,A_Sorted,A_numofentries,B_Sorted,B_chain,B_bucket);
    }




    free_memory(&A,&A_Sorted,&A_hist,&A_psum);
    free_memory(&B,&B_Sorted,&B_hist,&B_psum);
}