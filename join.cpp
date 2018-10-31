#include <iostream>
#include <stdlib.h>
#include <ctime>

using namespace std;

#define divisor 3 //hash function 2 mod value

struct Tuple {
    int32_t key;
    int32_t payload;
};

struct result {
    int32_t rowId1;
    int32_t rowId2;
};

int32_t dec_to_bin(int32_t decimal) {
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

void init_and_get_values1(int* n,int* numofentries,int* numofbuckets,Tuple** a,Tuple** hash,int32_t** hist,int32_t** psum)
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

    for (int i=0;i<(*numofentries);i++){
        (*a)[i].key=i;
        // (*a)[i].payload=rand()%1024;
        (*hash)[i].key=-1;
    }

    (*a)[0].payload = 0;
    (*a)[1].payload = 1;
    (*a)[2].payload = 8;
    (*a)[3].payload = 16;
    (*a)[4].payload = 2;
    (*a)[5].payload = 3;
    (*a)[6].payload = 4;
    (*a)[7].payload = 24;
    (*a)[8].payload = 5;
    (*a)[9].payload = 6;


    *hist=(int32_t*)malloc((*numofbuckets)*sizeof(int32_t));
    for(int i=0;i<(*numofbuckets);i++)
        (*hist)[i]=0;
    *psum=(int32_t*)malloc((*numofbuckets)*sizeof(int));
}

void init_and_get_values2(int* n,int* numofentries,int* numofbuckets,Tuple** a,Tuple** hash,int32_t** hist,int32_t** psum)
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

    for (int i=0;i<(*numofentries);i++){
        (*a)[i].key=i;
        // (*a)[i].payload=rand()%1024;
        (*hash)[i].key=-1;
    }

    (*a)[0].payload = 0;
    (*a)[1].payload = 1;
    (*a)[2].payload = 8;
    (*a)[3].payload = 16;
    (*a)[4].payload = 2;
    (*a)[5].payload = 3;
    (*a)[6].payload = 4;
    (*a)[7].payload = 24;
    (*a)[8].payload = 5;
    (*a)[9].payload = 6;
    (*a)[10].payload = 7;
    (*a)[11].payload = 8;
    (*a)[12].payload = 9;
    (*a)[13].payload = 6;
    (*a)[14].payload = 11;

    *hist=(int32_t*)malloc((*numofbuckets)*sizeof(int32_t));
    for(int i=0;i<(*numofbuckets);i++)
        (*hist)[i]=0;
    *psum=(int32_t*)malloc((*numofbuckets)*sizeof(int));
}

void sort_hashtable(int n,int numofentries,int numofbuckets,Tuple* a,Tuple** hash,int32_t** hist,int32_t** psum){
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

void create_indexing(int numofbuckets,int numofentries,Tuple *table,int32_t* hist, int32_t** chain, int32_t** bucket){
    // allocate chain and bucket
    *chain = (int32_t*)malloc(numofentries*sizeof(int32_t));
    for(int i=0;i<numofentries;i++){
        (*chain)[i]=-1;
    }
    *bucket = (int32_t*)malloc(numofbuckets*divisor*sizeof(int32_t));

    // fill chain and bucket with indexes
    int sum=0,tempPos,tempVal;
    bool write_on_bucket=true;
    for(int i=0; i<numofbuckets; i++){
        for(int j=sum; j<sum+hist[i]; j++){
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

void getResults(int n,Tuple *A,int A_numofentries, Tuple *B,int32_t *chain, int32_t *bucket){
    int h1,h2,chainVal,chainPos;
    for(int i=0;i<A_numofentries;i++){
        h1 = hashvalue(dec_to_bin(A[i].payload),n);
        h2 = hashfun2(A[i].payload);
        // cout << "h1 = " << h1 << " " << "h2 = " << h2 << endl;
        chainPos = bucket[h1*divisor+h2];
        // cout << chainPos << endl;
        // cout << "---------- " << A[i].key << " " << A[i].payload << endl;
        while(1){
            // cout << "comparing: " << B[chainPos].payload << " == " << A[i].payload << endl;
            if(B[chainPos].payload == A[i].payload){
                cout << A[i].key << "," << B[chainPos].key << endl;
            }
            // cout << "chainPos = " << chainPos << endl;
            if(chain[chainPos] == -1){
                break;
            }
            chainPos = chain[chainPos];
        }
    }
}

void free_memory(Tuple** a,Tuple** hash,int32_t** hist,int32_t** psum)
{
    free(*a);
    free(*hash);
    free(*hist);
    free(*psum);
}

int main(void){
    int n,A_numofentries,A_numofbuckets,B_numofentries,B_numofbuckets;
    Tuple *A,*A_Sorted,*B,*B_Sorted;
    int32_t *A_hist,*A_psum,*B_hist,*B_psum,*A_chain,*A_bucket,*B_chain,*B_bucket;
    srand ( time(NULL) );

    A_numofentries = 10;
    init_and_get_values1(&n,&A_numofentries,&A_numofbuckets,&A,&A_Sorted,&A_hist,&A_psum);
    sort_hashtable(n,A_numofentries,A_numofbuckets,A,&A_Sorted,&A_hist,&A_psum);

    B_numofentries = 15;
    init_and_get_values2(&n,&B_numofentries,&B_numofbuckets,&B,&B_Sorted,&B_hist,&B_psum);
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
    }




    free_memory(&A,&A_Sorted,&A_hist,&A_psum);
    free_memory(&B,&B_Sorted,&B_hist,&B_psum);
}