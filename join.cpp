#include <iostream>
#include <ctime>

using namespace std;

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
void init_and_get_values(int* n,int* numofentries,int* numofbuckets,int32_t** a,int32_t** hash,int32_t** hist,int32_t** psum)
{   
    //get values and calculate sizes
    *n=3;
    *numofentries=10;
    *numofbuckets=1;
    for(int i=0;i<*n;i++){
        *numofbuckets*=2;
    }
    //init
    *a=(int32_t*)malloc((*numofentries)*sizeof(int32_t));
    *hash=(int32_t*)malloc((*numofentries)*sizeof(int32_t));

    srand ( time(NULL) );
    for (int i=0;i<(*numofentries);i++){    
        (*a)[i]=rand()%1024;
        (*hash)[i]=-1;
    }

    *hist=(int32_t*)malloc((*numofbuckets)*sizeof(int32_t));
    for(int i=0;i<(*numofbuckets);i++)
        (*hist)[i]=0;
    *psum=(int32_t*)malloc((*numofbuckets)*sizeof(int));

}

void sort_hashtable(int n,int numofentries,int numofbuckets,int32_t* a,int32_t** hash,int32_t** hist,int32_t** psum){
    for(int i=0;i<numofentries;i++){
        int temp_binary=dec_to_bin(a[i]);
        cout<<a[i]<<endl;
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
        int temp_binary=dec_to_bin(a[i]);
        int index=hashvalue(temp_binary,n);
        int j=0;
        while(1){
            if((*hash)[(*psum)[index]+j]==-1){    
                (*hash)[(*psum)[index]+j]=a[i];
                break;
            }
            j++;
        }
    }
}

void free_memory(int32_t** a,int32_t** hash,int32_t** hist,int32_t** psum)
{
    free(*a);
    free(*hash);
    free(*hist);
    free(*psum);
}

int main(void){
    int n,numofentries,numofbuckets;
    int32_t *a,*hash,*hist,*psum;
    init_and_get_values(&n,&numofentries,&numofbuckets,&a,&hash,&hist,&psum);
    sort_hashtable(n,numofentries,numofbuckets,a,&hash,&hist,&psum);
    cout<<endl<<endl;
    for(int i=0;i<numofentries;i++){
        cout<<hash[i]<<endl;
    }
    free_memory(&a,&hash,&hist,&psum);
}