#include "join.h"
#include <time.h>

#define N 49999991 //largest prime number below 50.000.000
#define ceil(x,y) (x+y-1)/y

using namespace std;

int numofbuckets;

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
        // rels[i] = (relation*)malloc(sizeof(relation));
        rels[i] = new relation();
        rels[i]->numofentries=*(uint64_t*)(addr);
        addr+=sizeof(uint64_t);
        rels[i]->numofcols=*(uint64_t*)(addr);
        cout << filenames[i].c_str() << " entries: " << rels[i]->numofentries << " cols: " << rels[i]->numofcols << endl;
        addr+=sizeof(uint64_t);
        rels[i]->cols = (uint64_t**)malloc(rels[i]->numofcols*sizeof(uint64_t*));
        rels[i]->colStats = (ColStats**)malloc(rels[i]->numofcols*sizeof(ColStats*));
        for (int j=0;j<rels[i]->numofcols;j++) {
            rels[i]->cols[j] = (uint64_t*)(addr);
            addr+=rels[i]->numofentries*sizeof(uint64_t);
            // calculate stats for each collumn
            rels[i]->colStats[j] = (ColStats*)malloc(sizeof(ColStats));
            int max = rels[i]->cols[j][0], min = rels[i]->cols[j][0];
            for(int k=0;k<rels[i]->numofentries;k++){
                if(rels[i]->cols[j][k] > max){
                    max = rels[i]->cols[j][k];
                }
                else if(rels[i]->cols[j][k] < min){
                    min = rels[i]->cols[j][k];
                }
            }
            rels[i]->colStats[j]->u = max;
            rels[i]->colStats[j]->l = min;
            rels[i]->colStats[j]->f = rels[i]->numofentries;
            // create distinct values array to calculate distinct values
            int distinctSize = max-min+1;
            if(distinctSize > N){
                distinctSize = N;
            }
            bool *distinctVal = (bool*)malloc(distinctSize*sizeof(bool));
            // initialize array
            for(int k=0;k<distinctSize;k++){
                distinctVal[k] = false;
            }
            // if a value is found set position of the array to true
            for(int k=0;k<rels[i]->numofentries;k++){
                distinctVal[(rels[i]->cols[j][k] - min) % N] = true;
            }
            int counter=0;
            for(int k=0;k<distinctSize;k++){
                if(distinctVal[k] == true){
                    counter++;
                }
            }
            // update stats with distinct values
            rels[i]->colStats[j]->d = counter;
            // free distinct array
            free(distinctVal);
            // print stats
            cout << "u = " << rels[i]->colStats[j]->u << " l = " << rels[i]->colStats[j]->l << " f = " << rels[i]->colStats[j]->f << " d = " << rels[i]->colStats[j]->d << endl;
        }
    }
    return rels;
}

void sort_hashtable(uint64_t *col,int numofentries,Tuple** hash,int** hist,int** psum){
    int threadHist[THREADS][numofbuckets];
    for(int i=0;i<THREADS;i++){
        for(int j=0;j<numofbuckets;j++){
            threadHist[i][j] = 0;
        }
    }
    int start=0,end=0,entriesForThread = ceil(numofentries,THREADS);
    // lock
    pthread_mutex_lock(&jobScheduler->queueLock);
    for(int i=0;i<THREADS;i++){
        start = end;
        end += entriesForThread;
        if(end > numofentries){
            end = numofentries;
        }
        // cout << "numofentries: " << numofentries << " start: " << start << " end: " << end << endl;
        jobScheduler->Schedule(new HistogramJob(threadHist[i],start,end,col));
    }
    // signal
    pthread_cond_signal(&jobScheduler->queueNotEmpty);
    // unlock
    pthread_mutex_unlock(&jobScheduler->queueLock);

    jobScheduler->Barrier();

    for(int i=0;i<THREADS;i++){
        for(int j=0;j<numofbuckets;j++){
            (*hist)[j] += threadHist[i][j];
        }
    }

    for(int i=0;i<numofbuckets;i++){
        if(i==0)
            (*psum)[i]=0;
        else
            (*psum)[i]=(*psum)[i-1]+(*hist)[i-1];
    }

    // use secondary table to count how many of the items have been copied to sorted array
    int histCount[THREADS][numofbuckets];
    for(int i=0;i<THREADS;i++){
        for(int j=0;j<numofbuckets;j++){
            histCount[i][j]=0;
        }
    }

    Tuple threadHash[THREADS][numofentries];

    for(int i=0;i<numofentries;i++){
        threadHash[0][i].key = 0;
        threadHash[0][i].payload = 0;
    }

    start=0;
    end=0;
    // lock
    pthread_mutex_lock(&jobScheduler->queueLock);
    for(int i=0;i<THREADS;i++){
        start = end;
        end += entriesForThread;
        if(end > numofentries){
            end = numofentries;
        }
        // cout << "numofentries: " << numofentries << " start: " << start << " end: " << end << endl;
        jobScheduler->Schedule(new PartitionJob(threadHash[i],histCount[i],start,end,col,*psum));
    }
    // signal
    pthread_cond_signal(&jobScheduler->queueNotEmpty);
    // unlock
    pthread_mutex_unlock(&jobScheduler->queueLock);

    jobScheduler->Barrier();

    int totalHistCount[numofbuckets];
    for(int i=0;i<numofbuckets;i++){
        totalHistCount[i] = 0;
    }

    int threadHashPos=0;
    for(int i=0;i<numofbuckets;i++){
        if(i>0){
            threadHashPos += totalHistCount[i-1];
        }
        for(int j=0;j<THREADS;j++){
            for(int k=0;k<histCount[j][i];k++){
                (*hash)[(*psum)[i]+totalHistCount[i]].key = threadHash[j][k+threadHashPos].key;
                (*hash)[(*psum)[i]+totalHistCount[i]].payload = threadHash[j][k+threadHashPos].payload;
                totalHistCount[i]++;
            }
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

list* getResults(int numofentries,Tuple *A,int *A_hist,Tuple *B,int *chain, int *bucket, int biggestTable){
    int h1,h2,chainVal,chainPos;
    list *l = new list();
    list *thread_lists[numofbuckets];
    for(int i=0;i<numofbuckets;i++){
        thread_lists[i] = new list();
    }

    int start=0,end=0;
    // lock
    pthread_mutex_lock(&jobScheduler->queueLock);
    for(int i=0;i<numofbuckets;i++){
        start = end;
        end += A_hist[i];
        // cout << "numofentries: " << numofentries << " start: " << start << " end: " << end << endl;
        jobScheduler->Schedule(new JoinJob(thread_lists[i],start,end,A,B,chain,bucket,biggestTable));
    }
    // signal
    pthread_cond_signal(&jobScheduler->queueNotEmpty);
    // unlock
    pthread_mutex_unlock(&jobScheduler->queueLock);

    jobScheduler->Barrier();

    for(int i=0;i<numofbuckets;i++){
        int sum=0,limit;
        listnode *temp = thread_lists[i]->head;
        while(temp!=NULL){
            sum+=bufsize/sizeof(result);
            limit = bufsize/sizeof(result);
            if(sum > thread_lists[i]->tupleCount){
                limit = thread_lists[i]->tupleCount % (bufsize/sizeof(result));
            }
            for(int i=0;i<limit;i++){
                l->add(temp->tuples[i].rowId1,temp->tuples[i].rowId2);
            }
            temp = temp->next;
        }
        delete thread_lists[i];
    }
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
    time_t start,end;
    start = time(NULL);
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
    end = time(NULL);
    cout << "\t\t\tsorting: \t" << end-start << endl;
    // Create indexing to the array with the least amount of entries
    if(A_size < B_size){
        start = time(NULL);
        create_indexing(A_size,A_Sorted,A_hist,&A_chain,&A_bucket);
        end = time(NULL);
        cout << "\t\t\tindexing: \t" << end-start << endl;
        start = time(NULL);
        l = getResults(B_size,B_Sorted,B_hist,A_Sorted,A_chain,A_bucket,2);
        end = time(NULL);
        cout << "\t\t\tjoining: \t" << end-start << endl;
        free(A_chain);
        free(A_bucket);
    }else{
        start = time(NULL);
        create_indexing(B_size,B_Sorted,B_hist,&B_chain,&B_bucket);
        end = time(NULL);
        cout << "\t\t\tindexing: \t" << end-start << endl;
        start = time(NULL);
        l = getResults(A_size,A_Sorted,A_hist,B_Sorted,B_chain,B_bucket,1);
        end = time(NULL);
        cout << "\t\t\tjoining: \t" << end-start << endl;
        free(B_chain);
        free(B_bucket);
    }

    free_memory(&A_Sorted,&A_hist,&A_psum);
    free_memory(&B_Sorted,&B_hist,&B_psum);
    return l;
}