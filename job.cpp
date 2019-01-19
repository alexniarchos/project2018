#include <iostream>
#include "join.h"
#include <pthread.h>

using namespace std;

JobScheduler::JobScheduler(){
    pthread_mutex_init(&queueLock,NULL);
    pthread_cond_init(&queueNotEmpty,NULL);
    pthread_cond_init(&queueEmpty,NULL);
}

JobScheduler::~JobScheduler(){
    
}

bool JobScheduler::Init(int num_of_threads){
    KILL_THREADS = false;
    // create thread pool
    for(int i=0;i<num_of_threads;i++){
        pthread_create(&threads[i],NULL,threadFun,NULL);
    }
}

void JobScheduler::Schedule(Job *job){
    // add job to queue
    jobQueue.push_back(job);
}

int JobScheduler::ExecuteJob(){
    Job *job = jobQueue[0];

    // remove job from queue
    jobQueue.erase(jobQueue.begin());

    // unlock
    pthread_mutex_unlock(&queueLock);

    // execute job
    job->Run();

    // delete job
    delete job;

    // lock
    pthread_mutex_lock(&queueLock);

    totaljobs--;
    if(totaljobs==0){
        pthread_cond_signal(&jobScheduler->queueEmpty);
    }

    // unlock
    pthread_mutex_unlock(&queueLock);
}

void JobScheduler::Barrier(){
    pthread_mutex_lock(&queueLock);
    while(totaljobs > 0){
        pthread_cond_wait(&queueEmpty,&queueLock);
    }
    pthread_mutex_unlock(&queueLock);
}

bool JobScheduler::Destroy(){
    pthread_mutex_destroy(&queueLock);
    pthread_cond_destroy(&queueEmpty);
    pthread_cond_destroy(&queueNotEmpty);
}

void JobScheduler::Stop(){
    KILL_THREADS = true;
    pthread_cond_broadcast(&queueNotEmpty);
    for(int i=0;i<THREADS;i++){
        pthread_join(threads[i],NULL);
    }
}

int HistogramJob::Run(){
    for(int i=start;i<end;i++){
        uint64_t temp_binary=dec_to_bin(col[i]);
        int index=hashvalue(temp_binary,n_last_digits);
        hist[index]++;
    }
}

int PartitionJob::Run(){
    for(int i=start;i<end;i++){
        uint64_t temp_binary=dec_to_bin(col[i]);
        int index=hashvalue(temp_binary,n_last_digits);
        hash[psum[index]+histCount[index]].key=i;
        hash[psum[index]+histCount[index]].payload=col[i];
        histCount[index]++;
    }
}

int JoinJob::Run(){
    int h1,h2,chainVal,chainPos;
    for(int i=start;i<end;i++){
        h1 = hashvalue(dec_to_bin(A[i].payload),n_last_digits);
        h2 = hashfun2(A[i].payload);
        chainPos = bucket[h1*divisor+h2];
        if(chainPos == -1){
            continue;
        }
        while(1){
            if(B[chainPos].payload == A[i].payload){
                if(biggestTable == 1){
                    l->add(A[i].key,B[chainPos].key);
                }
                else if(biggestTable == 2){
                    l->add(B[chainPos].key,A[i].key);
                }
            }
            if(chain[chainPos] == -1){
                break;
            }
            chainPos = chain[chainPos];
        }
    }
}

void *threadFun(void*){
    while(1){
        // lock
        pthread_mutex_lock(&jobScheduler->queueLock);
        while(jobScheduler->jobQueue.size()<1 && jobScheduler->KILL_THREADS == false){
            // queue is empty wait for next batch of jobs to be scheduled
            pthread_cond_wait(&jobScheduler->queueNotEmpty,&jobScheduler->queueLock);
        }
        // cout << "kt = " << jobScheduler->KILL_THREADS << endl;
        if(jobScheduler->KILL_THREADS){
            pthread_mutex_unlock(&jobScheduler->queueLock);
            pthread_exit(NULL);
        }
        // execute job
        jobScheduler->ExecuteJob();
    }
}