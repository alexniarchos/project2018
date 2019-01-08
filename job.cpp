#include <iostream>
#include "job.h"
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

    // execute job
    job->Run();
}

void JobScheduler::Barrier(){
    pthread_mutex_lock(&queueLock);
    while(jobQueue.size()>0){
        pthread_cond_wait(&queueEmpty,&queueLock);
    }
    pthread_mutex_unlock(&queueLock);
}

bool JobScheduler::Destroy(){

}

void JobScheduler::Stop(){

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

void *threadFun(void*){
    while(1){
        // lock
        pthread_mutex_lock(&jobScheduler->queueLock);
        while(jobScheduler->jobQueue.size()<1 && jobScheduler->KILL_THREADS == false){
            // queue is empty wait for next batch of jobs to be scheduled
            pthread_cond_wait(&jobScheduler->queueNotEmpty,&jobScheduler->queueLock);
        }
        if(jobScheduler->KILL_THREADS == false){
            // execute job
            jobScheduler->ExecuteJob();
        }
        if(jobScheduler->jobQueue.size()==0){
            pthread_cond_signal(&jobScheduler->queueEmpty);
        }
        // unlock
        pthread_mutex_unlock(&jobScheduler->queueLock);
    }
}