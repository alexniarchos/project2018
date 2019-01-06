#include <iostream>
#include "job.h"
#include <pthread.h>

using namespace std;

JobScheduler::JobScheduler(){
    pthread_mutex_init(&queueLock,NULL);
    pthread_cond_init(&queueNotEmpty,NULL);
}

bool JobScheduler::Init(int num_of_threads){
    // create thread pool
    for(int i=0;i<num_of_threads;i++){
        pthread_create(&threads[i],NULL,threadFun,NULL);
    }
}

void JobScheduler::Schedule(Job *job){
    // lock
    pthread_mutex_lock(&queueLock);
    // add job to queue
    jobQueue.push_back(job);
    // signal
    pthread_cond_signal(&queueNotEmpty);
    // unlock
    pthread_mutex_unlock(&queueLock);
}

void *threadFun(void*){
    cout << "hello world" << endl;
}