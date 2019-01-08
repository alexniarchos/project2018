#include <iostream>
#include <stdlib.h>
#include <vector>

using namespace std;

#ifndef JOB_H
#define JOB_H

#define THREADS 4

void *threadFun(void*);

struct Tuple{
    int key;
    uint64_t payload;
};

// Abstract Class Job
class Job{
    public:
        Job()=default;
        virtual ~Job(){};
        // This method should be implemented by subclasses.
        virtual int Run() = 0;
};

class HistogramJob : public Job{
    public:
        int start,end;
        int *hist;
        uint64_t *col;
        HistogramJob(int* hist,int start,int end,uint64_t *col){
            this->hist = hist;
            this->start = start;
            this->end = end;
            this->col = col;
        };
        ~HistogramJob(){};
        int Run();
};

class PartitionJob : public Job{
    public:
        int start,end;
        int *histCount,*psum;
        Tuple *hash;
        uint64_t *col;
        PartitionJob(Tuple *hash,int* histCount,int start,int end,uint64_t *col,int *psum){
            this->histCount = histCount;
            this->start = start;
            this->end = end;
            this->col = col;
            this->hash = hash;
            this->psum = psum;
        };
        ~PartitionJob(){};
        int Run();
};

// Class JobScheduler
class JobScheduler {
    public:
        pthread_t threads[THREADS];
        vector<Job*> jobQueue;
        pthread_mutex_t queueLock;
        pthread_cond_t queueNotEmpty;
        pthread_cond_t queueEmpty;
        int KILL_THREADS;
        JobScheduler();
        ~JobScheduler();
        // Initializes the JobScheduler with the number of open threads.
        // Returns true if everything done right false else.
        bool Init(int num_of_threads);
        // Free all resources that the are allocated by JobScheduler
        // Returns true if everything done right false else.
        bool Destroy();
        // Waits Until executed all jobs in the queue.
        void Barrier();
        // Add a job in the queue and returns a JobId
        void Schedule(Job* job);
        // Remove first job from the queue and execute
        int ExecuteJob();
        // Waits until all threads finish their job, and after that close all threads.
        void Stop();
};

#endif