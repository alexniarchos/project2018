#include <iostream>
#include <stdlib.h>
#include <vector>

using namespace std;

enum JobType{histogramJob,partitionJob,joinJob};

#define THREADS 2

void *threadFun(void*);

// Abstract Class Job
class Job{
    public:
        Job();
        virtual ~Job();
        // This method should be implemented by subclasses.
        virtual int Run() = 0;
};

class HistogramJob : public Job{
    public:
        JobType jobType;
        HistogramJob(){
            jobType = histogramJob;
        }
};

// Class JobScheduler
class JobScheduler {
    public:
        pthread_t threads[THREADS];
        vector<Job*> jobQueue;
        pthread_mutex_t queueLock;
        pthread_cond_t queueNotEmpty;
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
        // Waits until all threads finish their job, and after that close all threads.
        void Stop();
};