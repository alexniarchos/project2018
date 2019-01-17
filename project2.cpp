#include "functions.h"
#include <time.h>

JobScheduler *jobScheduler;

int main(void){
    relation **rels = NULL;
    int numofrels;
    numofbuckets=1;
    for(int i=0;i<n_last_digits;i++){
        numofbuckets*=2;
    }
    rels = init_relations(&numofrels);

    // init job scheduler
    jobScheduler = new JobScheduler();
    jobScheduler->Init(THREADS);

    int counter = 1;
    while(1){
        char* line=NULL;
        size_t len=0;
        vector<string*> results;
        int ret;
        while((ret=getline(&line, &len, stdin)) != -1){
            if(line[0]=='F'){
                counter++;
                break;
            }
            SQLquery* query=new SQLquery();
            query->parser(line);
            cout << "Query: " << counter++ << endl;
            time_t start,end;
            start = time(NULL);
            categoriser(query,rels,results,numofrels);
            end = time(NULL);
            cout << "----query time: \t" << end-start << endl; 
            for(int i=0;i<query->predicates.size();i++)
                free(query->predicates[i]);
            for(int i=0;i<query->views.size();i++)
                free(query->views[i]);
            delete(query);
        }
        free(line);
        for(int i=0;i<results.size();i++){
            cout<<*results[i];
            delete results[i];
        }
        if(ret==-1)
            break;
    }
    jobScheduler->Stop();
    delete jobScheduler;
    for(int i=0;i<numofrels;i++){
        free(rels[i]->cols);
        for(int j=0;j<rels[i]->numofcols;j++){
            free(rels[i]->colStats[j]);
        }
        free(rels[i]->colStats);
        delete rels[i];
    }
    free(rels);
}