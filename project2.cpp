#include "functions.h"

int main(void){
    relation **rels = NULL;
    int numofrels;
    numofbuckets=1;
    for(int i=0;i<n_last_digits;i++){
        numofbuckets*=2;
    }
    rels = init_relations(&numofrels);
    char* line=NULL;
    size_t len=0;
    while(getline(&line, &len, stdin) != -1){
        if(line[0]=='F')
            break;
        SQLquery* query=new SQLquery();
        query->parser(line);
        categoriser(query,rels);
        delete(query);
    }       
    free(line);
}