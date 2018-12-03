#include <iostream>
#include <stdlib.h>
#include <ctime>
#include <cstring>
#include <stdio.h>
#include <fstream>
#include <vector>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

void categoriser(SQLquery* query){
    //while loop through predicates
        //find all the filters and put them on front
        //build score array with size the number of non filter predicates at the beggining 
        //and add after each iteration the new relations that have been used
        //put in an array the relations that have been filtered
    //endfor
}

int main(void){
    relation **rels = NULL;
    int numofrels;
    rels = init_relations(&numofrels);
    char* line=NULL;
    size_t len=0;
    while(getline(&line, &len, stdin) != -1){
        if(line[0]=='F')
            break;
        SQLquery* query=new SQLquery();
        query->parser(line);
        categoriser(query);
        delete(query);
    }       
    free(line);
}