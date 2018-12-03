#include <cstdio>
#include "string.h"
#include <cstdlib>
#include <iostream>
#include <vector>

using namespace std;

class SQLquery{
    vector<int*> views; //has 2 values || for example r0.c1 will be 0 1
    vector<int> relations;
    vector<int*> predicates; //has 5 values and if it is a filter the 5th value is -1|| for example r0.c1=r1.c2 values will be 0 1 0 1 2 and at this r0.c1>3000 0 1 1 3000 -1

    void parserelations(char* stringrelations){
        char* nextentry;
        while((nextentry=strtok_r(stringrelations," ",&stringrelations))){
            int entry=0;
            sscanf(nextentry,"%d",&entry);
            relations.push_back(entry);
        }
    }
    void parsepredicates(char* stringpredicates){
        char* nextentry;
        char* temp;
        while((nextentry=strtok_r(stringpredicates,"&",&stringpredicates))){
            int r1,r2,c1,c2;
            char* csymbol=(char*)malloc(2*sizeof(char));
            csymbol[1]='\0';
            int symbol;
            int i=0;
            int filterflag=0;
            while(nextentry[i]!='\0'){
                if(nextentry[i]=='='){
                    csymbol[0]=nextentry[i];
                    symbol=0;
                    break;
                }
                if(nextentry[i]=='>'){
                    csymbol[0]=nextentry[i];
                    symbol=1;
                    break;
                }
                if(nextentry[i]=='<'){
                    csymbol[0]=nextentry[i];
                    symbol=2;
                    break;
                }
                i++;
            }
            temp=strtok_r(nextentry,".",&nextentry);
            sscanf(temp,"%d",&r1);
            temp=strtok_r(nextentry,csymbol,&nextentry);
            sscanf(temp,"%d",&c1);
            temp=strtok_r(nextentry,".",&nextentry);
            sscanf(temp,"%d",&r2);
            temp=strtok_r(nextentry,"",&nextentry);
            if(temp!=NULL)
                sscanf(temp,"%d",&c2);
            else
                filterflag=1;
            
            if(filterflag){
                int entry[5]={r1,c1,symbol,r2,-1};
                predicates.push_back(entry);
            }
            else{
                int entry[5]={r1,c1,symbol,r2,c2};
                cout<<entry[0]<<" "<<entry[1]<<" "<<entry[2]<<" "<<entry[3]<<" "<<entry[4]<<" "<<endl;
                predicates.push_back(entry);
            }
            free(csymbol);
        }
    }
    void parseviews(char* stringviews){
        char* nextentry;
        while((nextentry=strtok_r(stringviews," ",&stringviews))){
            char* temp;
            int r,c;
            temp=strtok_r(nextentry,".",&nextentry);
            sscanf(temp,"%d",&r);
            temp=strtok_r(nextentry," \n",&nextentry);
            sscanf(temp,"%d",&c);
            int entry[2]={r,c};
            cout<<entry[0]<<" "<<entry[1]<<endl;
            views.push_back(entry);

        }
    }

    public:
        SQLquery(){};
        int parser(char* query){
            char *views,*relations,*predicates,*temp;
            relations=strtok_r(query,"|",&temp);
            predicates=strtok_r(temp,"|",&temp);
            views=temp;
            parserelations(relations);
            parsepredicates(predicates);
            parseviews(views);
        }

};


int main(void){
    char* line=NULL;
    size_t len=0;
    while(getline(&line, &len, stdin) != -1){
        if(line[0]=='F')
            break;
        SQLquery* query=new SQLquery();
        query->parser(line);
        delete(query);
    }
    free(line);
    
}