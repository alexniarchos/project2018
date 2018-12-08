#include "parser.h"

using namespace std;

void SQLquery::parserelations(char* stringrelations){
    char* nextentry;
    while((nextentry=strtok_r(stringrelations," ",&stringrelations))){
        int entry=0;
        sscanf(nextentry,"%d",&entry);
        relations.push_back(entry);
    }
}
void SQLquery::parsepredicates(char* stringpredicates){
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
            int* entry=new int[5]{relations[r1],c1,symbol,r2,-1};
            predicates.push_back(entry);
        }
        else{
            int* entry=new int[5]{relations[r1],c1,symbol,relations[r2],c2};
            predicates.push_back(entry);
        }
        free(csymbol);
    }
}
void SQLquery::parseviews(char* stringviews){
    char* nextentry;
    while((nextentry=strtok_r(stringviews," ",&stringviews))){
        char* temp;
        int r,c;
        temp=strtok_r(nextentry,".",&nextentry);
        sscanf(temp,"%d",&r);
        temp=strtok_r(nextentry," \n",&nextentry);
        sscanf(temp,"%d",&c);
        int* entry=new int[2]{relations[r],c};
        views.push_back(entry);

    }
}
int SQLquery::parser(char* query){
    char *views,*relations,*predicates,*temp;
    relations=strtok_r(query,"|",&temp);
    predicates=strtok_r(temp,"|",&temp);
    views=temp;
    parserelations(relations);
    parsepredicates(predicates);
    parseviews(views);
}