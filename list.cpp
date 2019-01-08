#include "list.h"
#include <iostream>
#include <string.h>

using namespace std;

listnode::listnode(){
    next = NULL;
    tuples = (result*)malloc(bufsize);
    memset(tuples,0,bufsize);
}

void listnode::add(int tupleCount,int num1,int num2){
    int index = tupleCount % (bufsize/sizeof(result));
    tuples[index].rowId1 = num1;
    tuples[index].rowId2 = num2;
}

void list::add(int num1,int num2){
    listnode *temp;
    if(head==NULL){//first add to list
        head = new listnode();
        temp = head;
        tail = head;
    }
    else{   
        if(tupleCount % (bufsize/sizeof(result)) == 0){//buffer is full create a new node
            tail->next = new listnode();
            tail = tail->next;
        }
    }

    tail->add(tupleCount,num1,num2);
    tupleCount++;
}

void list::print(){
    cout << "Printing result list..." << endl;
    int sum=0,limit;
    listnode *temp = head;
    while(temp!=NULL){
        sum+=bufsize/sizeof(result);
        limit = bufsize/sizeof(result);
        if(sum > tupleCount){
            limit = tupleCount % (bufsize/sizeof(result));
        }
        for(int i=0;i<limit;i++){
            cout << temp->tuples[i].rowId1 << " , " << temp->tuples[i].rowId2 << endl;
        }
        temp = temp->next;
        cout << "---end of buffer---" << endl;
    }
}

list::~list(){
    // free listnodes
    listnode *temp = head,*next;
    while(temp!=NULL){
        next = temp->next;
        delete temp;
        temp = next;
    }
}