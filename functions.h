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
#include "parser.h"
#include "join.h"

void categoriser(SQLquery* query,relation **rels);
int checkfilter(SQLquery* query);
void none_of_two_in_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults);
void both_in_diff_midresults(SQLquery* query,int index,relation** rels,vector<midResult*> &midresults);
int checkmidresults(int rel_index,vector<midResult*> &midresults);