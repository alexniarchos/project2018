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

<<<<<<< HEAD
void categoriser(SQLquery* query);
void none_of_two_in_midresults(int r0,int c0, int r1,int c1,vector<midResult*> midresults,relation** rels);
=======
void categoriser(SQLquery* query,relation **rels);
int checkfilter(SQLquery* query);
>>>>>>> 8bc96828d6cf1d34cca90083519031dec94e5d89
