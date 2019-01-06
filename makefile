CC = g++ 
CPPFLAGS = -g
default: project2 clean
project2: project2.o functions.o join.o parser.o job.o
	$(CC) $(CPPFLAGS) -o project2 project2.o join.o functions.o parser.o job.o -pthread
functions.o: functions.cpp functions.h
	$(CC) $(CPPFLAGS) -c functions.cpp
join.o: join.cpp join.h
	$(CC) $(CPPFLAGS) -c join.cpp
parser.o: parser.cpp parser.h
	$(CC) $(CPPFLAGS) -c parser.cpp
job.o: job.cpp job.h
	$(CC) $(CPPFLAGS) -c job.cpp
project2.o: project2.cpp functions.h
	$(CC) $(CPPFLAGS) -c project2.cpp
clean:
	rm *.o output