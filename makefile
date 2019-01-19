CC = g++ 
CPPFLAGS = -g
default: project clean
project: project.o functions.o join.o parser.o job.o list.o
	$(CC) $(CPPFLAGS) -o ./submission/build/release/project project.o join.o functions.o parser.o job.o list.o -pthread -O3
functions.o: functions.cpp functions.h
	$(CC) $(CPPFLAGS) -c functions.cpp
join.o: join.cpp join.h
	$(CC) $(CPPFLAGS) -c join.cpp
parser.o: parser.cpp parser.h
	$(CC) $(CPPFLAGS) -c parser.cpp
job.o: job.cpp job.h
	$(CC) $(CPPFLAGS) -c job.cpp
list.o: list.cpp list.h
	$(CC) $(CPPFLAGS) -c list.cpp
project2.o: project.cpp functions.h
	$(CC) $(CPPFLAGS) -c project.cpp
clean:
	rm *.o output