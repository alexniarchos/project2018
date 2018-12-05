CC = g++ 
CPPFLAGS = -g -Wall
default: project2 clean
project2: project2.o functions.o join.o parser.o
	$(CC) $(CFLAGS) -o project2 project2.o join.o functions.o parser.o
functions.o: functions.cpp functions.h
	$(CC) $(CFLAGS) -c functions.cpp
join.o: join.cpp join.h
	$(CC) $(CFLAGS) -c join.cpp
parser.o: parser.cpp parser.h
	$(CC) $(CFLAGS) -c parser.cpp
project2.o: project2.cpp functions.h
	$(CC) $(CFLAGS) -c project2.cpp
clean:
	rm *.o