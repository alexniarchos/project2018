CC = g++ 
CPPFLAGS = -g
default: project2 clean
project2: project2.o functions.o join.o parser.o
	$(CC) $(CPPFLAGS) -o project2 project2.o join.o functions.o parser.o
functions.o: functions.cpp functions.h
	$(CC) $(CPPFLAGS) -c functions.cpp
join.o: join.cpp join.h
	$(CC) $(CPPFLAGS) -c join.cpp
parser.o: parser.cpp parser.h
	$(CC) $(CPPFLAGS) -c parser.cpp
project2.o: project2.cpp functions.h
	$(CC) $(CPPFLAGS) -c project2.cpp
clean:
	rm *.o output