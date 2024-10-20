# Makefile

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pedantic
LDFLAGS = -shared -fPIC

all: libdataloader.so libparser.so main

libdataloader.so: dataloader.cpp schema.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<

libparser.so: parser.cpp schema.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<

main: main.cpp schema.h
	$(CXX) $(CXXFLAGS) -o $@ $< -L. -ldataloader -lparser -Wl,-rpath,.

clean:
	rm -f libdataloader.so libparser.so main

run: all
	./main

debug: CXXFLAGS += -g
debug: clean all

.PHONY: all clean run debug