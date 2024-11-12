CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pedantic
LDFLAGS = -shared -fPIC

HEADERS = schema.h parser.h planner.h executor.h

all: libdataloader.so libparser.so main

libdataloader.so: dataloader.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<

libparser.so: parser.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< -L. -ldataloader

main: main.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $@ $< -L. -ldataloader -lparser -Wl,-rpath,.

clean:
	rm -f libdataloader.so libparser.so main

run: all
	LD_LIBRARY_PATH=. ./main

debug: CXXFLAGS += -g
debug: clean all

.PHONY: all clean run debug