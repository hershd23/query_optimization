g++ -std=c++14 -c learned_index.cpp
g++ -std=c++14 -c main.cpp
g++ learned_index.o main.o -o learned_index_program
./learned_index_program