# JOIN ORDERING
## Running the code
```
g++ -fdiagnostics-color -std=c++11 -std=c++14 -std=c++17 -O3 -Wall -Werror -Wextra join_order_compare.cpp -o join_order_comp.out
./join_order_comp.out
```
## Results
### IKKBZ Optimizer:

IKKBZ Join Order: A B C D E F 

Optimization Time: 0.00975 ms

Estimated Join Cost: 1.5e-05

Join Execution Time: 27.8765 ms

Final Result Size: 196930 records

### Random Optimizer: (Baseline 1)

Random Join Order: F A E D C B 

Optimization Time: 0.004084 ms

Estimated Join Cost: 0.0015

Join Execution Time: 65.1615 ms

Final Result Size: 196930 records

### Greedy Optimizer: (Baseline 2)

Greedy Join Order: D E F A B C 

Optimization Time: 0.006625 ms

Estimated Join Cost: 7.5e-05

Join Execution Time: 24.6355 ms

Final Result Size: 196930 records

### Dynamic Programming Optimizer:

DP Join Order: F E D C B A 

Optimization Time: 0.028167 ms

Estimated Join Cost: 1.5e-05

Join Execution Time: 30.1943 ms

Final Result Size: 196930 records