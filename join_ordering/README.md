# JOIN ORDERING
## Results
### IKKBZ Optimizer:

IKKBZ Join Order: A B C D E F 

Optimization Time: 0.009167 ms

Estimated Join Cost: 1.5e-05

Join Execution Time: 42.414 ms

Final Result Size: 185535 records

### Random Optimizer: (Baseline 1)

Random Join Order: A F C B E D 

Optimization Time: 0.002833 ms

Estimated Join Cost: 0.0075

Join Execution Time: 32.7414 ms

Final Result Size: 185535 records

### Greedy Optimizer: (Baseline 2)

Greedy Join Order: D E F A B C 

Optimization Time: 0.00475 ms

Estimated Join Cost: 7.5e-05

Join Execution Time: 38.678 ms

Final Result Size: 360935 records

### Dynamic Programming Optimizer:

DP Join Order: F E D C B A 

Optimization Time: 0.0325 ms

Estimated Join Cost: 1.5e-05

Join Execution Time: 35.7727 ms

Final Result Size: 276158 records