## Observations
Comparing a 3 strategies for in memory operations.
The learned index is a simple linear regression for now for motivating the example
* Learned index + lineage search : Just plain bad. Difference of order of multiple magnitudes to simple binary serach
* Simple binary search stats:
Binary Search:
  Total operations: 19422
  Avg operations per search: 19.42
  Total time: 0.26 ms
  Avg time per search: 0.00 ms

* Learned Index + Binary Search: Hald the operations and about 2/3rd of the time to that of a binary search
Learned Index (Simple linear regression + linear search):
  Total operations: 9242
  Avg operations per search: 9.24
  Total time: 0.19 ms
  Avg time per search: 0.00 ms