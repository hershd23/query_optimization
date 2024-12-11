# Query Optimization Workbench

A query optimization workbench that implements various join ordering strategies and cost-based optimization for database queries.

## Overview

This workbench provides different query planning strategies:
- Joins First: Executes all joins before filters
- Filters First: Executes all filters before joins
- Try All Join Orders: Exhaustively tries all possible join orders (with filters first)
- Greedy Join Order: Uses a greedy approach to select join orders
- Dynamic Programming: Uses DP to build optimal plans from smaller subplans

## Query Format

Queries are written in a simple, structured format:
```
query_start
tables: table1, table2, ...
scalar_filters: table.column=value, ...
dynamic_filters: table1.column=table2.column, ...
joins: table1.column=table2.column, ...
query_end
```

## Example Queries

1. Find director for a specific movie:
```
query_start
tables: movie, director, movie_director
scalar_filters: movie.id=8854
joins: movie_director.did=director.id, movie.id=movie_director.mid
query_end
```

2. Find all male actors who acted in movies directed by Spielberg after 2000:
```
query_start
tables: movie, director, movie_director, actor, casts
scalar_filters: director.lname=Spielberg, movie.year>2000, actor.gender=M
joins: movie.id=movie_director.mid, movie_director.did=director.id, movie.id=casts.mid, casts.pid=actor.id
query_end
```

3. Find all movies directed by Nolan in the Drama genre:
```
query_start
tables: movie, director, movie_director, genre
scalar_filters: director.lname=Nolan, genre.genre=Drama
joins: movie.id=movie_director.mid, movie_director.did=director.id, movie.id=genre.mid
query_end
```

## Cost-Based Optimization

The workbench uses several metrics for cost estimation:
- Table sizes and selectivity factors from histograms
- I/O costs for scanning tables
- CPU costs for comparisons
- Join output size estimation

For joins, the output size is estimated as:

output_size = min(left_table_size, right_table_size)
text

## Plan Generation

Each planning strategy generates:
- Detailed cost estimates for each operation
- Selectivity factors
- Output sizes after each operation
- Total plan cost
- Plan generation time

The planner then selects the best plan based on the estimated total cost.

## Schema

The workbench uses the IMDB dataset schema:
```sql
actor(id int, fname string, lname string, gender string)
movie(id int, name string, year int)
director(id int, fname string, lname string)
casts(pid int, mid int, role string)
movie_director(did int, mid int)
genre(mid int, genre string)
```

## Building and Running
```
make
./main
```

## Example: How the outputs look like

```
Enter your query (type 'quit' alone on a line to exit):
query_start
tables: movie, director, movie_director
scalar_filters: movie.id=8854
joins: movie_director.did=director.id, movie.id=movie_director.mid
query_end

=== Query Components ===
Tables:
  Table: movie
  Table: director
  Table: movie_director

Scalar Filters:
  Scalar Filter: movie.id = 8854

Dynamic Filters:
  (none)

Joins:
  Join: movie_director.did = director.id
  Join: movie.id = movie_director.mid
=====================

Generating query plans...

=== Plan Generation Summary ===

Plan Type: JoinsFirst
Generation Time: 0.0510006 ms

=== Joins First Plan ===
Estimating costs for joins-first strategy:
Estimating join costs:
  Join movie_director.did = director.id (Cost: 63316102.400000, Selectivity: 0.714233, Output size: 21257)
  Join movie.id = movie_director.mid (Cost: 56191657.400000, Selectivity: 0.804824, Output size: 21257)
Estimating filter costs:
  Filter movie.id (Cost: 26433.000000, Selectivity: 0.000795, Output size: 16)
Total Estimated Cost: 1.19534e+08
=====================

Plan Type: FiltersFirst
Generation Time: 0.0299997 ms

=== Filters First Plan ===
Estimating costs for filters-first strategy:
Estimating filter costs:
  Filter movie.id (Cost: 26433.000000, Selectivity: 0.000795, Output size: 21)
Estimating join costs:
  Join movie_director.did = director.id (Cost: 63316102.400000, Selectivity: 0.714233, Output size: 21257)
  Join movie.id = movie_director.mid (Cost: 65917.700000, Selectivity: 0.000988, Output size: 21)
Total Estimated Cost: 6.34085e+07
=====================

Plan Type: TryAllJoinOrderPlan
Generation Time: 0.103001 ms

=== Try All Join Order Plan ===
Estimating costs for optimal-join-order strategy:
Estimating filter costs:
  Filter movie.id (Cost: 26433.000000, Selectivity: 0.000795, Output size: 21)
Trying all possible join orders:
Best join order found all permutations (Cost: 158200.900000):
  Join movie.id = movie_director.mid (Cost: 92283.200000, Selectivity: 0.000706, Output size: 21)
  Join movie_director.did = director.id (Cost: 65917.700000, Selectivity: 0.000988, Output size: 21)
Total Estimated Cost: 184634
============================

Plan Type: GreedyJoinPlan
Generation Time: 0.0640001 ms

=== Greedy Join Plan ===
Estimating costs for greedy join strategy:
Estimating filter costs:
  Filter movie.id (Cost: 26433.000000, Selectivity: 0.000795, Output size: 21)
Estimating join costs (greedy strategy):
  Join movie.id = movie_director.mid (Cost: 92283.200000, Selectivity: 0.000706, Output size: 21)
  Join movie_director.did = director.id (Cost: 65917.700000, Selectivity: 0.000988, Output size: 21)
Total Estimated Cost: 184634
=====================

Plan Type: DPJoinPlan
Generation Time: 0.276 ms

=== Dynamic Programming Join Plan ===
Estimating costs for dynamic programming join strategy:
Estimating filter costs:
  Filter movie.id (Cost: 26433.000000, Selectivity: 0.000795, Output size: 21)
Estimating join costs (dynamic programming):
Best join order found:
Join movie.id = movie_director.mid (Cost: 92283.200000, Selectivity: 0.000706, Output size: 21)
Join movie_director.did = director.id (Cost: 63316102.400000, Selectivity: 0.714233, Output size: 21257)
Total Estimated Cost: 6.34084e+07
==============================
===========================

=== Executing All Plans ===

Executing JoinsFirst Plan:

Executing query...
Joining movie_director and director
Joined table size: 29762 rows
Joining movie and movie_director
Joined table size: 29762 rows
Applying filter on movie.id
Filtered table size: 1 rows

Query execution completed. Found 1 rows.

Results have been written to output/results.txt

Executing FiltersFirst Plan:

Executing query...
Applying filter on movie.id
Filtered table size: 1 rows
Joining movie_director and director
Joined table size: 29762 rows
Joining movie and movie_director
Joined table size: 1 rows

Query execution completed. Found 29762 rows.

Results have been written to output/results.txt

Executing TryAllJoinOrder Plan:

Executing query...
Applying filter on movie.id
Filtered table size: 1 rows
Joining movie and movie_director
Joined table size: 1 rows
Joining movie_director and director
Joined table size: 1 rows

Query execution completed. Found 1 rows.

Results have been written to output/results.txt

Executing GreedyJoin Plan:

Executing query...
Applying filter on movie.id
Filtered table size: 1 rows
Joining movie and movie_director
Joined table size: 1 rows
Joining movie_director and director
Joined table size: 1 rows

Query execution completed. Found 1 rows.

Results have been written to output/results.txt

Executing DPJoin Plan:

Executing query...
Applying filter on movie.id
Filtered table size: 1 rows

Query execution completed. Found 1 rows.

Results have been written to output/results.txt

=== Execution Time Summary ===
JoinsFirst Plan: 28473.2 ms
FiltersFirst Plan: 12987.6 ms
TryAllJoinOrder Plan: 3.96 ms
GreedyJoin Plan: 3.775 ms
DPJoin Plan: 2.312 ms
```