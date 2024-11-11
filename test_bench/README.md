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