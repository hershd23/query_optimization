#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <limits>
#include <cmath>
#include <random>
#include <chrono>
#include <bitset>
#include <cstring>

const int MAX_RELATIONS = 16;

struct Record {
    int id;
    std::string data;
};

struct Relation {
    std::string name;
    int size;
    std::vector<Record> records;
};

struct JoinCondition {
    std::string left;
    std::string right;
    double selectivity;
};

class JoinGraph {
public:
    std::vector<Relation> relations;
    std::vector<JoinCondition> conditions;
    std::unordered_map<std::string, std::vector<std::string>> adjacencyList;

    /**
     * Adds a relation to the join graph.
     *
     * @param relation the relation to be added
     *
     * @return none
     *
     * @throws none
     */
    void addRelation(const Relation& relation) {
        relations.push_back(relation);
        adjacencyList[relation.name] = {};
    }

    /**
     * Adds a join condition to the join graph, updating the adjacency list accordingly.
     *
     * @param condition the join condition to be added
     *
     * @return none
     *
     * @throws none
     */
    void addJoinCondition(const JoinCondition& condition) {
        conditions.push_back(condition);
        adjacencyList[condition.left].push_back(condition.right);
        adjacencyList[condition.right].push_back(condition.left);
    }

    /**
     * Checks if the join graph is acyclic.
     *
     * @return true if the join graph is acyclic, false otherwise
     */
    bool isAcyclic() const {
        std::unordered_set<std::string> visited;
        std::string start = relations[0].name;
        return !hasCycle(start, "", visited);
    }

private:
    /**
     * Checks if a cycle exists in the join graph starting from the given node.
     *
     * @param current the current node being visited
     * @param parent the parent node of the current node
     * @param visited a set of nodes that have been visited
     *
     * @return true if a cycle is detected, false otherwise
     *
     * @throws none
     */
    bool hasCycle(const std::string& current, const std::string& parent, std::unordered_set<std::string>& visited) const {
        visited.insert(current);

        for (const auto& neighbor : adjacencyList.at(current)) {
            if (neighbor == parent) continue;
            if (visited.count(neighbor) > 0 || hasCycle(neighbor, current, visited)) {
                return true;
            }
        }

        return false;
    }
};

class JoinOptimizer {
public:
    /**
     * Generates an optimized join order for the given join graph using the IKKBZ algorithm.
     *
     * @param graph the join graph to optimize
     *
     * @return a vector of relation names representing the optimized join order
     *
     * @throws std::runtime_error if the join graph is not acyclic
     */
    static std::vector<std::string> ikkbzOptimize(const JoinGraph& graph) {
        if (!graph.isAcyclic()) {
            throw std::runtime_error("IKKBZ requires an acyclic join graph");
        }

        std::vector<std::string> joinOrder;
        std::unordered_set<std::string> processed;
        std::string start = findBestStartingRelation(graph);

        joinOrder.push_back(start);
        processed.insert(start);

        while (joinOrder.size() < graph.relations.size()) {
            std::string next = findNextBestRelation(graph, joinOrder.back(), processed);
            joinOrder.push_back(next);
            processed.insert(next);
        }

        return joinOrder;
    }

    /**
     * Generates a randomized join order for the given join graph.
     *
     * @param graph the join graph to optimize
     *
     * @return a vector of relation names representing the randomized join order
     *
     * @throws none
     */
    static std::vector<std::string> randomOptimize(const JoinGraph& graph) {
        std::vector<std::string> order;
        for (const auto& relation : graph.relations) {
            order.push_back(relation.name);
        }
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(order.begin(), order.end(), g);
        return order;
    }

    /**
     * Generates a join order for the given join graph using a greedy approach.
     *
     * @param graph the join graph to optimize
     *
     * @return a vector of relation names representing the optimized join order
     *
     * @throws none
     */
    static std::vector<std::string> greedyOptimize(const JoinGraph& graph) {
        std::vector<std::string> order;
        std::unordered_set<std::string> remaining;
        for (const auto& relation : graph.relations) {
            remaining.insert(relation.name);
        }

        // Start with the smallest relation
        auto start = std::min_element(graph.relations.begin(), graph.relations.end(),
            [](const Relation& a, const Relation& b) { return a.size < b.size; });
        order.push_back(start->name);
        remaining.erase(start->name);

        while (!remaining.empty()) {
            double best_selectivity = std::numeric_limits<double>::max();
            std::string best_next;

            for (const auto& condition : graph.conditions) {
                if (order.back() == condition.left && remaining.count(condition.right)) {
                    if (condition.selectivity < best_selectivity) {
                        best_selectivity = condition.selectivity;
                        best_next = condition.right;
                    }
                } else if (order.back() == condition.right && remaining.count(condition.left)) {
                    if (condition.selectivity < best_selectivity) {
                        best_selectivity = condition.selectivity;
                        best_next = condition.left;
                    }
                }
            }

            if (best_next.empty()) {
                // If no direct join found, add the smallest remaining relation
                auto next = std::min_element(graph.relations.begin(), graph.relations.end(),
                    [&remaining](const Relation& a, const Relation& b) {
                        return a.size < b.size && remaining.count(a.name);
                    });
                best_next = next->name;
            }

            order.push_back(best_next);
            remaining.erase(best_next);
        }

        return order;
    }

    /**
     * This function implements a dynamic programming approach to optimize the join order in a query plan.
     *
     * @param graph The join graph representing the query plan.
     *
     * @return The optimal join order as a vector of relation names.
     *
     * @throws std::runtime_error If the number of relations exceeds the maximum allowed by the DP optimizer.
     */
    static std::vector<std::string> dpOptimize(const JoinGraph& graph) {
        int n = graph.relations.size();
        if (n > MAX_RELATIONS) {
            throw std::runtime_error("DP optimizer cannot handle more than " + std::to_string(MAX_RELATIONS) + " relations");
        }

        std::vector<std::vector<double>> cost(1 << n, std::vector<double>(n, std::numeric_limits<double>::max()));
        std::vector<std::vector<int>> parent(1 << n, std::vector<int>(n, -1));

        // Initialize base cases (single relations)
        for (int i = 0; i < n; ++i) {
            cost[1 << i][i] = graph.relations[i].size;
        }

        // Iterate over all subsets of relations
        for (int subset = 1; subset < (1 << n); ++subset) {
            for (int i = 0; i < n; ++i) {
                if (!(subset & (1 << i))) continue;

                int prev_subset = subset ^ (1 << i);
                if (prev_subset == 0) continue;

                for (int j = 0; j < n; ++j) {
                    if (!(prev_subset & (1 << j))) continue;

                    double join_cost = cost[prev_subset][j];
                    auto it = std::find_if(graph.conditions.begin(), graph.conditions.end(),
                        [&](const JoinCondition& c) {
                            return (c.left == graph.relations[i].name && c.right == graph.relations[j].name) ||
                                (c.right == graph.relations[i].name && c.left == graph.relations[j].name);
                        });

                    if (it != graph.conditions.end()) {
                        join_cost *= it->selectivity;
                    } else {
                        join_cost *= 1.0;  // Penalize cross product
                    }

                    if (join_cost < cost[subset][i]) {
                        cost[subset][i] = join_cost;
                        parent[subset][i] = j;
                    }
                }
            }
        }

        // Find the optimal final join
        int final_subset = (1 << n) - 1;
        int last_joined = std::min_element(cost[final_subset].begin(), cost[final_subset].end()) - cost[final_subset].begin();

        // Reconstruct the optimal join order
        std::vector<std::string> order;
        int current_subset = final_subset;
        int current_relation = last_joined;
        while (current_subset > 0) {
            order.push_back(graph.relations[current_relation].name);
            int next_relation = parent[current_subset][current_relation];
            current_subset ^= (1 << current_relation);
            current_relation = next_relation;
        }
        std::reverse(order.begin(), order.end());

        return order;
    }

private:
    /**
     * Finds the relation with the lowest estimated cost in the given join graph.
     *
     * @param graph The join graph to search for the best starting relation.
     *
     * @return The name of the relation with the lowest estimated cost.
     */
    static std::string findBestStartingRelation(const JoinGraph& graph) {
        std::string best;
        double minCost = std::numeric_limits<double>::max();

        for (const auto& relation : graph.relations) {
            double cost = estimateCost(graph, relation.name);
            if (cost < minCost) {
                minCost = cost;
                best = relation.name;
            }
        }

        return best;
    }

    /**
     * Finds the next best relation to join in the given join graph, 
     * starting from the current relation and excluding already processed relations.
     *
     * @param graph The join graph to search for the next best relation.
     * @param current The name of the current relation.
     * @param processed A set of relation names that have already been processed.
     *
     * @return The name of the next best relation to join.
     */
    static std::string findNextBestRelation(const JoinGraph& graph, const std::string& current, const std::unordered_set<std::string>& processed) {
        std::string best;
        double minCost = std::numeric_limits<double>::max();

        for (const auto& neighbor : graph.adjacencyList.at(current)) {
            if (processed.count(neighbor) > 0) continue;

            double cost = estimateCost(graph, neighbor);
            if (cost < minCost) {
                minCost = cost;
                best = neighbor;
            }
        }

        return best;
    }

    /**
     * Estimates the cost of a relation in a join graph.
     *
     * @param graph The join graph to estimate the cost from.
     * @param relation The name of the relation to estimate the cost for.
     *
     * @return The estimated cost of the relation.
     *
     * @throws std::runtime_error If the relation is not found in the graph.
     */
    static double estimateCost(const JoinGraph& graph, const std::string& relation) {
        auto it = std::find_if(graph.relations.begin(), graph.relations.end(),
                               [&relation](const Relation& r) { return r.name == relation; });
        
        if (it == graph.relations.end()) {
            throw std::runtime_error("Relation not found in graph");
        }

        double cost = it->size;

        // Consider join selectivity and relation sizes
        for (const auto& condition : graph.conditions) {
            if (condition.left == relation || condition.right == relation) {
                const auto& other = (condition.left == relation) ? condition.right : condition.left;
                auto other_it = std::find_if(graph.relations.begin(), graph.relations.end(),
                                             [&other](const Relation& r) { return r.name == other; });
                
                cost *= condition.selectivity * other_it->size;
            }
        }

        // Factor in the number of attributes (assuming more attributes increase cost)
        cost *= std::log(it->records[0].data.size());

        return cost;
    }
};

/**
 * Generates a vector of random records with a specified size and prefix.
 *
 * @param size The number of records to generate.
 * @param prefix The prefix to be added to each record.
 *
 * @return A vector of random records.
 */
std::vector<Record> generateRandomRecords(int size, const std::string& prefix) {
    std::vector<Record> records;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, size * 10);

    for (int i = 0; i < size; ++i) {
        records.push_back({dis(gen), prefix + std::to_string(i)});
    }
    return records;
}

/**
 * Performs an inner join on two vectors of records based on matching IDs.
 *
 * @param left The left vector of records to join.
 * @param right The right vector of records to join.
 *
 * @return A vector of joined records, where each record contains the matching ID and concatenated data.
 */
std::vector<Record> performJoin(const std::vector<Record>& left, const std::vector<Record>& right) {
    std::vector<Record> result;
    for (const auto& l : left) {
        for (const auto& r : right) {
            if (l.id == r.id) {
                result.push_back({l.id, l.data + "-" + r.data});
            }
        }
    }
    return result;
}

/**
 * Estimates the cost of a join operation based on the provided join graph and order.
 *
 * @param graph The join graph containing the conditions for the join operation.
 * @param order The order in which the joins should be performed.
 *
 * @return The estimated cost of the join operation.
 */
double estimateJoinCost(const JoinGraph& graph, const std::vector<std::string>& order) {
    double cost = 1.0;
    for (size_t i = 1; i < order.size(); ++i) {
        auto it = std::find_if(graph.conditions.begin(), graph.conditions.end(),
            [&](const JoinCondition& c) {
                return (c.left == order[i-1] && c.right == order[i]) ||
                       (c.right == order[i-1] && c.left == order[i]);
            });
        if (it != graph.conditions.end()) {
            cost *= it->selectivity;
        } else {
            // Penalize for cross product
            cost *= 1.0;
        }
    }
    return cost;
}

/**
 * Runs the given join graph optimization function and measures its execution time.
 * Prints the join order, optimization time, estimated join cost, join execution time,
 * and final result size.
 *
 * @param graph The join graph containing the relations and conditions.
 * @param strategy The name of the optimization strategy being used.
 * @param optimizeFunc A function pointer to the optimization function to be executed.
 *
 * @return void
 *
 * @throws None
 */
void runAndMeasure(const JoinGraph& graph, const std::string& strategy, 
                   std::vector<std::string> (*optimizeFunc)(const JoinGraph&)) {
    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::string> order = optimizeFunc(graph);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> optimizationTime = end - start;

    std::cout << strategy << " Join Order: ";
    for (const auto& relation : order) {
        std::cout << relation << " ";
    }
    std::cout << std::endl;

    std::cout << "Optimization Time: " << optimizationTime.count() << " ms" << std::endl;

    double estimatedCost = estimateJoinCost(graph, order);
    std::cout << "Estimated Join Cost: " << estimatedCost << std::endl;

    start = std::chrono::high_resolution_clock::now();
    std::vector<Record> result = graph.relations[0].records;
    for (size_t i = 1; i < order.size(); ++i) {
        auto it = std::find_if(graph.relations.begin(), graph.relations.end(),
                               [&](const Relation& r) { return r.name == order[i]; });
        result = performJoin(result, it->records);
    }
    end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> joinTime = end - start;

    std::cout << "Join Execution Time: " << joinTime.count() << " ms" << std::endl;
    std::cout << "Final Result Size: " << result.size() << " records" << std::endl;
    std::cout << std::endl;
}

int main() {
    JoinGraph graph;

    // Create a more complex join graph
    graph.addRelation({"A", 10000, generateRandomRecords(10000, "A")});
    graph.addRelation({"B", 15000, generateRandomRecords(15000, "B")});
    graph.addRelation({"C", 20000, generateRandomRecords(20000, "C")});
    graph.addRelation({"D", 5000, generateRandomRecords(5000, "D")});
    graph.addRelation({"E", 25000, generateRandomRecords(25000, "E")});
    graph.addRelation({"F", 8000, generateRandomRecords(8000, "F")});

    graph.addJoinCondition({"A", "B", 0.1});
    graph.addJoinCondition({"B", "C", 0.05});
    graph.addJoinCondition({"C", "D", 0.2});
    graph.addJoinCondition({"D", "E", 0.15});
    graph.addJoinCondition({"E", "F", 0.1});

    try {
        std::cout << "IKKBZ Optimizer:" << std::endl;
        runAndMeasure(graph, "IKKBZ", JoinOptimizer::ikkbzOptimize);

        std::cout << "Random Optimizer:" << std::endl;
        runAndMeasure(graph, "Random", JoinOptimizer::randomOptimize);

        std::cout << "Greedy Optimizer:" << std::endl;
        runAndMeasure(graph, "Greedy", JoinOptimizer::greedyOptimize);

        std::cout << "Dynamic Programming Optimizer:" << std::endl;
        runAndMeasure(graph, "DP", JoinOptimizer::dpOptimize);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}