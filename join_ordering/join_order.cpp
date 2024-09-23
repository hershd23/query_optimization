#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <limits>
#include <cmath>
#include <random>
#include <chrono>

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

    void addRelation(const Relation& relation) {
        relations.push_back(relation);
        adjacencyList[relation.name] = {};
    }

    void addJoinCondition(const JoinCondition& condition) {
        conditions.push_back(condition);
        adjacencyList[condition.left].push_back(condition.right);
        adjacencyList[condition.right].push_back(condition.left);
    }

    bool isAcyclic() const {
        std::unordered_set<std::string> visited;
        std::string start = relations[0].name;
        return !hasCycle(start, "", visited);
    }

private:
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

class IKKBZOptimizer {
public:
    static std::vector<std::string> optimizeJoinOrder(const JoinGraph& graph) {
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

private:
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
        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::string> optimizedOrder = IKKBZOptimizer::optimizeJoinOrder(graph);
        
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> optimizationTime = end - start;

        std::cout << "Optimized Join Order: ";
        for (const auto& relation : optimizedOrder) {
            std::cout << relation << " ";
        }
        std::cout << std::endl;

        std::cout << "Optimization Time: " << optimizationTime.count() << " ms" << std::endl;

        // Perform the actual join based on the optimized order
        start = std::chrono::high_resolution_clock::now();

        std::vector<Record> result = graph.relations[0].records;
        for (size_t i = 1; i < optimizedOrder.size(); ++i) {
            auto it = std::find_if(graph.relations.begin(), graph.relations.end(),
                                   [&](const Relation& r) { return r.name == optimizedOrder[i]; });
            result = performJoin(result, it->records);
        }

        end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> joinTime = end - start;

        std::cout << "Join Execution Time: " << joinTime.count() << " ms" << std::endl;
        std::cout << "Final Result Size: " << result.size() << " records" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}