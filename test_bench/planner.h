// planner.h
#pragma once
#include "schema.h"
#include "parser.h"
#include <memory>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <unordered_set>
#include <iostream>

#ifdef __GNUC__
#define UNUSED(x) (void)(x)
#else
#define UNUSED(x)
#endif

struct CostAndSelectivity {
    double cost;
    double selectivity;
    
    CostAndSelectivity(double c, double s) : cost(c), selectivity(s) {}
};

class Plan {
protected:
    Schema* schema;
    QueryComponents components;
    std::vector<std::shared_ptr<Component>> componentExecutionOrder;

    CostAndSelectivity estimateFilterCostAndSelectivity(
        const std::string& tableName, 
        const std::string& column,
        Predicate::Op op,
        const Field& value) {
        
        const double SCAN_COST_FACTOR = 1.0;
        auto table = schema->getTable(tableName);
        
        double selectivity = table->estimateSelectivity(column, op, value);
        size_t inputSize = table->data.size();
        
        double cost = (inputSize * SCAN_COST_FACTOR) + (inputSize * selectivity);
        
        return CostAndSelectivity(cost, selectivity);
    }

    CostAndSelectivity estimateJoinCostAndSelectivity(
        /*
        NOTE: Right now we only assume inner joins on primary keys of two tables
        */
        const std::string& leftTable, 
        const std::string& rightTable,
        const std::string& leftCol,
        const std::string& rightCol,
        size_t leftSize,
        size_t rightSize) {
        
        // Unused variables
        UNUSED(leftTable);
        UNUSED(rightTable);
        UNUSED(leftCol);
        UNUSED(rightCol);
        
        const double CPU_COST_FACTOR = 0.1;
        const double IO_COST_FACTOR = 1.0;
        
        // Use smaller table size for selectivity
        double selectivity = static_cast<double>(std::min(leftSize, rightSize)) / 
                           static_cast<double>(std::max(leftSize, rightSize));
        
        double ioCost = (leftSize + rightSize) * IO_COST_FACTOR;
        double cpuCost = (leftSize * rightSize) * CPU_COST_FACTOR;
        
        return CostAndSelectivity(ioCost + cpuCost, selectivity);
    }

public:
    Plan(Schema* schema, const QueryComponents& components) 
        : schema(schema), components(components) {}
    
    virtual ~Plan() = default;
    virtual void generatePlan() = 0;
    virtual double estimateCost() = 0;
    virtual void printPlan() const = 0;
    const std::vector<std::shared_ptr<Component>>& getExecutionOrder() const {
        return componentExecutionOrder;
    }

    void printExecutionOrder() const {
        std::cout << "\nExecution Order of Components:\n";
        for (const auto& component : componentExecutionOrder) {
            if (auto filter = std::dynamic_pointer_cast<ScalarFilterComponent>(component)) {
                if(filter->rhsValue.getType() == FieldType::STRING) {
                    std::cout << "  Filter: " << filter->lhsTable << "." << filter->lhsColumn 
                         << " " << FilterComponent::predicateToString(filter->predicate) 
                         << " " << filter->rhsValue.getStringValue() << "\n";
                }
                else if(filter->rhsValue.getType() == FieldType::INTEGER) {
                    std::cout << "  Filter: " << filter->lhsTable << "." << filter->lhsColumn 
                         << " " << FilterComponent::predicateToString(filter->predicate) 
                         << " " << filter->rhsValue.getIntValue() << "\n";
                }
            }
            else if (auto join = std::dynamic_pointer_cast<JoinComponent>(component)) {
                std::cout << "  Join: " << join->lhsTable << "." << join->lhsColumn 
                         << " = " << join->rhsTable << "." << join->rhsColumn << "\n";
            }
        }
        std::cout << "------------------------\n";
    }
};

class JoinsFirstPlan : public Plan {
private:
    double totalCost;
    std::vector<std::string> executionSteps;
    std::unordered_map<std::string, size_t> tableSizes;

public:
    JoinsFirstPlan(Schema* schema, const QueryComponents& components) 
        : Plan(schema, components), totalCost(0.0) {}

    void generatePlan() override {
        // Initialize table sizes
        for (const auto& table : components.tables) {
            tableSizes[table->tableName] = schema->getTable(table->tableName)->data.size();
        }

        executionSteps.push_back("Estimating costs for joins-first strategy:");
        
        // Estimate join costs first
        executionSteps.push_back("Estimating join costs:");
        for (const auto& join : components.joins) {
            auto costAndSel = estimateJoinCostAndSelectivity(
                join->lhsTable, join->rhsTable,
                join->lhsColumn, join->rhsColumn,
                tableSizes[join->lhsTable],
                tableSizes[join->rhsTable]);
            
            totalCost += costAndSel.cost;
            
            // Update table sizes after join
            size_t outputSize = static_cast<size_t>(
                std::min(tableSizes[join->lhsTable], tableSizes[join->rhsTable])
            );
            
            tableSizes[join->lhsTable] = outputSize;
            tableSizes[join->rhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Join " + join->lhsTable + "." + join->lhsColumn +
                " = " + join->rhsTable + "." + join->rhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");

            componentExecutionOrder.push_back(join);
        }

        // Then estimate filter costs
        executionSteps.push_back("Estimating filter costs:");
        for (const auto& filter : components.scalarFilters) {
            auto costAndSel = estimateFilterCostAndSelectivity(
                filter->lhsTable,
                filter->lhsColumn,
                filter->predicate,
                filter->rhsValue);
            
            totalCost += costAndSel.cost;
            
            // Update table size after filter
            size_t outputSize = static_cast<size_t>(
                tableSizes[filter->lhsTable] * costAndSel.selectivity);
            tableSizes[filter->lhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Filter " + filter->lhsTable + "." + filter->lhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");

            componentExecutionOrder.push_back(filter);
        }
    }

    double estimateCost() override {
        return totalCost;
    }

    void printPlan() const override {
        std::cout << "\n=== Joins First Plan ===\n";
        for (const auto& step : executionSteps) {
            std::cout << step << "\n";
        }
        std::cout << "Total Estimated Cost: " << totalCost << "\n";
        std::cout << "=====================\n";
    }
};

class FiltersFirstPlan : public Plan {
private:
    double totalCost;
    std::vector<std::string> executionSteps;
    std::unordered_map<std::string, size_t> tableSizes;

public:
    FiltersFirstPlan(Schema* schema, const QueryComponents& components) 
        : Plan(schema, components), totalCost(0.0) {}

    void generatePlan() override {
        // Initialize table sizes
        for (const auto& table : components.tables) {
            tableSizes[table->tableName] = schema->getTable(table->tableName)->data.size();
        }

        executionSteps.push_back("Estimating costs for filters-first strategy:");
        
        // Estimate filter costs first
        executionSteps.push_back("Estimating filter costs:");
        for (const auto& filter : components.scalarFilters) {
            auto costAndSel = estimateFilterCostAndSelectivity(
                filter->lhsTable,
                filter->lhsColumn,
                filter->predicate,
                filter->rhsValue);
            
            totalCost += costAndSel.cost;
            
            // Update table size after filter
            size_t outputSize = static_cast<size_t>(
                tableSizes[filter->lhsTable] * costAndSel.selectivity);
            tableSizes[filter->lhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Filter " + filter->lhsTable + "." + filter->lhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");
            
            componentExecutionOrder.push_back(filter);
        }

        // Then estimate join costs
        executionSteps.push_back("Estimating join costs:");
        for (const auto& join : components.joins) {
            auto costAndSel = estimateJoinCostAndSelectivity(
                join->lhsTable, join->rhsTable,
                join->lhsColumn, join->rhsColumn,
                tableSizes[join->lhsTable],
                tableSizes[join->rhsTable]);
            
            totalCost += costAndSel.cost;
            
            // Update table sizes after join
            size_t outputSize = static_cast<size_t>(
                std::min(tableSizes[join->lhsTable], tableSizes[join->rhsTable])
                );
            
            tableSizes[join->lhsTable] = outputSize;
            tableSizes[join->rhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Join " + join->lhsTable + "." + join->lhsColumn +
                " = " + join->rhsTable + "." + join->rhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");

            componentExecutionOrder.push_back(join);
        }
    }

    double estimateCost() override {
        return totalCost;
    }

    void printPlan() const override {
        std::cout << "\n=== Filters First Plan ===\n";
        for (const auto& step : executionSteps) {
            std::cout << step << "\n";
        }
        std::cout << "Total Estimated Cost: " << totalCost << "\n";
        std::cout << "=====================\n";
    }
};

class TryAllJoinOrderPlan : public Plan {
private:
    double totalCost;
    std::vector<std::string> executionSteps;
    std::unordered_map<std::string, size_t> tableSizes;
    std::vector<std::shared_ptr<Component>> joinComponentExecutionOrder;
    

    // Helper function to generate all possible permutations of joins
    std::vector<std::vector<std::shared_ptr<JoinComponent>>> generateJoinPermutations() {
        std::vector<std::shared_ptr<JoinComponent>> joins = components.joins;
        std::vector<std::vector<std::shared_ptr<JoinComponent>>> result;
        
        std::function<void(std::vector<std::shared_ptr<JoinComponent>>&, int)> permute = 
            [&](std::vector<std::shared_ptr<JoinComponent>>& arr, int start) {
                if (start == (int)arr.size()) {
                    result.push_back(arr);
                    return;
                }
                for (size_t i = start; i < arr.size(); i++) {
                    std::swap(arr[start], arr[i]);
                    permute(arr, start + 1);
                    std::swap(arr[start], arr[i]);
                }
            };
        
        permute(joins, 0);
        return result;
    }

public:
    TryAllJoinOrderPlan(Schema* schema, const QueryComponents& components) 
        : Plan(schema, components), totalCost(0.0) {}

    void generatePlan() override {
        // Initialize table sizes
        for (const auto& table : components.tables) {
            tableSizes[table->tableName] = schema->getTable(table->tableName)->data.size();
        }

        executionSteps.push_back("Estimating costs for optimal-join-order strategy:");
        
        // First apply filters
        executionSteps.push_back("Estimating filter costs:");
        for (const auto& filter : components.scalarFilters) {
            auto costAndSel = estimateFilterCostAndSelectivity(
                filter->lhsTable,
                filter->lhsColumn,
                filter->predicate,
                filter->rhsValue);
            
            totalCost += costAndSel.cost;
            
            // Update table size after filter
            size_t outputSize = static_cast<size_t>(
                tableSizes[filter->lhsTable] * costAndSel.selectivity);
            tableSizes[filter->lhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Filter " + filter->lhsTable + "." + filter->lhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");

            componentExecutionOrder.push_back(filter);
        }

        // Try all possible join orders
        executionSteps.push_back("Trying all possible join orders:");
        auto joinPermutations = generateJoinPermutations();
        
        double bestJoinCost = std::numeric_limits<double>::max();
        std::vector<std::string> bestJoinSteps;
        std::unordered_map<std::string, size_t> bestSizes;
        
        for (const auto& joinOrder : joinPermutations) {
            double currentJoinCost = 0.0;
            std::vector<std::string> currentSteps;
            auto currentSizes = tableSizes;  // Start with sizes after filters
            
            for (const auto& join : joinOrder) {
                auto costAndSel = estimateJoinCostAndSelectivity(
                    join->lhsTable, join->rhsTable,
                    join->lhsColumn, join->rhsColumn,
                    currentSizes[join->lhsTable],
                    currentSizes[join->rhsTable]);
                
                currentJoinCost += costAndSel.cost;
                
                // Update sizes after join
                size_t outputSize = static_cast<size_t>(
                    std::min(currentSizes[join->lhsTable], currentSizes[join->rhsTable])
                );
                
                currentSizes[join->lhsTable] = outputSize;
                currentSizes[join->rhsTable] = outputSize;
                
                currentSteps.push_back(
                    "  Join " + join->lhsTable + "." + join->lhsColumn +
                    " = " + join->rhsTable + "." + join->rhsColumn +
                    " (Cost: " + std::to_string(costAndSel.cost) +
                    ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                    ", Output size: " + std::to_string(outputSize) + ")");
            }
            
            if (currentJoinCost < bestJoinCost) {
                bestJoinCost = currentJoinCost;
                bestJoinSteps = currentSteps;
                bestSizes = currentSizes;
                joinComponentExecutionOrder.clear();
                for (const auto& join : joinOrder) {
                    joinComponentExecutionOrder.push_back(join);
                }
            }
        }
        
        // Add best join order to execution steps
        executionSteps.push_back("Best join order found all permutations (Cost: " + 
            std::to_string(bestJoinCost) + "):");
        executionSteps.insert(executionSteps.end(), 
            bestJoinSteps.begin(), bestJoinSteps.end());

        // Finally merge componentExecutionOrder with joinComponentExecutionOrder
        componentExecutionOrder.insert(
            componentExecutionOrder.end(),
            joinComponentExecutionOrder.begin(),
            joinComponentExecutionOrder.end()
        );
        
        totalCost += bestJoinCost;
        tableSizes = bestSizes;
    }

    double estimateCost() override {
        return totalCost;
    }

    void printPlan() const override {
        std::cout << "\n=== Try All Join Order Plan ===\n";
        for (const auto& step : executionSteps) {
            std::cout << step << "\n";
        }
        std::cout << "Total Estimated Cost: " << totalCost << "\n";
        std::cout << "============================\n";
    }
};

// Add these two new plan classes to planner.h

class GreedyJoinPlan : public Plan {
private:
    double totalCost;
    std::vector<std::string> executionSteps;
    std::unordered_map<std::string, size_t> tableSizes;

    // Helper function to find the best next join
    std::pair<size_t, double> findBestNextJoin(
        const std::vector<std::shared_ptr<JoinComponent>>& remainingJoins,
        const std::unordered_set<std::string>& joinedTables) {
        
        double bestCost = std::numeric_limits<double>::max();
        size_t bestJoinIdx = 0;
        
        for (size_t i = 0; i < remainingJoins.size(); ++i) {
            const auto& join = remainingJoins[i];
            
            // Check if this join can be performed
            bool canJoinLeft = joinedTables.find(join->lhsTable) != joinedTables.end();
            bool canJoinRight = joinedTables.find(join->rhsTable) != joinedTables.end();
            
            if ((canJoinLeft && !canJoinRight) || (!canJoinLeft && canJoinRight)) {
                auto costAndSel = estimateJoinCostAndSelectivity(
                    join->lhsTable, join->rhsTable,
                    join->lhsColumn, join->rhsColumn,
                    tableSizes[join->lhsTable],
                    tableSizes[join->rhsTable]);
                
                if (costAndSel.cost < bestCost) {
                    bestCost = costAndSel.cost;
                    bestJoinIdx = i;
                }
            }
        }
        
        return {bestJoinIdx, bestCost};
    }

public:
    GreedyJoinPlan(Schema* schema, const QueryComponents& components) 
        : Plan(schema, components), totalCost(0.0) {}

    void generatePlan() override {
        executionSteps.push_back("Estimating costs for greedy join strategy:");
        
        // Initialize table sizes
        for (const auto& table : components.tables) {
            tableSizes[table->tableName] = schema->getTable(table->tableName)->data.size();
        }

        // First apply filters
        executionSteps.push_back("Estimating filter costs:");
        for (const auto& filter : components.scalarFilters) {
            auto costAndSel = estimateFilterCostAndSelectivity(
                filter->lhsTable,
                filter->lhsColumn,
                filter->predicate,
                filter->rhsValue);
            
            totalCost += costAndSel.cost;
            
            size_t outputSize = static_cast<size_t>(
                tableSizes[filter->lhsTable] * costAndSel.selectivity);
            tableSizes[filter->lhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Filter " + filter->lhsTable + "." + filter->lhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");
            
            componentExecutionOrder.push_back(filter);
        }

        // Greedy join ordering
        executionSteps.push_back("Estimating join costs (greedy strategy):");
        
        std::vector<std::shared_ptr<JoinComponent>> remainingJoins = components.joins;
        std::unordered_set<std::string> joinedTables;
        
        // Start with the smallest filtered table
        auto smallestTable = std::min_element(
            tableSizes.begin(), tableSizes.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });
        joinedTables.insert(smallestTable->first);

        while (!remainingJoins.empty()) {
            auto [bestJoinIdx, joinCost] = findBestNextJoin(remainingJoins, joinedTables);
            auto& bestJoin = remainingJoins[bestJoinIdx];
            
            auto costAndSel = estimateJoinCostAndSelectivity(
                bestJoin->lhsTable, bestJoin->rhsTable,
                bestJoin->lhsColumn, bestJoin->rhsColumn,
                tableSizes[bestJoin->lhsTable],
                tableSizes[bestJoin->rhsTable]);
            
            totalCost += costAndSel.cost;
            
            size_t outputSize = static_cast<size_t>(
                std::min(tableSizes[bestJoin->lhsTable], tableSizes[bestJoin->rhsTable])
            );
            
            tableSizes[bestJoin->lhsTable] = outputSize;
            tableSizes[bestJoin->rhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Join " + bestJoin->lhsTable + "." + bestJoin->lhsColumn +
                " = " + bestJoin->rhsTable + "." + bestJoin->rhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");
            
            componentExecutionOrder.push_back(bestJoin);
            
            joinedTables.insert(bestJoin->lhsTable);
            joinedTables.insert(bestJoin->rhsTable);
            
            remainingJoins.erase(remainingJoins.begin() + bestJoinIdx);
        }
    }

    double estimateCost() override {
        return totalCost;
    }

    void printPlan() const override {
        std::cout << "\n=== Greedy Join Plan ===\n";
        for (const auto& step : executionSteps) {
            std::cout << step << "\n";
        }
        std::cout << "Total Estimated Cost: " << totalCost << "\n";
        std::cout << "=====================\n";
    }
};

class DPJoinPlan : public Plan {
private:
    double totalCost;
    std::vector<std::string> executionSteps;
    std::unordered_map<std::string, size_t> tableSizes;

    struct SubPlan {
        std::vector<std::string> tables;
        double cost;
        std::vector<std::string> joinSequence;  // Store the sequence of joins
        
        SubPlan() : cost(0.0) {}
    };

public:
    DPJoinPlan(Schema* schema, const QueryComponents& components) 
        : Plan(schema, components), totalCost(0.0) {}

    void generatePlan() override {
        executionSteps.push_back("Estimating costs for dynamic programming join strategy:");
        
        // Initialize table sizes and apply filters first
        for (const auto& table : components.tables) {
            tableSizes[table->tableName] = schema->getTable(table->tableName)->data.size();
        }

        // Apply filters first
        executionSteps.push_back("Estimating filter costs:");
        for (const auto& filter : components.scalarFilters) {
            auto costAndSel = estimateFilterCostAndSelectivity(
                filter->lhsTable,
                filter->lhsColumn,
                filter->predicate,
                filter->rhsValue);
            
            totalCost += costAndSel.cost;
            
            size_t outputSize = static_cast<size_t>(
                tableSizes[filter->lhsTable] * costAndSel.selectivity);
            tableSizes[filter->lhsTable] = outputSize;
            
            executionSteps.push_back(
                "  Filter " + filter->lhsTable + "." + filter->lhsColumn +
                " (Cost: " + std::to_string(costAndSel.cost) +
                ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                ", Output size: " + std::to_string(outputSize) + ")");

            componentExecutionOrder.push_back(filter);
        }

        // Dynamic programming for join ordering
        executionSteps.push_back("Estimating join costs (dynamic programming):");
        
        std::unordered_map<std::string, SubPlan> dpTable;
        
        // Initialize single-table subplans
        for (const auto& table : components.tables) {
            SubPlan plan;
            plan.tables = {table->tableName};
            plan.cost = 0;  // Already filtered
            dpTable[table->tableName] = plan;
        }

        // Build up larger plans
        for (size_t size = 2; size <= components.tables.size(); ++size) {
            std::unordered_map<std::string, SubPlan> newPlans;
            
            for (const auto& [key1, plan1] : dpTable) {
                for (const auto& [key2, plan2] : dpTable) {
                    if (plan1.tables.size() + plan2.tables.size() == size) {
                        // Try all possible joins between these sets of tables
                        for (const auto& join : components.joins) {
                            if (canJoinPlans(plan1, plan2, join)) {
                                auto costAndSel = estimateJoinCostAndSelectivity(
                                    join->lhsTable, join->rhsTable,
                                    join->lhsColumn, join->rhsColumn,
                                    tableSizes[join->lhsTable],
                                    tableSizes[join->rhsTable]);
                                
                                SubPlan newPlan;
                                newPlan.tables = plan1.tables;
                                newPlan.tables.insert(newPlan.tables.end(),
                                    plan2.tables.begin(), plan2.tables.end());
                                newPlan.cost = plan1.cost + plan2.cost + costAndSel.cost;
                                
                                // Store join sequence
                                newPlan.joinSequence = plan1.joinSequence;
                                newPlan.joinSequence.insert(newPlan.joinSequence.end(),
                                    plan2.joinSequence.begin(), plan2.joinSequence.end());
                                newPlan.joinSequence.push_back(
                                    "Join " + join->lhsTable + "." + join->lhsColumn +
                                    " = " + join->rhsTable + "." + join->rhsColumn +
                                    " (Cost: " + std::to_string(costAndSel.cost) +
                                    ", Selectivity: " + std::to_string(costAndSel.selectivity) +
                                    ", Output size: " + std::to_string(
                                        std::min(static_cast<size_t>(tableSizes[join->lhsTable]), 
                                                static_cast<size_t>(tableSizes[join->rhsTable]))
                                        ) + ")");
                                
                                std::string newKey = createPlanKey(newPlan.tables);
                                if (newPlans.find(newKey) == newPlans.end() ||
                                    newPlans[newKey].cost > newPlan.cost) {
                                    newPlans[newKey] = newPlan;
                                }
                            }
                        }
                    }
                }
            }
            
            dpTable.insert(newPlans.begin(), newPlans.end());
        }

        // Find and use the best complete plan
        std::string finalKey = createPlanKey(getAllTableNames());
        if (dpTable.find(finalKey) != dpTable.end()) {
            totalCost = dpTable[finalKey].cost;
            executionSteps.push_back("Best join order found:");
            executionSteps.insert(executionSteps.end(),
                dpTable[finalKey].joinSequence.begin(),
                dpTable[finalKey].joinSequence.end());
        }
        // NOTE: We are not getting the executable join order for DP. Will come to it later
    }

    double estimateCost() override {
        return totalCost;
    }

    void printPlan() const override {
        std::cout << "\n=== Dynamic Programming Join Plan ===\n";
        for (const auto& step : executionSteps) {
            std::cout << step << "\n";
        }
        std::cout << "Total Estimated Cost: " << totalCost << "\n";
        std::cout << "==============================\n";
    }

private:
    bool canJoinPlans(const SubPlan& plan1, const SubPlan& plan2,
                      const std::shared_ptr<JoinComponent>& join) {
        // Check if these plans can be joined using this join condition
        bool plan1HasLeft = std::find(plan1.tables.begin(), plan1.tables.end(),
            join->lhsTable) != plan1.tables.end();
        bool plan1HasRight = std::find(plan1.tables.begin(), plan1.tables.end(),
            join->rhsTable) != plan1.tables.end();
        bool plan2HasLeft = std::find(plan2.tables.begin(), plan2.tables.end(),
            join->lhsTable) != plan2.tables.end();
        bool plan2HasRight = std::find(plan2.tables.begin(), plan2.tables.end(),
            join->rhsTable) != plan2.tables.end();
        
        return (plan1HasLeft && plan2HasRight) || (plan1HasRight && plan2HasLeft);
    }

    std::string createPlanKey(const std::vector<std::string>& tables) {
        std::vector<std::string> sortedTables = tables;
        std::sort(sortedTables.begin(), sortedTables.end());
        std::string key;
        for (const auto& table : sortedTables) {
            if (!key.empty()) key += ",";
            key += table;
        }
        return key;
    }

    std::vector<std::string> getAllTableNames() {
        std::vector<std::string> names;
        for (const auto& table : components.tables) {
            names.push_back(table->tableName);
        }
        return names;
    }
};

class Planner {
private:
    Schema* schema;
    QueryComponents components;
    std::vector<std::unique_ptr<Plan>> plans;
    std::unordered_map<std::string, double> planGenerationTimes;  // Store timing info

    // Helper function to get current time in milliseconds
    double getCurrentTimeMs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count() / 1000.0;  // Convert to milliseconds
    }

public:
    Planner(Schema* schema, const QueryComponents& components) 
        : schema(schema), components(components) {}

    void generatePlans() {
        std::cout << "\nGenerating query plans...\n";
        
        // Create and time JoinsFirstPlan
        auto startTime = getCurrentTimeMs();
        plans.push_back(std::make_unique<JoinsFirstPlan>(schema, components));
        plans.back()->generatePlan();
        auto endTime = getCurrentTimeMs();
        planGenerationTimes["JoinsFirst"] = endTime - startTime;
        
        // Create and time FiltersFirstPlan
        startTime = getCurrentTimeMs();
        plans.push_back(std::make_unique<FiltersFirstPlan>(schema, components));
        plans.back()->generatePlan();
        endTime = getCurrentTimeMs();
        planGenerationTimes["FiltersFirst"] = endTime - startTime;

        startTime = getCurrentTimeMs();
        plans.push_back(std::make_unique<TryAllJoinOrderPlan>(schema, components));
        plans.back()->generatePlan();
        endTime = getCurrentTimeMs();
        planGenerationTimes["TryAllJoinOrderPlan"] = endTime - startTime;
        
        // Create and time GreedyJoinPlan 
        startTime = getCurrentTimeMs();
        plans.push_back(std::make_unique<GreedyJoinPlan>(schema, components));
        plans.back()->generatePlan();
        endTime = getCurrentTimeMs();
        planGenerationTimes["GreedyJoinPlan"] = endTime - startTime;

        startTime = getCurrentTimeMs();
        plans.push_back(std::make_unique<DPJoinPlan>(schema, components));
        plans.back()->generatePlan();
        endTime = getCurrentTimeMs();
        planGenerationTimes["DPJoinPlan"] = endTime - startTime;
    }

    void printAllPlans() const {
        std::cout << "\n=== Plan Generation Summary ===\n";
        for (size_t i = 0; i < plans.size(); ++i) {
            const auto& plan = plans[i];
            std::string planType;
            
            // Determine plan type
            if (dynamic_cast<JoinsFirstPlan*>(plan.get())) {
                planType = "JoinsFirst";
            } else if (dynamic_cast<FiltersFirstPlan*>(plan.get())) {
                planType = "FiltersFirst";
            } else if (dynamic_cast<TryAllJoinOrderPlan*>(plan.get())) {
                planType = "TryAllJoinOrderPlan";
            } else if (dynamic_cast<GreedyJoinPlan*>(plan.get())) {
                planType = "GreedyJoinPlan";
            } else if (dynamic_cast<DPJoinPlan*>(plan.get())) {
                planType = "DPJoinPlan";
            }
            
            std::cout << "\nPlan Type: " << planType << "\n";
            std::cout << "Generation Time: " << planGenerationTimes.at(planType) << " ms\n";
            plan->printPlan();
        }
        std::cout << "===========================\n";
    }

    Plan* getBestPlan() {
        if (plans.empty()) {
            return nullptr;
        }
        
        auto bestPlan = std::min_element(plans.begin(), plans.end(),
            [](const auto& a, const auto& b) {
                return a->estimateCost() < b->estimateCost();
            });
            
        std::string bestPlanType;
        if (dynamic_cast<JoinsFirstPlan*>(bestPlan->get())) {
            bestPlanType = "JoinsFirst";
        } else if (dynamic_cast<FiltersFirstPlan*>(bestPlan->get())) {
            bestPlanType = "FiltersFirst";
        } else if (dynamic_cast<TryAllJoinOrderPlan*>(bestPlan->get())) {
            bestPlanType = "TryAllJoinOrderPlan";
        } else if (dynamic_cast<GreedyJoinPlan*>(bestPlan->get())) {
            bestPlanType = "GreedyJoinPlan";
        } else if (dynamic_cast<DPJoinPlan*>(bestPlan->get())) {
            bestPlanType = "DPJoinPlan";
        }
        
        std::cout << "\nBest Plan Selected: " << bestPlanType << "\n";
        std::cout << "Plan Generation Time: " << planGenerationTimes[bestPlanType] << " ms\n";
        std::cout << "Estimated Cost: " << bestPlan->get()->estimateCost() << "\n";
        bestPlan->get()->printExecutionOrder();  // Add this line
        
        return bestPlan->get();
    }
};
