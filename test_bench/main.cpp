// main.cpp
#include "schema.h"
#include "parser.h"
#include "planner.h"
#include "executor.h"
#include <iostream>
#include <string>
#include <vector>

extern "C" {
    Schema* createAndLoadIMDBData();
}

void processQuery(const std::vector<std::string>& queryLines, Schema* schema) {
    try {
        auto queryComponents = SimpleParser::parse(queryLines, schema);
        
        // Create planner and generate plans
        Planner planner(schema, queryComponents);
        planner.generatePlans();
        
        // Print all plans and their costs
        planner.printAllPlans();

        std::cout << "\n=== Executing All Plans ===\n";
        
        // Get and execute all plans
        auto allPlans = planner.getAllPlans();
        std::vector<std::pair<std::string, double>> executionTimes;
        
        for (auto plan : allPlans) {
            std::string planType = planner.getPlanType(plan);
            std::cout << "\nExecuting " << planType << " Plan:\n";
            
            Executor executor(schema);
            
            // Time the execution
            auto startTime = std::chrono::high_resolution_clock::now();
            executor.executeQuery(plan->getExecutionOrder());
            auto endTime = std::chrono::high_resolution_clock::now();
            
            double executionTime = std::chrono::duration_cast<std::chrono::microseconds>(
                endTime - startTime).count() / 1000.0;  // Convert to milliseconds
            
            executionTimes.push_back({planType, executionTime});
        }
        
        // Print execution time summary
        std::cout << "\n=== Execution Time Summary ===\n";
        for (const auto& [planType, time] : executionTimes) {
            std::cout << planType << " Plan: " << time << " ms\n";
        }
        
        // } catch (const std::exception& e) {
        //     std::cerr << "Error processing query: " << e.what() << std::endl;
        // }
        
        // // Get the best plan
        // Plan* bestPlan = planner.getBestPlan();
        // if (bestPlan) {
        //     std::cout << "\nBest Plan Selected:\n";
        //     bestPlan->printPlan();
        //     Executor executor(schema);
        //     executor.executeQuery(bestPlan->getExecutionOrder());
        // }
        
    } catch (const std::exception& e) {
        std::cerr << "Error processing query: " << e.what() << std::endl;
    }
}

int main() {
    // Load the IMDB data
    Schema* schema = createAndLoadIMDBData();
    if (!schema) {
        std::cerr << "Failed to load IMDB data. Exiting." << std::endl;
        return 1;
    }

    std::cout << "IMDB data loaded successfully." << std::endl;
    
    // Main loop
    while (true) {
        std::cout << "\nEnter your query (type 'quit' alone on a line to exit):\n";
        std::vector<std::string> queryLines;
        std::string line;
        bool isQuit = false;

        while (std::getline(std::cin, line)) {
            if (line == "quit") {
                isQuit = true;
                break;
            }
            
            queryLines.push_back(line);
            
            // Break when we see query_end
            if (line.find("query_end") != std::string::npos) {
                break;
            }
        }

        if (isQuit) {
            break;
        }

        if (!queryLines.empty()) {
            processQuery(queryLines, schema);
        }
    }

    delete schema;
    return 0;
}