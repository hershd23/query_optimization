// main.cpp
#include "schema.h"
#include "parser.h"
#include "planner.h"
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
        
        // Get the best plan
        Plan* bestPlan = planner.getBestPlan();
        if (bestPlan) {
            std::cout << "\nBest Plan Selected:\n";
            bestPlan->printPlan();
        }
        
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