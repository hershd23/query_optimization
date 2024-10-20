// main.cpp
#include "schema.h"
#include <iostream>
#include <string>

// Forward declarations of external functions
extern "C" {
    Schema* createAndLoadIMDBData();
    void parseSQL(const char* sql, const Schema* schema);
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
    std::string sql;
    while (true) {
        std::cout << "Enter an SQL query (or 'quit' to exit):" << std::endl;
        std::getline(std::cin, sql);

        if (sql == "quit") {
            break;
        }

        // Parse and process the SQL query
        parseSQL(sql.c_str(), schema);
    }

    // Clean up
    delete schema;

    return 0;
}