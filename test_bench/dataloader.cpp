// dataloader.cpp
#include "schema.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>

void Table::print(int limit) const {
    std::cout << "Table: " << name << std::endl;
    for (const auto& col : columns) {
        std::cout << col.name << "\t";
    }
    std::cout << std::endl;
    
    for (int i = 0; i < std::min(limit, static_cast<int>(data.size())); ++i) {
        for (const auto& cell : data[i]) {
            if(cell.getType() == FieldType::INTEGER){
                std::cout << cell.getIntValue() << "\t";
            }
            else{
                std::cout << cell.getStringValue() << "\t";
            }
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;

    // // Also print the last 5
    // if (data.size() > 5) {
    //     for (size_t i = data.size() - 5; i < data.size(); ++i) {
    //         // Only print at index 2
    //         const auto& cell = data[i][2];
    //         if(cell.getType() == FieldType::INTEGER){
    //             std::cout << cell.getIntValue() << "\t";
    //         }
    //         else{
    //             std::cout << cell.getStringValue() << "\t"; 
    //         }
    //         std::cout << std::endl;
    //     }
    // }
}

void Table::printToFile() const {
    std::filesystem::create_directories("output");
    std::ofstream outFile("output/result.txt", std::ios::app);
    if (!outFile) {
        throw std::runtime_error("Unable to open output/result.txt for writing");
    }

    const int COLUMN_WIDTH = 20;  // Fixed width for all columns

    // Write table name
    outFile << "Table: " << name << "\n";
    
    // Write column headers
    for (const auto& col : columns) {
        outFile << std::left << std::setw(COLUMN_WIDTH) << col.name;
    }
    outFile << "\n";
    
    // Write separator line
    for (size_t i = 0; i < columns.size(); ++i) {
        outFile << std::string(COLUMN_WIDTH, '-');
    }
    outFile << "\n";
    
    // Write data rows
    for (const auto& row : data) {
        for (const auto& cell : row) {
            if(cell.getType() == FieldType::INTEGER) {
                outFile << std::left << std::setw(COLUMN_WIDTH) 
                       << std::to_string(cell.getIntValue());
            } else {
                outFile << std::left << std::setw(COLUMN_WIDTH) 
                       << cell.getStringValue();
            }
        }
        outFile << "\n";
    }
    outFile << "\n";

    outFile.close();
}

void Schema::print() const {
    for (const auto& pair : tables) {
        pair.second->print();
    }
}

void Schema::printTableColumns(const std::string& name) const {
    auto table = getTable(name);
    for (const auto& col : table->columns) {
        std::cout << col.name << "\t";
    }
    std::cout << std::endl;
}

void loadSchemaFromFile(Schema& schema, const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open schema file: " + filename);
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string tableName;
        std::getline(iss, tableName, '(');
        
        auto table = std::make_shared<Table>(tableName);
        
        std::string columnDef;
        while (std::getline(iss, columnDef, ',')) {
            size_t spacePos = columnDef.find(' ');
            if (spacePos != std::string::npos) {
                std::string columnName = columnDef.substr(0, spacePos);
                std::string columnType = columnDef.substr(spacePos + 1);
                columnType.erase(std::remove(columnType.begin(), columnType.end(), ')'), columnType.end());
                if(!isalpha(columnName[0])) {
                    columnName.erase(columnName.begin());
                }

                if (columnType == "int") {
                    std::cout<<"Column name: "<<columnName<<" Column table: "<<tableName<<std::endl;
                    table->addColumn(columnName, tableName, FieldType::INTEGER);
                } else if (columnType == "string") {
                    table->addColumn(columnName, tableName, FieldType::STRING);
                } else {
                    throw std::runtime_error("Unknown column type: " + columnType);
                }
            }
        }
        
        schema.addTable(tableName, table);
    }
}

void loadDataFromFile(Schema& schema, const std::string& tableName, const std::string& filename) {
    std::cout << "Loading Data from " << filename << std::endl;
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open data file: " + filename);
    }

    auto table = schema.getTable(tableName);
    
    std::string line;
    while (std::getline(file, line)) {
        // Skip empty lines
        if (line.empty() || line.find_first_not_of(" \t\r\n") == std::string::npos) {
            continue;
        }

        std::vector<Field> row;
        std::istringstream iss(line);
        std::string value;
        size_t columnIndex = 0;
        
        while (std::getline(iss, value, '|')) {
            // Trim whitespace from both ends
            auto start = value.find_first_not_of(" \t\r\n");
            auto end = value.find_last_not_of(" \t\r\n");
            
            if (start != std::string::npos && end != std::string::npos) {
                value = value.substr(start, end - start + 1);
            } else {
                value = ""; // If string contains only whitespace
            }

            if (table->columns[columnIndex].type == FieldType::INTEGER) {
                if (!value.empty()) {
                    try {
                        row.push_back(Field(std::stoi(value)));
                    } catch (const std::exception& e) {
                        std::cerr << "Error converting value '" << value 
                                << "' to integer at column " << columnIndex 
                                << " in file " << filename << std::endl;
                        throw;
                    }
                } else {
                    row.push_back(Field(0)); // or handle empty integer fields differently
                }
            } else if (table->columns[columnIndex].type == FieldType::STRING) {
                row.push_back(Field(value));
            }
            
            ++columnIndex;
        }
        
        // Verify we have the correct number of columns
        if (columnIndex != table->columns.size()) {
            std::cerr << "Warning: Row has " << columnIndex << " columns, expected " 
                     << table->columns.size() << " columns" << std::endl;
        }
        
        table->addRow(row);
    }
    
    table->recomputeHistogramsForIntegerColumn();
}

Schema loadIMDBData(const std::string& schemaFile, const std::string& dataDir) {
    Schema schema;
    loadSchemaFromFile(schema, schemaFile);

    const std::vector<std::string> tableNames = {"actor", "movie", "director", "casts", "movie_director", "genre"};
    for (const auto& tableName : tableNames) {
        std::string dataFile = dataDir + "/" + tableName + ".txt";
        loadDataFromFile(schema, tableName, dataFile);
        std::cout<<"Table size "<<tableName<<": "<<schema.getTableSize(tableName)<<std::endl;

    }

    return schema;
}

extern "C" Schema* createAndLoadIMDBData() {
    try {
        Schema* schema = new Schema(loadIMDBData("0.1/imdb_schema.txt", "0.1"));
        std::cout << "Data loaded successfully." << std::endl;

        double selectivity = schema->tables["movie"].get()->estimateSelectivity("year", Predicate::Op::GREATER_THAN, Field(1999));
        std::cout << "Selectivity: " << selectivity << std::endl;
        selectivity = schema->tables["actor"].get()->estimateSelectivity("lname", Predicate::Op::EQUALS, Field("Cruise"));
        std::cout << "Selectivity: " << selectivity << std::endl;
        return schema;
    } catch (const std::exception& e) {
        std::cerr << "Error loading data: " << e.what() << std::endl;
        return nullptr;
    }
}