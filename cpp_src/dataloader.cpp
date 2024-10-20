// dataloader.cpp
#include "schema.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

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
                    table->addColumn(columnName, FieldType::INTEGER);
                } else if (columnType == "string") {
                    table->addColumn(columnName, FieldType::STRING);
                } else {
                    throw std::runtime_error("Unknown column type: " + columnType);
                }
            }
        }
        
        schema.addTable(tableName, table);
    }
}

void loadDataFromFile(Schema& schema, const std::string& tableName, const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open data file: " + filename);
    }

    auto table = schema.getTable(tableName);
    auto columns = table->columns;
    
    std::string line;
    while (std::getline(file, line)) {
        std::vector<Field> row;
        std::istringstream iss(line);
        std::string value;
        size_t columnIndex = 0;
        while (std::getline(iss, value, '|')) {
            if(columns[columnIndex].type == FieldType::INTEGER) {
                row.push_back(Field(std::stoi(value)));
            }
            else if(columns[columnIndex].type == FieldType::STRING) {
                row.push_back(Field(value));
            }
            ++columnIndex;
        }
        table->addRow(row);
    }
}

Schema loadIMDBData(const std::string& schemaFile, const std::string& dataDir) {
    Schema schema;
    loadSchemaFromFile(schema, schemaFile);

    const std::vector<std::string> tableNames = {"actor", "movie", "director", "casts", "movie_director", "genre"};
    for (const auto& tableName : tableNames) {
        std::string dataFile = dataDir + "/" + tableName + ".txt";
        loadDataFromFile(schema, tableName, dataFile);
        std::cout<<"Table size "<<tableName<<": "<<schema.getTableSize(tableName)<<std::endl;
        schema.print();
        schema.printTableColumns(tableName);
    }

    return schema;
}

extern "C" Schema* createAndLoadIMDBData() {
    try {
        Schema* schema = new Schema(loadIMDBData("0.1/imdb_schema.txt", "0.1"));
        std::cout << "Data loaded successfully." << std::endl;
        return schema;
    } catch (const std::exception& e) {
        std::cerr << "Error loading data: " << e.what() << std::endl;
        return nullptr;
    }
}