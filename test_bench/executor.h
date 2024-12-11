// executor.h
#pragma once
#include "schema.h"
#include "parser.h"
#include <unordered_map>
#include <iomanip>
#include <fstream>

class Executor {
private:
    Schema* schema;
    std::unordered_map<std::string, std::shared_ptr<Table>> tableMap;

    std::shared_ptr<Table> applyFilter(std::shared_ptr<Table> table,
                                     const std::string& baseTableName, 
                                     const std::string& column,
                                     Predicate::Op op, 
                                     const Field& value) {
        auto filteredTable = std::make_shared<Table>(table->name + "_filtered");
        
        // Copy the column definitions from the original table
        for (const auto& col : table->getColumns()) {
            filteredTable->addColumn(col.name, col.baseTableName, col.type);
        }

        // Apply filter condition
        for (const auto& row : table->data) {
            int colIndex = table->getColumnIndex(column, baseTableName);
            if (colIndex == -1) {
                throw std::runtime_error("Column not found (Filter): " + column);
            }

            bool matches = false;
            switch (op) {
                case Predicate::Op::EQUALS:
                    matches = row[colIndex] == value;
                    break;
                case Predicate::Op::GREATER_THAN:
                    matches = row[colIndex] > value;
                    break;
                case Predicate::Op::LESS_THAN:
                    matches = row[colIndex] < value;
                    break;
                // Add other operators as needed
                /*
                    Add for 
                    LESS_THAN_OR_EQ,
                    GREATER_THAN_OR_EQ,
                    NOT_EQUALS
                    also default is no match
                */
                case Predicate::Op::LESS_THAN_OR_EQ:
                    matches = row[colIndex] <= value;
                    break;
                case Predicate::Op::GREATER_THAN_OR_EQ:
                    matches = row[colIndex] >= value;
                    break;
                case Predicate::Op::NOT_EQUALS:
                    matches = row[colIndex] != value;
                    break;
                default:
                    break;
            }

            if (matches) {
                filteredTable->data.push_back(row);
            }
        }

        // Recompute histograms for the filtered table
        filteredTable->recomputeHistogramsForIntegerColumn();
        return filteredTable;
    }

    std::shared_ptr<Table> joinTables(std::shared_ptr<Table> leftTable,
                                    std::shared_ptr<Table> rightTable,
                                    const std::string& leftBaseTable,
                                    const std::string& rightBaseTable,
                                    const std::string& leftCol,
                                    const std::string& rightCol) {
        auto joinedTable = std::make_shared<Table>(leftTable->name + "_" + rightTable->name + "_joined");
        
        // Copy columns from both tables
        for (const auto& col : leftTable->getColumns()) {
            joinedTable->addColumn(col.name, col.baseTableName, col.type);
        }
        for (const auto& col : rightTable->getColumns()) {
            joinedTable->addColumn(col.name, col.baseTableName, col.type);
        }

        int leftColIndex = leftTable->getColumnIndex(leftCol, leftBaseTable);
        int rightColIndex = rightTable->getColumnIndex(rightCol, rightBaseTable);

        if (leftColIndex == -1 || rightColIndex == -1) {
            throw std::runtime_error("Join column not found");
        }

        // Perform nested loop join
        for (const auto& leftRow : leftTable->data) {
            for (const auto& rightRow : rightTable->data) {
                if (leftRow[leftColIndex] == rightRow[rightColIndex]) {
                    std::vector<Field> joinedRow;
                    joinedRow.insert(joinedRow.end(), leftRow.begin(), leftRow.end());
                    joinedRow.insert(joinedRow.end(), rightRow.begin(), rightRow.end());
                    joinedTable->data.push_back(joinedRow);
                }
            }
        }

        // TODO: Do we want to do this or can we go without it?
        // Recompute histograms for the joined table
        joinedTable->recomputeHistogramsForIntegerColumn();
        return joinedTable;
    }

public:
    Executor(Schema* schema) : schema(schema) {}

    void executeQuery(const std::vector<std::shared_ptr<Component>>& componentOrder) {
        // Initialize table map with original tables
        tableMap.clear();
        std::cout << "\nExecuting query...\n";

        // First pass: Load all required tables
        for (const auto& component : componentOrder) {
            if (auto filter = std::dynamic_pointer_cast<ScalarFilterComponent>(component)) {
                if (tableMap.find(filter->lhsTable) == tableMap.end()) {
                    tableMap[filter->lhsTable] = schema->getTable(filter->lhsTable);
                }
            }
            else if (auto join = std::dynamic_pointer_cast<JoinComponent>(component)) {
                if (tableMap.find(join->lhsTable) == tableMap.end()) {
                    tableMap[join->lhsTable] = schema->getTable(join->lhsTable);
                }
                if (tableMap.find(join->rhsTable) == tableMap.end()) {
                    tableMap[join->rhsTable] = schema->getTable(join->rhsTable);
                }
            }
        }

        // Second pass: Execute operations in order
        for (const auto& component : componentOrder) {
            if (auto filter = std::dynamic_pointer_cast<ScalarFilterComponent>(component)) {
                std::cout << "Applying filter on " << filter->lhsTable 
                         << "." << filter->lhsColumn << "\n";
                
                auto& table = tableMap[filter->lhsTable];
                table = applyFilter(table, filter->lhsTable, filter->lhsColumn, 
                                  filter->predicate, filter->rhsValue);
                
                std::cout << "Filtered table size: " << table->data.size() << " rows\n";
            }
            else if (auto join = std::dynamic_pointer_cast<JoinComponent>(component)) {
                std::cout << "Joining " << join->lhsTable << " and " 
                         << join->rhsTable << "\n";
                
                auto leftTable = tableMap[join->lhsTable];
                auto rightTable = tableMap[join->rhsTable];
                
                auto joinedTable = joinTables(leftTable, rightTable,
                                            join->lhsTable, join->rhsTable,
                                            join->lhsColumn, join->rhsColumn);
                
                // Update both table references to point to joined result
                tableMap[join->lhsTable] = joinedTable;
                tableMap[join->rhsTable] = joinedTable;
                
                std::cout << "Joined table size: " << joinedTable->data.size() 
                         << " rows\n";
            }
        }

        // Print final result
        if (!tableMap.empty()) {
            auto finalTable = tableMap.begin()->second;
            std::cout << "\nQuery execution completed. Found " 
                    << finalTable->data.size() << " rows.\n";
            writeResultToFile(finalTable);
        }
    }

    void writeResultToFile(std::shared_ptr<Table> table) {
        std::ofstream outFile("output/results.txt");
        if (!outFile) {
            std::cerr << "Error: Could not open output/results.txt for writing\n";
            return;
        }

        // Write header
        outFile << "Query Result\n";
        outFile << "============\n";
        outFile << "Total Rows: " << table->data.size() << "\n\n";

        // Write column headers with proper spacing
        const int COLUMN_WIDTH = 20;
        for (const auto& col : table->columns) {
            outFile << std::left << std::setw(COLUMN_WIDTH) 
                    << (col.baseTableName + "." + col.name);
        }
        outFile << "\n";

        // Write separator line
        for (size_t i = 0; i < table->columns.size(); ++i) {
            outFile << std::string(COLUMN_WIDTH, '-');
        }
        outFile << "\n";

        // Write data rows (limit to 100 rows for readability)
        const size_t MAX_ROWS = 1000;
        size_t rowsToShow = std::min(table->data.size(), MAX_ROWS);
        
        for (size_t i = 0; i < rowsToShow; ++i) {
            for (const auto& field : table->data[i]) {
                if(field.getType() == FieldType::STRING) {
                    outFile << std::left << std::setw(COLUMN_WIDTH) << field.getStringValue();
                }
                else if(field.getType() == FieldType::INTEGER) {
                    outFile << std::left << std::setw(COLUMN_WIDTH) << field.getIntValue();
                }
                else{
                    // error handling
                    std::cerr << "Error: Invalid field type.\n";
                }
            }
            outFile << "\n";
        }

        // If there are more rows, indicate that
        if (table->data.size() > MAX_ROWS) {
            outFile << "\n... and " << (table->data.size() - MAX_ROWS) 
                    << " more rows\n";
        }

        outFile.flush();
        outFile.close();
        
        std::cout << "\nResults have been written to output/results.txt\n";
    }
};