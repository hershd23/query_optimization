// parser.cpp
#include "parser.h"
#include <sstream>
#include <iostream>

QueryComponents SimpleParser::parse(const std::vector<std::string>& queryLines, Schema* schema) {
    QueryComponents components;
    bool queryStarted = false;
    
    for (const auto& line : queryLines) {
        std::string trimmedLine = line;
        trim(trimmedLine);
        
        if (trimmedLine == "query_start") {
            queryStarted = true;
            continue;
        }
        
        if (trimmedLine == "query_end") {
            break;
        }
        
        if (!queryStarted || trimmedLine.empty()) {
            continue;
        }

        size_t colonPos = trimmedLine.find(':');
        if (colonPos == std::string::npos) {
            continue;
        }

        std::string section = trimmedLine.substr(0, colonPos);
        std::string content = trimmedLine.substr(colonPos + 1);
        trim(content);

        if (section == "tables") {
            auto tableNames = splitAndTrim(content, ',');
            for (const auto& tableName : tableNames) {
                components.tables.push_back(
                    std::make_shared<TableComponent>(tableName));
            }
        }
        else if (section == "scalar_filters" && !content.empty()) {
            auto filters = splitAndTrim(content, ',');
            for (const auto& filter : filters) {
                // Parse filter of form: table.column=value
                size_t dotPos = filter.find('.');
                size_t opPos = std::string::npos;
                for (const auto& op : {">=", "<=", "!=", "=", ">", "<"}) {
                    opPos = filter.find(op);
                    if (opPos != std::string::npos) {
                        std::string tableName = filter.substr(0, dotPos);
                        std::string columnName = filter.substr(dotPos + 1, opPos - dotPos - 1);
                        std::string opStr = filter.substr(opPos, op[1] == '=' ? 2 : 1);
                        std::string value = filter.substr(opPos + (op[1] == '=' ? 2 : 1));
                        trim(value);

                        // Create appropriate Field based on column type
                        auto table = schema->getTable(tableName);
                        auto columnType = table->getColumnType(columnName);
                        Field fieldValue = (columnType == FieldType::INTEGER) ? 
                            Field(std::stoi(value)) : Field(value);

                        components.scalarFilters.push_back(
                            std::make_shared<ScalarFilterComponent>(
                                tableName,
                                columnName,
                                FilterComponent::stringToPredicate(opStr),
                                fieldValue
                            )
                        );
                        break;
                    }
                }
            }
        }
        else if (section == "dynamic_filters" && !content.empty()) {
            auto filters = splitAndTrim(content, ',');
            for (const auto& filter : filters) {
                // Parse filter of form: table1.column1=table2.column2
                size_t dotPos1 = filter.find('.');
                size_t opPos = filter.find('='); // Assuming only equality for dynamic filters
                size_t dotPos2 = filter.find('.', opPos);
                
                if (dotPos1 != std::string::npos && opPos != std::string::npos && dotPos2 != std::string::npos) {
                    std::string lhsTable = filter.substr(0, dotPos1);
                    std::string lhsColumn = filter.substr(dotPos1 + 1, opPos - dotPos1 - 1);
                    std::string rhsTable = filter.substr(opPos + 1, dotPos2 - opPos - 1);
                    std::string rhsColumn = filter.substr(dotPos2 + 1);
                    
                    trim(lhsTable); trim(lhsColumn);
                    trim(rhsTable); trim(rhsColumn);

                    components.dynamicFilters.push_back(
                        std::make_shared<DynamicFilterComponent>(
                            lhsTable,
                            lhsColumn,
                            Predicate::Op::EQUALS,
                            rhsTable,
                            rhsColumn
                        )
                    );
                }
            }
        }
        else if (section == "joins" && !content.empty()) {
            auto joins = splitAndTrim(content, ',');
            for (const auto& join : joins) {
                // Parse join of form: table1.column1=table2.column2
                size_t dotPos1 = join.find('.');
                size_t opPos = join.find('='); // Assuming only equality joins for now
                size_t dotPos2 = join.find('.', opPos);
                
                if (dotPos1 != std::string::npos && opPos != std::string::npos && dotPos2 != std::string::npos) {
                    std::string lhsTable = join.substr(0, dotPos1);
                    std::string lhsColumn = join.substr(dotPos1 + 1, opPos - dotPos1 - 1);
                    std::string rhsTable = join.substr(opPos + 1, dotPos2 - opPos - 1);
                    std::string rhsColumn = join.substr(dotPos2 + 1);
                    
                    trim(lhsTable); trim(lhsColumn);
                    trim(rhsTable); trim(rhsColumn);

                    components.joins.push_back(
                        std::make_shared<JoinComponent>(
                            lhsTable,
                            lhsColumn,
                            Predicate::Op::EQUALS,
                            rhsTable,
                            rhsColumn
                        )
                    );
                }
            }
        }
    }

    validateQuery(components, schema);
    components.print();
    return components;
}

void SimpleParser::validateQuery(QueryComponents& components, Schema* schema) {
    // Validate tables
    for (const auto& table : components.tables) {
        if (!schema->getTable(table->tableName)) {
            throw std::runtime_error("Table not found: " + table->tableName);
        }
    }

    // Validate scalar filters
    for (const auto& filter : components.scalarFilters) {
        auto table = schema->getTable(filter->lhsTable);
        if (!table) {
            throw std::runtime_error("Table in scalar filter not found: " + filter->lhsTable);
        }
        
        auto columnType = table->getColumnType(filter->lhsColumn);
        if (columnType == FieldType::INVALID) {
            throw std::runtime_error("Column not found in table " + 
                filter->lhsTable + ": " + filter->lhsColumn);
        }
        
        if (columnType != filter->rhsValue.getType()) {
            throw std::runtime_error("Type mismatch in scalar filter for " + 
                filter->lhsTable + "." + filter->lhsColumn);
        }
    }

    // Validate joins
    for (const auto& join : components.joins) {
        auto leftTable = schema->getTable(join->lhsTable);
        auto rightTable = schema->getTable(join->rhsTable);
        
        if (!leftTable || !rightTable) {
            throw std::runtime_error("Table in join condition not found: " + 
                (!leftTable ? join->lhsTable : join->rhsTable));
        }

        auto leftType = leftTable->getColumnType(join->lhsColumn);
        auto rightType = rightTable->getColumnType(join->rhsColumn);
        
        if (leftType == FieldType::INVALID || rightType == FieldType::INVALID) {
            throw std::runtime_error("Column not found in join condition between " + 
                join->lhsTable + " and " + join->rhsTable);
        }
        
        if (leftType != rightType) {
            throw std::runtime_error("Type mismatch in join condition between " + 
                join->lhsTable + "." + join->lhsColumn + " and " + 
                join->rhsTable + "." + join->rhsColumn);
        }
    }

    // Similar validation for dynamic filters
    for (const auto& filter : components.dynamicFilters) {
        auto leftTable = schema->getTable(filter->lhsTable);
        auto rightTable = schema->getTable(filter->rhsTable);
        
        if (!leftTable || !rightTable) {
            throw std::runtime_error("Table in dynamic filter not found: " + 
                (!leftTable ? filter->lhsTable : filter->rhsTable));
        }

        auto leftType = leftTable->getColumnType(filter->lhsColumn);
        auto rightType = rightTable->getColumnType(filter->rhsColumn);
        
        if (leftType == FieldType::INVALID || rightType == FieldType::INVALID) {
            throw std::runtime_error("Column not found in dynamic filter between " + 
                filter->lhsTable + " and " + filter->rhsTable);
        }
        
        if (leftType != rightType) {
            throw std::runtime_error("Type mismatch in dynamic filter between " + 
                filter->lhsTable + "." + filter->lhsColumn + " and " + 
                filter->rhsTable + "." + filter->rhsColumn);
        }
    }
}