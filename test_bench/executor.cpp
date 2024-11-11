#include <memory>
#include <functional>
#include <iostream>
#include <set>
#include "schema.h"

// Forward declarations
class Operator;
class Plan;

// Abstract base class for all operators
class Operator {
public:
    virtual ~Operator() = default;
    virtual std::shared_ptr<Table> execute() = 0;

    static int getTableColumnIndex(std::shared_ptr<Table> table, const std::string& tableName, const std::string& columnName) {
        // for(size_t i = 0; i < table->columns.size(); i++) {
        //     std::cout << "Column " << table->columns[i].name << " Table name " << table->columns[i].tableName << std::endl;
        //     if(table->columns[i].name == columnName && table->columns[i].tableName == tableName) {
        //         return i;
        //     }
        // }
        // throw std::runtime_error("Column " + tableName + "." + columnName + " not found in table " + table->name);

        std::cout<<"getting index for table: "<<tableName <<" column: "<<columnName<<std::endl;
        return table->getColumnIndex(columnName, tableName);
    }
};

// Scan operator
class ScanOperator : public Operator {
private:
    std::shared_ptr<Table> table;

public:
    ScanOperator(std::shared_ptr<Table> table) : table(table) {}

    std::shared_ptr<Table> execute() override {
        std::cout << "Scanning table " << table->name << "of size" << table->data.size() << std::endl;
        table->print();
        return table;
    }
};

class ProjectOperator : public Operator {
private:
    std::shared_ptr<Operator> child;
    std::vector<std::string> columnNames;
    std::vector<std::string> tableNames;

public:
    ProjectOperator(std::shared_ptr<Operator> child, const std::vector<std::string>& columnNames, const std::vector<std::string>& tableNames)
        : child(child), columnNames(columnNames), tableNames(tableNames) {}

    // Inside the relevant operator (likely a ProjectOperator or similar)
    std::shared_ptr<Table> execute() override {
        std::cout<<"Executing ProjectOperator"<<std::endl;
        auto inputTable = child->execute();
        auto outputTable = std::make_shared<Table>(inputTable->name + "_projected");

        std::set<int> columnIndices;

        // TODO: Add support for multiple table names

        for (const auto& column : columnNames) {
            for(const auto& col : inputTable->columns) {
                if(col.name == column) {
                    outputTable->addColumn(col.name, col.tableName, col.type);
                    columnIndices.insert(inputTable->getColumnIndex(col.name, col.tableName));
                }
            }
        }

        for (const auto& row : inputTable->data) {
            std::vector<Field> outputRow;
            for (const auto& index : columnIndices) {
                outputRow.push_back(row[index]);
            }
            outputTable->addRow(outputRow);
        }

        
        return outputTable;
    }
};

// Filter operator
class FilterOperator : public Operator {
private:
    std::shared_ptr<Operator> child;
    std::function<bool(const std::vector<Field>&, int)> predicate;
    std::string tableName;
    std::string tableColumn;

public:
    FilterOperator(std::shared_ptr<Operator> child, 
                   std::function<bool(const std::vector<Field>&, int)> predicate,
                   const std::string& tableName,
                   const std::string& tableColumn)
        : child(child), predicate(predicate), tableName(tableName), tableColumn(tableColumn) {
            if (!child) {
                throw std::runtime_error("Filter operator created with null input");
            }
        }

    std::shared_ptr<Table> execute() override {
        if (!child) {
            throw std::runtime_error("Null input operator in filter execution");
        }
        std::cout<<"Executing FilterOperator"<<std::endl;
        auto inputTable = child->execute();
        std::cout<<"Input table: "<<inputTable->name<<std::endl;
        auto outputTable = std::make_shared<Table>(inputTable->name + "_filtered");

        // Get the column index
        size_t index = getTableColumnIndex(inputTable, tableName, tableColumn);

        // Copy the schema
        for (const auto& col : inputTable->getColumns()) {
            outputTable->addColumn(col.name, col.tableName, col.type);
        }

        // Apply the filter
        for (const auto& row : inputTable->data) {
            if (predicate(row, index)) {
                outputTable->addRow(row);
            }
        }

        return outputTable;
    }
};

// Join operator
class JoinOperator : public Operator {
private:
    std::shared_ptr<Operator> leftChild;
    std::shared_ptr<Operator> rightChild;
    std::function<bool(const std::vector<Field>&, const std::vector<Field>&, int, int)> joinPredicate;
    std::string actualLeftTableName;
    std::string actualRightTableName;
    std::string leftColumn;
    std::string rightColumn;

public:
    JoinOperator(std::shared_ptr<Operator> leftChild, 
                 std::shared_ptr<Operator> rightChild,
                 std::function<bool(const std::vector<Field>&, const std::vector<Field>&, int, int)> joinPredicate,
                 const std::string& actualLeftTableName,
                 const std::string& actualRightTableName,
                 const std::string& leftColumn,
                 const std::string& rightColumn)
        : leftChild(leftChild), rightChild(rightChild), joinPredicate(joinPredicate), actualLeftTableName(actualLeftTableName), actualRightTableName(actualRightTableName),
          leftColumn(leftColumn), rightColumn(rightColumn) {
            if (!leftChild) {
                throw std::runtime_error("Join operator created with null left child");
            }
            if (!rightChild) {
                throw std::runtime_error("Join operator created with null right child");
            }
        }

    std::shared_ptr<Table> execute() override {
        std::cout<<"Join operator"<<std::endl;
        auto leftTable = leftChild->execute();
        std::cout<<"Left table: "<<leftTable->name<<std::endl;
        auto rightTable = rightChild->execute();
        std::cout<<"Right table: "<<rightTable->name<<std::endl;
        auto outputTable = std::make_shared<Table>(leftTable->name + "_join_" + rightTable->name);
        std::cout<<"Executing JoinOperator on "<<leftTable->name<<" and "<<rightTable->name<<std::endl;

        // Get the index of the columns
        std::cout<<"Left column: "<<leftColumn<<"Left table: "<<actualLeftTableName<<std::endl;
        std::cout<<"Right column: "<<rightColumn<<"Right table: "<<actualRightTableName<<std::endl;
        int leftIndex = getTableColumnIndex(leftTable, actualLeftTableName, leftColumn);
        int rightIndex = getTableColumnIndex(rightTable, actualRightTableName, rightColumn);

        // Combine schemas
        for (const auto& col : leftTable->getColumns()) {
            //outputTable->addColumn(col.name, leftTable->name, col.type);
            outputTable->addColumn(col.name, col.tableName, col.type);
        }
        for (const auto& col : rightTable->getColumns()) {
            //outputTable->addColumn(col.name, rightTable->name , col.type);
            outputTable->addColumn(col.name, col.tableName, col.type);
        }

        std::cout<<"Created output table: "<<outputTable->name<<std::endl;

        // print cols of the output table
        for (const auto& col : outputTable->getColumns()) {
            std::cout << col.name << " ";
        }
        std::cout << std::endl;

        // Perform the join (NESTED LOOP JOIN)
        for (const auto& leftRow : leftTable->data) {
            for (const auto& rightRow : rightTable->data) {
                if (joinPredicate(leftRow, rightRow, leftIndex, rightIndex)) {
                    std::vector<Field> joinedRow;
                    // Add prefix to identify which table the columns come from
                    for (size_t i = 0; i < leftRow.size(); i++) {
                        joinedRow.push_back(leftRow[i]);
                    }
                    
                    // Skip the join column from the right table to avoid duplication
                    for (size_t i = 0; i < rightRow.size(); i++) {
                        joinedRow.push_back(rightRow[i]);
                    }
                    
                    outputTable->addRow(joinedRow);
                }
            }
        }

        std::cout<<"Row size is: "<<outputTable->data[0].size()<<std::endl;

        outputTable->print();

        return outputTable;
    }
};

// Plan class
class Plan {
private:
    std::shared_ptr<Operator> root;
    Schema& schema;
    std::unordered_map<std::string, std::shared_ptr<Operator>> tableOperators;

    std::shared_ptr<Operator> createFilterOrJoin(const WhereNode* whereNode, std::shared_ptr<Operator> currentOp) {
        for (auto& condition : whereNode->conditions) {
            if (condition.isJoinCondition) {
                // This is a join condition
                std::string leftTable = condition.lhs.table;
                std::string rightTable = std::get<Condition::Column>(condition.rhs).table;

                auto rightOp = tableOperators[rightTable];
                if (!rightOp) {
                    throw std::runtime_error("Table not found: " + rightTable);
                }

                auto joinPredicate = [&condition](const std::vector<Field>& leftRow, const std::vector<Field>& rightRow, int leftIndex, int rightIndex) {
                    // Implement join logic based on the condition
                    // This is a simplified version and needs to be expanded based on your Condition structure
                    if(condition.comparator == "=") {
                        return leftRow[leftIndex] == rightRow[rightIndex];
                    }
                    else if(condition.comparator == ">") {
                        return leftRow[leftIndex] > rightRow[rightIndex];
                    }
                    else if(condition.comparator == "<") {
                        return leftRow[leftIndex] < rightRow[rightIndex];
                    }
                    else if(condition.comparator == ">=") {
                        return leftRow[leftIndex] >= rightRow[rightIndex];
                    }
                    else if(condition.comparator == "<=") {
                        return leftRow[leftIndex] <= rightRow[rightIndex];
                    }
                    return true; // Placeholder
                };

                currentOp = std::make_shared<JoinOperator>(currentOp, rightOp, joinPredicate, 
                            leftTable, rightTable, condition.lhs.name, std::get<Condition::Column>(condition.rhs).name);
            } else {
                // This is a filter condition
                auto predicate = [&condition](const std::vector<Field>& row, int ind) {
                    // Implement filter logic based on the condition
                    // This is a simplified version and needs to be expanded based on your Condition structure
                    // Check which type the variant holds
                    Field* rhs = NULL;
                    switch(condition.rhs.index()) {
                        case 0:  // Column should be index 0 if defined first in variant
                            throw std::runtime_error("RHS is a column not a string or int " + std::get<0>(condition.rhs).name);
                            break;
                        case 1:  // int
                            rhs = new Field(std::get<1>(condition.rhs));
                            break;
                        case 2:  // string
                            rhs = new Field(std::get<2>(condition.rhs));
                            break;
                    }

                    // std::cout<<"Rhs index is: "<<rhs->getStringValue()<<std::endl;
                    // if(row[index] == Field("Spicer")){
                    //     std::cout<<"Row index is: "<<row[index].getStringValue()<<std::endl;
                        
                    // }

                    if(condition.comparator == "=") {
                        return row[ind] == *rhs;
                    }
                    else if(condition.comparator == ">") {
                        return row[ind] > *rhs;
                    }
                    else if(condition.comparator == "<") {
                        return row[ind] < *rhs;
                    }
                    else if(condition.comparator == ">=") {
                        return row[ind] >= *rhs;
                    }
                    else if(condition.comparator == "<=") {
                        return row[ind] <= *rhs;
                    }
                    return true; // Placeholder
                };

                currentOp = std::make_shared<FilterOperator>(currentOp, predicate, condition.lhs.table, condition.lhs.name);
            }
        }
        return currentOp;
    }

public:
    Plan(Schema& schema) : schema(schema) {}

    void createPlan(const std::vector<std::unique_ptr<ASTNode>>& ast) {
        std::shared_ptr<Operator> currentOp;

        // First pass: create ScanOperators for all tables
        for (const auto& node : ast) {
            if (auto fromNode = dynamic_cast<FromNode*>(node.get())) {
                for (const auto& tableRef : fromNode->tables) {
                    auto table = schema.getTable(tableRef.table);
                    std::cout<<"Got table name for scan: "<<table->name<<std::endl;
                    auto scanOp = std::make_shared<ScanOperator>(table);
                    tableOperators[tableRef.alias.empty() ? tableRef.table : tableRef.alias] = scanOp;
                }
            }
        }

        std::vector<std::string> finalColumnNames;
        std::vector<std::string> finalTableNames;

        // Second pass: create the operator tree
        for (const auto& node : ast) {
            if (auto fromNode = dynamic_cast<FromNode*>(node.get())) {
                // Start with the first table
                currentOp = tableOperators[fromNode->tables[0].table];
            } else if (auto whereNode = dynamic_cast<WhereNode*>(node.get())) {
                currentOp = createFilterOrJoin(whereNode, currentOp);
            } else if (auto joinNode = dynamic_cast<JoinNode*>(node.get())) {
                auto rightOp = tableOperators[joinNode->alias.empty() ? joinNode->table : joinNode->alias];
                if (!rightOp) {
                    throw std::runtime_error("Table not found: " + joinNode->table);
                }

                auto joinPredicate = [&joinNode](const std::vector<Field>& leftRow, const std::vector<Field>& rightRow, int leftIndex, int rightIndex) {
                    // Implement join logic based on the joinNode->condition
                    // This is a simplified version and needs to be expanded based on your Condition structure

                    if(joinNode->condition.comparator == "=") {
                        return leftRow[leftIndex] == rightRow[rightIndex];
                    }
                    else if(joinNode->condition.comparator == ">") {
                        return leftRow[leftIndex] > rightRow[rightIndex];
                    }
                    else if(joinNode->condition.comparator == "<") {
                        return leftRow[leftIndex] < rightRow[rightIndex];
                    }
                    else if(joinNode->condition.comparator == ">=") {
                        return leftRow[leftIndex] >= rightRow[rightIndex];
                    }
                    else if(joinNode->condition.comparator == "<=") {
                        return leftRow[leftIndex] <= rightRow[rightIndex];
                    }
                    return true; // Placeholder
                };

                currentOp = std::make_shared<JoinOperator>(currentOp, rightOp, joinPredicate, joinNode->condition.lhs.table, 
                            joinNode->condition.lhs.name, std::get<Condition::Column>(joinNode->condition.rhs).table, 
                                                            std::get<Condition::Column>(joinNode->condition.rhs).name);
            }
            else if(auto selectNode = dynamic_cast<SelectNode*>(node.get())) {
                for(const auto& column : selectNode->columns) {
                    finalColumnNames.push_back(column.name);
                    finalTableNames.push_back(column.table);
                }
            }
            // Add more conditions for other types of nodes (e.g., SELECT, GROUP BY, etc.)
        }

        // Add project operator


        auto projectOp = std::make_shared<ProjectOperator>(currentOp, finalColumnNames, finalTableNames);
        currentOp = projectOp;
        root = currentOp;
    }

    std::shared_ptr<Table> executePlan() {
        std::cout<<"Executing"<<std::endl;
        if (!root) {
            throw std::runtime_error("Plan not created yet");
        }
        return root->execute();
    }
};

// Example usage in main.cpp or wherever you're parsing the SQL
void executeQuery(const char* sql, Schema& schema) {
    try {
        SQLParser parser(sql);
        auto ast = parser.parse();

        Plan queryPlan(schema);
        queryPlan.createPlan(ast);
        auto resultTable = queryPlan.executePlan();

        std::cout << "Query result:" << std::endl;
        resultTable->print();
    } catch (const std::exception& e) {
        std::cerr << "Error executing query: " << e.what() << std::endl;
    }
}