#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <unordered_map>
#include <variant>
#include <functional>
#include <set>
#include "schema.h"

// Forward declaration of Schema class from dataloader.cpp
class Schema;

class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual void print(int indent = 0) const = 0;
};

class SelectNode : public ASTNode {
public:
    struct Column {
        std::string table;
        std::string name;
    };
    std::vector<Column> columns;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "SELECT" << std::endl;
        for (const auto& col : columns) {
            std::cout << std::string(indent + 2, ' ') << (col.table.empty() ? "" : col.table + ".") << col.name << std::endl;
        }
    }
};

class FromNode : public ASTNode {
public:
    struct TableRef {
        std::string table;
        std::string alias;
    };
    std::vector<TableRef> tables;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "FROM" << std::endl;
        for (const auto& table : tables) {
            std::cout << std::string(indent + 2, ' ') << table.table;
            if (!table.alias.empty()) {
                std::cout << " AS " << table.alias;
            }
            std::cout << std::endl;
        }
    }
};

class Condition {
public:
    struct Column {
        std::string table;
        std::string name;
    };
    Column lhs;
    std::string comparator;
    std::variant<Column, int, std::string> rhs;
    bool isJoinCondition;

    Condition() : isJoinCondition(false) {}

    void print(int indent = 0) const {
        std::cout << std::string(indent, ' ');
        if (!lhs.table.empty()) {
            std::cout << lhs.table << ".";
        }
        std::cout << lhs.name << " " << comparator << " ";
        std::visit([](const auto& v) {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Column>) {
                if (!v.table.empty()) {
                    std::cout << v.table << ".";
                }
                std::cout << v.name;
            } else {
                std::cout << v;
            }
        }, rhs);
        if (isJoinCondition) {
            std::cout << " (JOIN condition)";
        }
        std::cout << std::endl;
    }
};

class WhereNode : public ASTNode {
public:
    std::vector<Condition> conditions;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "WHERE" << std::endl;
        for (const auto& condition : conditions) {
            condition.print(indent + 2);
        }
    }
};

class JoinNode : public ASTNode {
public:
    std::string table;
    std::string alias;
    Condition condition;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "JOIN " << table;
        if (!alias.empty()) {
            std::cout << " AS " << alias;
        }
        std::cout << " ON ";
        condition.print(indent + 2);
    }
};

class GroupByNode : public ASTNode {
public:
    std::vector<std::string> columns;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "GROUP BY" << std::endl;
        for (const auto& col : columns) {
            std::cout << std::string(indent + 2, ' ') << col << std::endl;
        }
    }
};

class HavingNode : public ASTNode {
public:
    std::vector<Condition> conditions;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "HAVING" << std::endl;
        for (const auto& condition : conditions) {
            condition.print(indent + 2);
        }
    }
};

class SQLParser {
private:
    std::string query;
    size_t pos = 0;
    std::unordered_map<std::string, std::string> aliases;

    void skipWhitespace() {
        while (pos < query.length() && std::isspace(query[pos])) {
            pos++;
        }
    }

    std::string parseIdentifier() {
        skipWhitespace();
        size_t start = pos;
        while (pos < query.length() && (std::isalnum(query[pos]) || query[pos] == '_' || query[pos] == '.')) {
            pos++;
        }
        return query.substr(start, pos - start);
    }

    Condition::Column parseColumn() {
        Condition::Column column;
        std::string identifier = parseIdentifier();
        size_t dotPos = identifier.find('.');
        if (dotPos != std::string::npos) {
            column.table = identifier.substr(0, dotPos);
            column.name = identifier.substr(dotPos + 1);
        } else {
            column.name = identifier;
        }
        return column;
    }

    std::string parseComparator() {
        skipWhitespace();
        if (pos + 1 < query.length() && (query.substr(pos, 2) == "<=" || query.substr(pos, 2) == ">=")) {
            pos += 2;
            return query.substr(pos - 2, 2);
        } else if (pos < query.length() && (query[pos] == '<' || query[pos] == '>' || query[pos] == '=')) {
            pos++;
            return query.substr(pos - 1, 1);
        }
        throw std::runtime_error("Invalid comparator at position " + std::to_string(pos));
    }

    std::variant<Condition::Column, int, std::string> parseRightHandSide() {
        skipWhitespace();
        if (std::isalpha(query[pos]) || query[pos] == '_') {
            return parseColumn();
        } else if (std::isdigit(query[pos])) {
            size_t start = pos;
            while (pos < query.length() && std::isdigit(query[pos])) {
                pos++;
            }
            return std::stoi(query.substr(start, pos - start));
        } else if (query[pos] == '\'') {
            pos++; // Skip opening quote
            size_t start = pos;
            while (pos < query.length() && query[pos] != '\'') {
                pos++;
            }
            if (pos >= query.length()) {
                throw std::runtime_error("Unterminated string at position " + std::to_string(start - 1));
            }
            pos++; // Skip closing quote
            return query.substr(start, pos - start - 1);
        }
        throw std::runtime_error("Invalid right-hand side at position " + std::to_string(pos));
    }

    Condition parseCondition() {
        Condition condition;
        condition.lhs = parseColumn();
        condition.comparator = parseComparator();
        condition.rhs = parseRightHandSide();
        
        // Check if it's a JOIN condition
        condition.isJoinCondition = std::holds_alternative<Condition::Column>(condition.rhs) &&
                                    !condition.lhs.table.empty() &&
                                    !std::get<Condition::Column>(condition.rhs).table.empty();
        
        return condition;
    }

    bool isKeyword(const std::string& word) {
        static const std::vector<std::string> keywords = {
            "SELECT", "FROM", "WHERE", "JOIN", "GROUP BY", "HAVING"
        };
        std::string upper = word;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        return std::any_of(keywords.begin(), keywords.end(),
                           [&upper](const std::string& keyword) {
                               return upper.compare(0, keyword.length(), keyword) == 0;
                           });
    }

    FromNode::TableRef parseTableRef() {
        FromNode::TableRef tableRef;
        tableRef.table = parseIdentifier();
        skipWhitespace();
        if (pos < query.length() && (query.substr(pos, 2) == "AS" || query.substr(pos, 2) == "as")) {
            pos += 2; // Skip "AS"
            skipWhitespace();
        }
        if (pos < query.length() && std::isalnum(query[pos]) &&(query.substr(pos, 5) != "WHERE" || query.substr(pos, 5) == "where")) {
            tableRef.alias = parseIdentifier();
            aliases[tableRef.alias] = tableRef.table;
        }
        return tableRef;
    }

public:
    SQLParser(const std::string& sql) : query(sql) {}

    std::unique_ptr<SelectNode> parseSelect() {
        auto selectNode = std::make_unique<SelectNode>();
        pos += 6; // Skip "SELECT"
        while (true) {
            skipWhitespace();
            std::string identifier = parseIdentifier();
            SelectNode::Column column;
            size_t dotPos = identifier.find('.');
            if (dotPos != std::string::npos) {
                column.table = identifier.substr(0, dotPos);
                column.name = identifier.substr(dotPos + 1);
            } else {
                column.name = identifier;
            }
            selectNode->columns.push_back(column);
            skipWhitespace();
            if (query[pos] != ',') break;
            pos++; // Skip comma
        }
        return selectNode;
    }

    std::unique_ptr<FromNode> parseFrom() {
        auto fromNode = std::make_unique<FromNode>();
        pos += 4; // Skip "FROM"
        while (true) {
            skipWhitespace();
            fromNode->tables.push_back(parseTableRef());
            skipWhitespace();
            if (query[pos] != ',') break;
            pos++; // Skip comma
        }
        return fromNode;
    }

    std::unique_ptr<WhereNode> parseWhere() {
        auto whereNode = std::make_unique<WhereNode>();
        pos += 5; // Skip "WHERE"
        do {
            skipWhitespace();
            whereNode->conditions.push_back(parseCondition());
            skipWhitespace();
        } while (pos < query.length() && query.substr(pos, 3) == "AND" && (pos += 3));
        return whereNode;
    }

    std::unique_ptr<JoinNode> parseJoin() {
        auto joinNode = std::make_unique<JoinNode>();
        pos += 4; // Skip "JOIN"
        skipWhitespace();
        joinNode->table = parseIdentifier();
        skipWhitespace();
        if (pos < query.length() && (query.substr(pos, 2) == "AS" || query.substr(pos, 2) == "as")) {
            pos += 2; // Skip "AS"
            skipWhitespace();
            joinNode->alias = parseIdentifier();
            aliases[joinNode->alias] = joinNode->table;
        }
        skipWhitespace();
        pos += 2; // Skip "ON"
        joinNode->condition = parseCondition();
        joinNode->condition.isJoinCondition = true; // Explicitly set this for JOIN nodes
        return joinNode;
    }

    std::unique_ptr<GroupByNode> parseGroupBy() {
        auto groupByNode = std::make_unique<GroupByNode>();
        pos += 8; // Skip "GROUP BY"
        while (true) {
            skipWhitespace();
            groupByNode->columns.push_back(parseIdentifier());
            skipWhitespace();
            if (query[pos] != ',') break;
            pos++; // Skip comma
        }
        return groupByNode;
    }

    std::unique_ptr<HavingNode> parseHaving() {
        auto havingNode = std::make_unique<HavingNode>();
        pos += 6; // Skip "HAVING"
        do {
            skipWhitespace();
            havingNode->conditions.push_back(parseCondition());
            skipWhitespace();
        } while (pos < query.length() && query.substr(pos, 3) == "AND" && (pos += 3));
        return havingNode;
    }

    std::vector<std::unique_ptr<ASTNode>> parse() {
        std::vector<std::unique_ptr<ASTNode>> ast;
        while (pos < query.length()) {
            skipWhitespace();
            if (pos >= query.length()) break;

            if (query[pos] == ';') {
                pos++; // Skip the semicolon
                break; // End of SQL statement
            }

            if (query.substr(pos, 6) == "SELECT") {
                ast.push_back(parseSelect());
            } else if (query.substr(pos, 4) == "FROM") {
                ast.push_back(parseFrom());
            } else if (query.substr(pos, 5) == "WHERE") {
                ast.push_back(parseWhere());
            } else if (query.substr(pos, 4) == "JOIN") {
                ast.push_back(parseJoin());
            } else if (query.substr(pos, 8) == "GROUP BY") {
                ast.push_back(parseGroupBy());
            } else if (query.substr(pos, 6) == "HAVING") {
                ast.push_back(parseHaving());
            } else {
                throw std::runtime_error("Unexpected token at position " + std::to_string(pos));
            }
        }

        skipWhitespace();
        if (pos < query.length()) {
            throw std::runtime_error("Unexpected content after semicolon at position " + std::to_string(pos));
        }

        return ast;
    }
};

/*

EXECUTOR

*/

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


// This function will be called from main.cpp
extern "C" void parseSQL(const char* sql, const Schema* schema) {
    try {
        // save start time
        auto start = std::chrono::steady_clock::now();
        SQLParser parser(sql);
        auto ast = parser.parse();
        // parsing end time
        auto parseEnd = std::chrono::steady_clock::now();

        std::cout << "Parsed SQL:" << std::endl;
        for (const auto& node : ast) {
            node->print();
        }
        std::cout << "Parsing completed successfully." << std::endl;

        // Planning and executing start time
        auto planStart = std::chrono::steady_clock::now();
        Plan queryPlan(*const_cast<Schema*>(schema));
        queryPlan.createPlan(ast);
        auto planEnd = std::chrono::steady_clock::now();
        auto resultTable = queryPlan.executePlan();

        auto executionEnd = std::chrono::steady_clock::now();

        std::cout<<"Parsing time: "<<std::chrono::duration_cast<std::chrono::microseconds>(parseEnd - start).count()<<" microseconds"<<std::endl;
        std::cout<<"Planning time: "<<std::chrono::duration_cast<std::chrono::microseconds>(planEnd - planStart).count()<<" microseconds"<<std::endl;
        std::cout<<"Execution time: "<<std::chrono::duration_cast<std::chrono::microseconds>(executionEnd - planEnd).count()<<" microseconds"<<std::endl;

        std::cout <<"Size of the result table: "<<resultTable->data.size()<<std::endl;
        resultTable->printToFile();

    } catch (const std::exception& e) {
        std::cerr << "Error parsing SQL: " << e.what() << std::endl;
    }
}
/*
SELECT director.fname, director.lname, movie.name FROM movie, movie_director, director WHERE  movie.id=movie_director.mid AND movie_director.did=director.id AND movie.id=8854;

SELECT director.fname, director.lname, movie.name FROM movie, movie_director, director WHERE movie.id=8854 AND movie_director.did=director.id AND movie.id=movie_director.mid;
*/