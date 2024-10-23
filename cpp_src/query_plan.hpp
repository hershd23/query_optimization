#ifndef QUERY_PLAN_HPP
#define QUERY_PLAN_HPP

#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <functional>
#include <iostream>

class Schema;  // Forward declaration

// Define a Row type to represent a single row of data
using Row = std::vector<std::variant<int, double, std::string>>;

// Define a Table type to represent a collection of rows
using Table = std::vector<Row>;

// Abstract base class for logical plan nodes
class LogicalPlanNode {
public:
    virtual ~LogicalPlanNode() = default;
    virtual void print(int indent = 0) const = 0;
};

// Logical plan node for SELECT operations
class LogicalSelect : public LogicalPlanNode {
public:
    std::vector<std::string> columns;
    std::unique_ptr<LogicalPlanNode> input;

    LogicalSelect(std::vector<std::string> cols, std::unique_ptr<LogicalPlanNode> in)
        : columns(std::move(cols)), input(std::move(in)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "LogicalSelect: ";
        for (const auto& col : columns) {
            std::cout << col << " ";
        }
        std::cout << std::endl;
        input->print(indent + 2);
    }
};

// Logical plan node for FROM operations
class LogicalFrom : public LogicalPlanNode {
public:
    std::string table_name;

    explicit LogicalFrom(std::string name) : table_name(std::move(name)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "LogicalFrom: " << table_name << std::endl;
    }
};

// Logical plan node for WHERE operations
class LogicalFilter : public LogicalPlanNode {
public:
    std::string condition;
    std::unique_ptr<LogicalPlanNode> input;

    LogicalFilter(std::string cond, std::unique_ptr<LogicalPlanNode> in)
        : condition(std::move(cond)), input(std::move(in)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "LogicalFilter: " << condition << std::endl;
        input->print(indent + 2);
    }
};

// Logical plan node for JOIN operations
class LogicalJoin : public LogicalPlanNode {
public:
    std::unique_ptr<LogicalPlanNode> left;
    std::unique_ptr<LogicalPlanNode> right;
    std::string condition;

    LogicalJoin(std::unique_ptr<LogicalPlanNode> l, std::unique_ptr<LogicalPlanNode> r, std::string cond)
        : left(std::move(l)), right(std::move(r)), condition(std::move(cond)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "LogicalJoin: " << condition << std::endl;
        left->print(indent + 2);
        right->print(indent + 2);
    }
};

// Abstract base class for physical plan nodes
class PhysicalPlanNode {
public:
    virtual ~PhysicalPlanNode() = default;
    virtual void print(int indent = 0) const = 0;
    virtual Table execute(const Schema& schema) const = 0;
};

// Physical plan node for SELECT operations
class PhysicalSelect : public PhysicalPlanNode {
public:
    std::vector<std::string> columns;
    std::unique_ptr<PhysicalPlanNode> input;

    PhysicalSelect(std::vector<std::string> cols, std::unique_ptr<PhysicalPlanNode> in)
        : columns(std::move(cols)), input(std::move(in)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "PhysicalSelect: ";
        for (const auto& col : columns) {
            std::cout << col << " ";
        }
        std::cout << std::endl;
        input->print(indent + 2);
    }

    Table execute(const Schema& schema) const override {
        Table inputTable = input->execute(schema);
        Table result;
        for (const auto& row : inputTable) {
            Row newRow;
            for (const auto& col : columns) {
                // Here you would use the schema to find the correct index for each column
                // and add the corresponding value from the input row to the new row
                // For simplicity, we'll just add all columns for now
                newRow = row;
            }
            result.push_back(newRow);
        }
        return result;
    }
};

// Physical plan node for FROM operations
class PhysicalScan : public PhysicalPlanNode {
public:
    std::string table_name;

    explicit PhysicalScan(std::string name) : table_name(std::move(name)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "PhysicalScan: " << table_name << std::endl;
    }

    Table execute(const Schema& schema) const override {
        return schema.getTable(table_name);
    }
};

// Physical plan node for WHERE operations
class PhysicalFilter : public PhysicalPlanNode {
public:
    std::function<bool(const Row&, const Schema&)> condition;
    std::unique_ptr<PhysicalPlanNode> input;

    PhysicalFilter(std::function<bool(const Row&, const Schema&)> cond, std::unique_ptr<PhysicalPlanNode> in)
        : condition(std::move(cond)), input(std::move(in)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "PhysicalFilter" << std::endl;
        input->print(indent + 2);
    }

    Table execute(const Schema& schema) const override {
        Table inputTable = input->execute(schema);
        Table result;
        for (const auto& row : inputTable) {
            if (condition(row, schema)) {
                result.push_back(row);
            }
        }
        return result;
    }
};

// Physical plan node for JOIN operations
class PhysicalHashJoin : public PhysicalPlanNode {
public:
    std::unique_ptr<PhysicalPlanNode> left;
    std::unique_ptr<PhysicalPlanNode> right;
    std::string leftKey;
    std::string rightKey;

    PhysicalHashJoin(std::unique_ptr<PhysicalPlanNode> l, std::unique_ptr<PhysicalPlanNode> r,
                     std::string lk, std::string rk)
        : left(std::move(l)), right(std::move(r)), leftKey(std::move(lk)), rightKey(std::move(rk)) {}

    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "PhysicalHashJoin: " << leftKey << " = " << rightKey << std::endl;
        left->print(indent + 2);
        right->print(indent + 2);
    }

    Table execute(const Schema& schema) const override {
        Table leftTable = left->execute(schema);
        Table rightTable = right->execute(schema);
        Table result;

        // Implement hash join logic here
        // For simplicity, we'll use a nested loop join for now
        for (const auto& leftRow : leftTable) {
            for (const auto& rightRow : rightTable) {
                // Compare join keys
                if (schema.getColumnValue(leftRow, "", leftKey) == schema.getColumnValue(rightRow, "", rightKey)) {
                    // Combine matching rows
                    Row combinedRow = leftRow;
                    combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
                    result.push_back(combinedRow);
                }
            }
        }

        return result;
    }
};

#endif // QUERY_PLAN_HPP