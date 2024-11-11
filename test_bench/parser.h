// parser.h
#pragma once
#include "schema.h"
#include <string>
#include <vector>
#include <memory>
#include <sstream>

class Component {
public:
    virtual ~Component() = default;
    virtual void print() const = 0;
};

class TableComponent : public Component {
public:
    std::string tableName;

    explicit TableComponent(const std::string& tableName) 
        : tableName(tableName) {}

    void print() const override {
        std::cout << "Table: " << tableName << std::endl;
    }
};

class FilterComponent : public Component {
public:
    std::string lhsTable;
    std::string lhsColumn;
    Predicate::Op predicate;

    FilterComponent(const std::string& lhsTable, const std::string& lhsColumn, 
                   Predicate::Op predicate)
        : lhsTable(lhsTable), lhsColumn(lhsColumn), predicate(predicate) {}

    static Predicate::Op stringToPredicate(const std::string& pred) {
        if (pred == "=") return Predicate::Op::EQUALS;
        if (pred == ">") return Predicate::Op::GREATER_THAN;
        if (pred == "<") return Predicate::Op::LESS_THAN;
        if (pred == ">=") return Predicate::Op::GREATER_THAN_OR_EQ;
        if (pred == "<=") return Predicate::Op::LESS_THAN_OR_EQ;
        if (pred == "!=") return Predicate::Op::NOT_EQUALS;
        throw std::runtime_error("Invalid predicate: " + pred);
    }

    static std::string predicateToString(Predicate::Op pred) {
        switch (pred) {
            case Predicate::Op::EQUALS: return "=";
            case Predicate::Op::GREATER_THAN: return ">";
            case Predicate::Op::LESS_THAN: return "<";
            case Predicate::Op::GREATER_THAN_OR_EQ: return ">=";
            case Predicate::Op::LESS_THAN_OR_EQ: return "<=";
            case Predicate::Op::NOT_EQUALS: return "!=";
            default: return "UNKNOWN";
        }
    }
};

class ScalarFilterComponent : public FilterComponent {
public:
    Field rhsValue;

    ScalarFilterComponent(const std::string& lhsTable, const std::string& lhsColumn,
                         Predicate::Op predicate, const Field& rhsValue)
        : FilterComponent(lhsTable, lhsColumn, predicate), rhsValue(rhsValue) {}

    void print() const override {
        if (rhsValue.getType() == FieldType::INTEGER) {
            std::cout << "Scalar Filter: " << lhsTable << "." << lhsColumn 
                 << " " << predicateToString(predicate) << " " 
                 << rhsValue.getIntValue() << std::endl;
        }
        else if(rhsValue.getType() == FieldType::STRING) {
            std::cout << "Scalar Filter: " << lhsTable << "." << lhsColumn 
                 << " " << predicateToString(predicate) << " " 
                 << rhsValue.getStringValue() << std::endl;
        }
    }
};

class DynamicFilterComponent : public FilterComponent {
public:
    std::string rhsTable;
    std::string rhsColumn;

    DynamicFilterComponent(const std::string& lhsTable, const std::string& lhsColumn,
                          Predicate::Op predicate, const std::string& rhsTable, 
                          const std::string& rhsColumn)
        : FilterComponent(lhsTable, lhsColumn, predicate), 
          rhsTable(rhsTable), rhsColumn(rhsColumn) {}

    void print() const override {
        std::cout << "Dynamic Filter: " << lhsTable << "." << lhsColumn 
                 << " " << predicateToString(predicate) << " " 
                 << rhsTable << "." << rhsColumn << std::endl;
    }
};

class JoinComponent : public FilterComponent {
public:
    std::string rhsTable;
    std::string rhsColumn;

    JoinComponent(const std::string& lhsTable, const std::string& lhsColumn,
                 Predicate::Op predicate, const std::string& rhsTable, 
                 const std::string& rhsColumn)
        : FilterComponent(lhsTable, lhsColumn, predicate), 
          rhsTable(rhsTable), rhsColumn(rhsColumn) {}

    void print() const override {
        std::cout << "Join: " << lhsTable << "." << lhsColumn 
                 << " " << predicateToString(predicate) << " " 
                 << rhsTable << "." << rhsColumn << std::endl;
    }
};

class QueryComponents {
public:
    std::vector<std::shared_ptr<TableComponent>> tables;
    std::vector<std::shared_ptr<ScalarFilterComponent>> scalarFilters;
    std::vector<std::shared_ptr<DynamicFilterComponent>> dynamicFilters;
    std::vector<std::shared_ptr<JoinComponent>> joins;

    void print() const {
        std::cout << "\n=== Query Components ===\n";
        
        std::cout << "Tables:\n";
        for (const auto& table : tables) {
            std::cout << "  ";
            table->print();
        }
        
        std::cout << "\nScalar Filters:\n";
        if (scalarFilters.empty()) {
            std::cout << "  (none)\n";
        } else {
            for (const auto& filter : scalarFilters) {
                std::cout << "  ";
                filter->print();
            }
        }
        
        std::cout << "\nDynamic Filters:\n";
        if (dynamicFilters.empty()) {
            std::cout << "  (none)\n";
        } else {
            for (const auto& filter : dynamicFilters) {
                std::cout << "  ";
                filter->print();
            }
        }
        
        std::cout << "\nJoins:\n";
        if (joins.empty()) {
            std::cout << "  (none)\n";
        } else {
            for (const auto& join : joins) {
                std::cout << "  ";
                join->print();
            }
        }
        std::cout << "=====================\n";
    }
};

class SimpleParser {
private:
    static void trim(std::string& s) {
        s.erase(0, s.find_first_not_of(" \t\n\r\f\v"));
        s.erase(s.find_last_not_of(" \t\n\r\f\v") + 1);
    }

    static std::vector<std::string> splitAndTrim(const std::string& input, char delimiter) {
        std::vector<std::string> result;
        std::stringstream ss(input);
        std::string item;
        
        while (std::getline(ss, item, delimiter)) {
            trim(item);
            if (!item.empty()) {
                result.push_back(item);
            }
        }
        return result;
    }

    static std::pair<std::string, std::string> parseTableColumn(const std::string& input) {
        size_t dotPos = input.find('.');
        if (dotPos == std::string::npos) {
            throw std::runtime_error("Invalid table.column format: " + input);
        }
        return {input.substr(0, dotPos), input.substr(dotPos + 1)};
    }

public:
    static QueryComponents parse(const std::vector<std::string>& queryLines, Schema* schema);
    static void validateQuery(QueryComponents& components, Schema* schema);
};