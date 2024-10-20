// schema.h
#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <iostream>

enum class FieldType {
    INTEGER,
    STRING
};

class Field {
private:
    std::variant<int, std::string> value;  // Holds either int or string
    FieldType type;                        // Tracks the type of the value

public:
    // Constructor for integer type
    Field(int intValue) : value(intValue), type(FieldType::INTEGER) {}

    // Constructor for string type
    Field(const std::string& strValue) : value(strValue), type(FieldType::STRING) {}

    // Set value as integer
    void setValue(int intValue) {
        value = intValue;
        type = FieldType::INTEGER;
    }

    // Set value as string
    void setValue(const std::string& strValue) {
        value = strValue;
        type = FieldType::STRING;
    }

    // Get the type of the field (INTEGER or STRING)
    FieldType getType() const {
        return type;
    }

    // Get the value as integer (throws exception if type is not integer)
    int getIntValue() const {
        if (type != FieldType::INTEGER) {
            throw std::runtime_error("Field does not contain an integer.");
        }
        return std::get<int>(value);
    }

    // Get the value as string (throws exception if type is not string)
    std::string getStringValue() const {
        if (type != FieldType::STRING) {
            throw std::runtime_error("Field does not contain a string.");
        }
        return std::get<std::string>(value);
    }

    // Print the value
    void printValue() const {
        if (type == FieldType::INTEGER) {
            std::cout << std::get<int>(value);
        } else {
            std::cout << std::get<std::string>(value);
        }
    }
};

class Column {
public:
    std::string name;
    FieldType type;

    Column(const std::string& name, const FieldType type) : name(name), type(type) {}
};

class Table {
public:
    std::string name;
    std::vector<Column> columns;
    std::vector<std::vector<Field>> data;

    Table(const std::string& name) : name(name) {}

    void addColumn(const std::string& name, const FieldType type) {
        columns.emplace_back(name, type);
    }

    void addRow(const std::vector<Field>& row) {
        if (row.size() != columns.size()) {
            throw std::runtime_error("Row size does not match column count for table " + name);
        }
        // Verify the data types match
        for (size_t i = 0; i < row.size(); ++i) {
            if (row[i].getType() != columns[i].type) {
                throw std::runtime_error("Data type mismatch for column " + columns[i].name + " in table " + name);
            }
        }
        data.push_back(row);
    }

    void print(int limit = 5) const;
};

class Schema {
public:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;

    void addTable(const std::string& name, std::shared_ptr<Table> table) {
        tables[name] = table;
    }

    std::shared_ptr<Table> getTable(const std::string& name) const {
        auto it = tables.find(name);
        if (it == tables.end()) {
            throw std::runtime_error("Table not found: " + name);
        }
        return it->second;
    }

    size_t getTableSize(const std::string& name) const {
        auto table = getTable(name);
        return table->data.size();
    }

    void printTableColumns(const std::string& name) const;

    void print() const;
};

#endif // SCHEMA_H