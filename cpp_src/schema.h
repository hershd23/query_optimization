// schema.h
#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <iostream>
#include <stdexcept>


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
        std::cout << "Value: ";
        if (type == FieldType::INTEGER) {
            std::cout << std::get<int>(value);
        } else {
            std::cout << std::get<std::string>(value);
        }
    }

    // Equality operator
    bool operator==(const Field& other) const {
        if (type != other.type) {
            std::string lhsType = (type == FieldType::INTEGER) ? "INTEGER" : "STRING";
            std::string rhsType = (other.type == FieldType::INTEGER) ? "INTEGER" : "STRING";
            throw std::runtime_error("Cannot compare fields of different types LHS type is " + lhsType + " RHS type is " + rhsType);
        }
        
        if (type == FieldType::INTEGER) {
            return std::get<int>(value) == std::get<int>(other.value);
        } else {
            return std::get<std::string>(value) == std::get<std::string>(other.value);
        }
    }

    // Less than operator
    bool operator<(const Field& other) const {
        if (type != other.type) {
            throw std::runtime_error("Cannot compare fields of different types");
        }
        
        if (type == FieldType::INTEGER) {
            return std::get<int>(value) < std::get<int>(other.value);
        } else {
            return std::get<std::string>(value) < std::get<std::string>(other.value);
        }
    }

    // Greater than operator
    bool operator>(const Field& other) const {
        if (type != other.type) {
            throw std::runtime_error("Cannot compare fields of different types");
        }
        
        if (type == FieldType::INTEGER) {
            return std::get<int>(value) > std::get<int>(other.value);
        } else {
            return std::get<std::string>(value) > std::get<std::string>(other.value);
        }
    }

    // Less than or equal operator
    bool operator<=(const Field& other) const {
        if (type != other.type) {
            throw std::runtime_error("Cannot compare fields of different types");
        }
        
        if (type == FieldType::INTEGER) {
            return std::get<int>(value) <= std::get<int>(other.value);
        } else {
            return std::get<std::string>(value) <= std::get<std::string>(other.value);
        }
    }

    // Greater than or equal operator
    bool operator>=(const Field& other) const {
        if (type != other.type) {
            throw std::runtime_error("Cannot compare fields of different types");
        }
        
        if (type == FieldType::INTEGER) {
            return std::get<int>(value) >= std::get<int>(other.value);
        } else {
            return std::get<std::string>(value) >= std::get<std::string>(other.value);
        }
    }
};

class Predicate {
public:
    enum class Op {
        EQUALS,
        GREATER_THAN,
        LESS_THAN,
        LESS_THAN_OR_EQ,
        GREATER_THAN_OR_EQ,
        NOT_EQUALS
    };

    Predicate(const std::string& field, Op op, const Field& operand)
        : field(field), op(op), operand(operand) {}

    std::string getField() const { return field; }
    Op getOp() const { return op; }
    const Field& getOperand() const { return operand; }

private:
    std::string field;
    Op op;
    Field operand;
};

class IntHistogram {
private:
    std::vector<int> buckets;
    int minVal;
    int maxVal;
    int bucketSize;
    int totalValues;

public:
    IntHistogram(int numBuckets, int min, int max) 
        : buckets(numBuckets, 0), minVal(min), maxVal(max), totalValues(0) {
        bucketSize = std::max(1, (max - min + 1) / numBuckets);
    }

    void addValue(int value) {
        if (value >= minVal && value <= maxVal) {
            int bucketIndex = (value - minVal) / bucketSize;
            bucketIndex = std::min(bucketIndex, static_cast<int>(buckets.size()) - 1);
            buckets[bucketIndex]++;
            totalValues++;
        }
    }

    double estimateSelectivity(Predicate::Op op, int value) const {
        value = std::min(maxVal, std::max(minVal, value));
        int bucketIndex = (value - minVal) / bucketSize;
        bucketIndex = std::min(bucketIndex, static_cast<int>(buckets.size()) - 1);

        switch (op) {
            case Predicate::Op::EQUALS:
                {

                    return buckets[bucketIndex] / static_cast<double>(totalValues);
                }
            case Predicate::Op::GREATER_THAN:
                {
                    int count = 0;
                    //for (size_t i = 0; i < buckets.size(); ++i) {
                    for (size_t i = bucketIndex; i < buckets.size(); ++i) {
                        count += buckets[i];
                    }
                    return count / static_cast<double>(totalValues);
                }
            case Predicate::Op::LESS_THAN:
                {
                    int count = 0;
                    for (int i = 0; i <= bucketIndex; ++i) {
                        count += buckets[i];
                    }
                    return count / static_cast<double>(totalValues);
                }
            // Implement other operations as needed
            default:
                throw std::runtime_error("Unsupported operation for selectivity estimation");
        }
    }

    double avgSelectivity() const {
        return 1.0 / buckets.size();
    }
};

class StringHistogram {
private:
    IntHistogram hist;

    static int stringToInt(const std::string& s) {
        int v = 0;
        for (size_t i = 0; i < 4; ++i) {
            if (i < s.length()) {
                v += static_cast<int>(s[i]) << ((3 - i) * 8);
            }
        }

        if (!(s.empty() || s == "zzzz")) {
            v = std::max(v, minVal());
            v = std::min(v, maxVal());
        }

        return v;
    }

    static int minVal() {
        return stringToInt("");
    }

    static int maxVal() {
        return stringToInt("zzzz");
    }

public:
    StringHistogram(int buckets) : hist(buckets, minVal(), maxVal()) {}

    void addValue(const std::string& s) {
        int val = stringToInt(s);
        hist.addValue(val);
    }

    double estimateSelectivity(Predicate::Op op, const std::string& s) {
        int val = stringToInt(s);
        return hist.estimateSelectivity(op, val);
    }

    double avgSelectivity() {
        return hist.avgSelectivity();
    }
};

class Column {
public:
    std::string name;
    std::string tableName;
    FieldType type;
    std::unique_ptr<IntHistogram> intHistogram;
    std::unique_ptr<StringHistogram> stringHistogram;

    Column(const std::string& name, const std::string& tableName, const FieldType type) : name(name), tableName(tableName), type(type) {

        if (type == FieldType::INTEGER) {
            intHistogram = std::make_unique<IntHistogram>(2000, 0, 1000000);
        } else if (type == FieldType::STRING) {
            stringHistogram = std::make_unique<StringHistogram>(200);
        }
    }

    // Delete copy constructor and copy assignment operator
    Column(const Column&) = delete;
    Column& operator=(const Column&) = delete;

    // Implement move constructor and move assignment operator
    Column(Column&& other) noexcept
        : name(std::move(other.name)), tableName(std::move(other.tableName)) , type(other.type),
          intHistogram(std::move(other.intHistogram)),
          stringHistogram(std::move(other.stringHistogram)) {}

    Column& operator=(Column&& other) noexcept {
        if (this != &other) {
            name = std::move(other.name);
            tableName = std::move(other.tableName);
            type = other.type;
            intHistogram = std::move(other.intHistogram);
            stringHistogram = std::move(other.stringHistogram);
        }
        return *this;
    }
};

class Table {
public:
    std::string name;
    std::vector<Column> columns;
    std::vector<std::vector<Field>> data;

    Table(const std::string& name) : name(name) {}

    void addColumn(const std::string& name, const std::string& tableName, const FieldType type) {
        std::cout<<"Adding "<<name<<" and name "<<tableName<<std::endl;
        columns.emplace_back(name, tableName, type);
    }

    void addRow(const std::vector<Field>& row) {
        if (row.size() != columns.size()) {
            throw std::runtime_error("Row size does not match column count for table " + name);
        }
        for (size_t i = 0; i < row.size(); ++i) {
            if (row[i].getType() != columns[i].type) {
                throw std::runtime_error("Data type mismatch for column " + columns[i].name + " in table " + name);
            }
            if (columns[i].type == FieldType::INTEGER) {
                if (!columns[i].intHistogram) {
                    throw std::runtime_error("intHistogram is not initialized for column " + columns[i].name);
                }
                columns[i].intHistogram->addValue(row[i].getIntValue());
            } else if (columns[i].type == FieldType::STRING) {
                if (!columns[i].stringHistogram) {
                    throw std::runtime_error("stringHistogram is not initialized for column " + columns[i].name);
                }
                columns[i].stringHistogram->addValue(row[i].getStringValue());
            }
        }
        data.push_back(row);
    }

    int getColumnIndex(const std::string& columnName, const std::string& tableName) const {
        for (size_t i = 0; i < columns.size(); ++i) {
            std::cout<<"Column name: "<<columns[i].name<<" Column table: "<<columns[i].tableName <<"Searching for tableName: "<<tableName<<" Column name:"<<std::endl;
            if (columns[i].name == columnName && columns[i].tableName == tableName) {
                return i;
            }
        }
        throw std::runtime_error("Column not found: " + columnName);
    }

    void print(int limit = 5) const;

    void printToFile() const;

    double estimateSelectivity(const std::string& columnName, Predicate::Op op, const Field& value) const {
        auto it = std::find_if(columns.begin(), columns.end(), 
                               [&columnName](const Column& col) { return col.name == columnName; });
        if (it == columns.end()) {
            throw std::runtime_error("Column not found: " + columnName);
        }

        if (it->type == FieldType::INTEGER) {
            return it->intHistogram->estimateSelectivity(op, value.getIntValue());
        } else if (it->type == FieldType::STRING) {
            return it->stringHistogram->estimateSelectivity(op, value.getStringValue());
        }

        throw std::runtime_error("Unsupported field type for selectivity estimation");
    }

    // Table is now move-only
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    // Implement move constructor for Table
    Table(Table&& other) noexcept
        : name(std::move(other.name)),
          columns(std::move(other.columns)),
          data(std::move(other.data)) {}

    // Implement move assignment operator for Table
    Table& operator=(Table&& other) noexcept {
        if (this != &other) {
            name = std::move(other.name);
            columns = std::move(other.columns);
            data = std::move(other.data);
        }
        return *this;
    }

    // Add a method to get a const reference to the columns
    const std::vector<Column>& getColumns() const {
        return columns;
    }

    void recomputeHistogramsForIntegerColumn() {
        size_t columnIndex = 0;
        for (Column& column : columns) {
            if (column.type == FieldType::INTEGER) {
                // Find min and max values for the integer column
                int minVal = INT_MAX;
                int maxVal = INT_MIN;

                for (const auto& row : data) {
                    int currentVal = row[columnIndex].getIntValue();
                    if (currentVal < minVal) minVal = currentVal;
                    if (currentVal > maxVal) maxVal = currentVal;
                }

                // Recreate the histogram with the new min and max values
                column.intHistogram = std::make_unique<IntHistogram>(2000, minVal, maxVal);

                std::cout<<"New min and max values for column: "<<column.name<<" are: " <<minVal<<" "<<maxVal<<std::endl;

                // Populate the histogram with data from the column
                for (const auto& row : data) {
                    column.intHistogram->addValue(row[columnIndex].getIntValue());
                }
            }
            ++columnIndex;
        }
    }
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