// parser.cpp
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <cctype>
#include <algorithm>

// Forward declaration of Schema class from dataloader.cpp
class Schema;

class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual void print(int indent = 0) const = 0;
};

class SelectNode : public ASTNode {
public:
    std::vector<std::string> columns;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "SELECT" << std::endl;
        for (const auto& col : columns) {
            std::cout << std::string(indent + 2, ' ') << col << std::endl;
        }
    }
};

class FromNode : public ASTNode {
public:
    std::string table;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "FROM " << table << std::endl;
    }
};

class WhereNode : public ASTNode {
public:
    std::string condition;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "WHERE " << condition << std::endl;
    }
};

class JoinNode : public ASTNode {
public:
    std::string table;
    std::string condition;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "JOIN " << table << " ON " << condition << std::endl;
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
    std::string condition;
    void print(int indent = 0) const override {
        std::cout << std::string(indent, ' ') << "HAVING " << condition << std::endl;
    }
};

class SQLParser {
private:
    std::string query;
    size_t pos = 0;

    void skipWhitespace() {
        while (pos < query.length() && std::isspace(query[pos])) {
            pos++;
        }
    }

    std::string parseIdentifier() {
        skipWhitespace();
        size_t start = pos;
        while (pos < query.length() && (std::isalnum(query[pos]) || query[pos] == '_')) {
            pos++;
        }
        return query.substr(start, pos - start);
    }

    std::string parseCondition() {
        skipWhitespace();
        size_t start = pos;
        int parentheses = 0;
        while (pos < query.length()) {
            if (query[pos] == '(') parentheses++;
            else if (query[pos] == ')') parentheses--;
            else if (query[pos] == ';' || (parentheses == 0 && isKeyword(query.substr(pos)))) break;
            pos++;
        }
        return query.substr(start, pos - start);
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

public:
    SQLParser(const std::string& sql) : query(sql) {}

    std::unique_ptr<SelectNode> parseSelect() {
        auto selectNode = std::make_unique<SelectNode>();
        pos += 6; // Skip "SELECT"
        while (true) {
            skipWhitespace();
            selectNode->columns.push_back(parseIdentifier());
            skipWhitespace();
            if (query[pos] != ',') break;
            pos++; // Skip comma
        }
        return selectNode;
    }

    std::unique_ptr<FromNode> parseFrom() {
        auto fromNode = std::make_unique<FromNode>();
        pos += 4; // Skip "FROM"
        skipWhitespace();
        fromNode->table = parseIdentifier();
        return fromNode;
    }

    std::unique_ptr<WhereNode> parseWhere() {
        auto whereNode = std::make_unique<WhereNode>();
        pos += 5; // Skip "WHERE"
        whereNode->condition = parseCondition();
        return whereNode;
    }

    std::unique_ptr<JoinNode> parseJoin() {
        auto joinNode = std::make_unique<JoinNode>();
        pos += 4; // Skip "JOIN"
        skipWhitespace();
        joinNode->table = parseIdentifier();
        skipWhitespace();
        pos += 2; // Skip "ON"
        joinNode->condition = parseCondition();
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
        havingNode->condition = parseCondition();
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

// This function will be called from main.cpp
extern "C" void parseSQL(const char* sql, const Schema* schema) {
    try {
        SQLParser parser(sql);
        auto ast = parser.parse();
        std::cout << "Parsed SQL:" << std::endl;
        for (const auto& node : ast) {
            node->print();
        }
        std::cout << "Parsing completed successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error parsing SQL: " << e.what() << std::endl;
    }
}