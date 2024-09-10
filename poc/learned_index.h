#ifndef LEARNED_INDEX_H
#define LEARNED_INDEX_H

#include <vector>

class LinearRegression {
private:
    double slope;
    double intercept;

public:
    void fit(const std::vector<double>& X, const std::vector<double>& y);
    double predict(double x) const;
};

class LearnedIndex {
private:
    std::vector<int> data;
    LinearRegression model;
    int binary_search(int key, int left, int right) const;

public:
    mutable int operations;

    LearnedIndex(const std::vector<int>& input_data);
    int search(int key) const;
};

#endif // LEARNED_INDEX_H