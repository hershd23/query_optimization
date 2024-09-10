#include "learned_index.h"
#include <algorithm>
#include <numeric>
#include <cmath>

void LinearRegression::fit(const std::vector<double>& X, const std::vector<double>& y) {
    double n = X.size();
    double sum_X = std::accumulate(X.begin(), X.end(), 0.0);
    double sum_y = std::accumulate(y.begin(), y.end(), 0.0);
    double sum_XY = std::inner_product(X.begin(), X.end(), y.begin(), 0.0);
    double sum_X2 = std::inner_product(X.begin(), X.end(), X.begin(), 0.0);

    slope = (n * sum_XY - sum_X * sum_y) / (n * sum_X2 - sum_X * sum_X);
    intercept = (sum_y - slope * sum_X) / n;
}

double LinearRegression::predict(double x) const {
    return slope * x + intercept;
}

LearnedIndex::LearnedIndex(const std::vector<int>& input_data) : data(input_data), operations(0) {
    std::vector<double> X(data.size());
    std::vector<double> y(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        X[i] = i;
        y[i] = data[i];
    }
    model.fit(X, y);
}

int LearnedIndex::binary_search(int key, int left, int right) const {
    while (left <= right) {
        ++operations;
        int mid = left + (right - left) / 2;

        if (data[mid] == key) {
            return mid;
        }

        if (data[mid] < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return -1;
}

int LearnedIndex::search(int key) const {
    operations = 0;
    double predicted_pos = model.predict(key);
    int pos = std::round(predicted_pos);
    pos = std::max(0, std::min(pos, static_cast<int>(data.size()) - 1));

    // Define a search range around the predicted position
    int search_range = std::max(1, static_cast<int>(std::sqrt(data.size()))); // You can adjust this
    int left = std::max(0, pos - search_range);
    int right = std::min(static_cast<int>(data.size()) - 1, pos + search_range);

    // Perform binary search within the range
    return binary_search(key, left, right);
}