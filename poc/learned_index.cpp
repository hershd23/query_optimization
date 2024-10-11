#include "learned_index.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <string>

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

// Linear search
int LearnedIndex::linear_search(int key, int pos) const {
    int limit = 10;
    int left_limit = 0;
    int right_limit = 0;
    if(data[pos] == key) {
        return pos;
    }
    while(data[pos] > key && pos >= 0 && left_limit < limit) {
        ++operations;
        --pos;
        ++left_limit;
    }
    while(data[pos] < key && pos < data.size() && right_limit < limit) {
        ++operations;
        ++pos;
        ++right_limit;
    }

    if (pos < data.size() && pos >= 0 && data[pos] == key) {
        return pos;
    }

    return -1;
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


    /**
     * @brief Search for a key in the learned index using the specified search type.
     * 
     * The search first predicts the position of the key using the linear model,
     * then performs a binary search within a range of elements around the
     * predicted position. The search range is set to the square root of the
     * size of the data.
     * 
     * @param key The key to search for.
     * @param type The type of search to perform. Currently, only "linear" is supported.
     * @return The index of the element if found, -1 otherwise.
     */
int LearnedIndex::search(int key, std::string type) const {
    operations = 0;
    double predicted_pos = model.predict(key);
    int pos = std::round(predicted_pos);
    pos = std::max(0, std::min(pos, static_cast<int>(data.size()) - 1));

    if (type == "linear") {
        return linear_search(key, pos);
    }

    // Define a search range around the predicted position
    int search_range = std::max(1, static_cast<int>(std::sqrt(data.size()))); // You can adjust this
    int left = std::max(0, pos - search_range);
    int right = std::min(static_cast<int>(data.size()) - 1, pos + search_range);

    // Perform binary search within the range
    return binary_search(key, left, right);
}