#ifndef BINARY_SEARCH_H
#define BINARY_SEARCH_H

#include <vector>

int binary_search(const std::vector<int>& data, int key, int& operations) {
    operations = 0;
    int left = 0;
    int right = data.size() - 1;

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

#endif // BINARY_SEARCH_H