#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include "binary_search.h"
#include "learned_index.h"

std::vector<int> generate_random_data(int size, int max_value) {
    std::vector<int> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, max_value);

    for (int& num : data) {
        num = dis(gen);
    }

    std::sort(data.begin(), data.end());
    return data;
}

int main() {
    const int DATA_SIZE = 1000000;
    const int MAX_VALUE = 2000000;
    const int NUM_SEARCHES = 1000;

    std::vector<int> data = generate_random_data(DATA_SIZE, MAX_VALUE);
    LearnedIndex learned_index(data);

    std::vector<int> search_keys = generate_random_data(NUM_SEARCHES, MAX_VALUE);

    // Learned Index Search
    int learned_total_ops = 0;
    auto learned_start = std::chrono::high_resolution_clock::now();

    for (int key : search_keys) {
        learned_index.search(key);
        learned_total_ops += learned_index.operations;
    }

    auto learned_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> learned_duration = learned_end - learned_start;

    // Binary Search
    int binary_total_ops = 0;
    auto binary_start = std::chrono::high_resolution_clock::now();

    for (int key : search_keys) {
        int ops;
        binary_search(data, key, ops);
        binary_total_ops += ops;
    }

    auto binary_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> binary_duration = binary_end - binary_start;

    // Print results
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Comparison of Learned Index vs Binary Search" << std::endl;
    std::cout << "Data size: " << DATA_SIZE << ", Searches performed: " << NUM_SEARCHES << std::endl;
    std::cout << std::endl;
    std::cout << "Learned Index (Simple linear regression + binary search):" << std::endl;
    std::cout << "  Total operations: " << learned_total_ops << std::endl;
    std::cout << "  Avg operations per search: " << static_cast<double>(learned_total_ops) / NUM_SEARCHES << std::endl;
    std::cout << "  Total time: " << learned_duration.count() << " ms" << std::endl;
    std::cout << "  Avg time per search: " << learned_duration.count() / NUM_SEARCHES << " ms" << std::endl;
    std::cout << std::endl;
    std::cout << "Binary Search:" << std::endl;
    std::cout << "  Total operations: " << binary_total_ops << std::endl;
    std::cout << "  Avg operations per search: " << static_cast<double>(binary_total_ops) / NUM_SEARCHES << std::endl;
    std::cout << "  Total time: " << binary_duration.count() << " ms" << std::endl;
    std::cout << "  Avg time per search: " << binary_duration.count() / NUM_SEARCHES << " ms" << std::endl;

    return 0;
}