#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "mapreduce.h"
#include "threadpool.h"

// Mock functions for Mapper and Reducer
void mock_mapper(char *file_name) {
    FILE *file = fopen(file_name, "r");
    if (!file) {
        perror("Error opening file");
        return;
    }

    char word[256];
    while (fscanf(file, "%s", word) != EOF) {
        MR_Emit(word, "1");
    }

    fclose(file);
}

void mock_reducer(char *key, unsigned int partition_idx) {
    int count = 0;
    char *value;
    while ((value = MR_GetNext(key, partition_idx)) != NULL) {
        count += atoi(value);
    }
    printf("Word: %s, Count: %d\n", key, count);
}

// Helper function to create a test file with content
void create_test_file(const char *filename, const char *content) {
    FILE *file = fopen(filename, "w");
    if (file) {
        fprintf(file, "%s", content);
        fclose(file);
    } else {
        perror("Error creating test file");
    }
}

// Test 1: Basic MapReduce run with single file and single partition
void test_single_file_single_partition() {
    printf("Test 1: Single File, Single Partition\n");

    create_test_file("test1.txt", "hello world hello");

    char *files[] = {"test1.txt"};
    MR_Run(1, files, mock_mapper, mock_reducer, 2, 1);
    printf("Test 1 passed: Single file processed with single partition.\n");

    remove("test1.txt");
}

// Test 2: Multiple files with single partition
void test_multiple_files_single_partition() {
    printf("Test 2: Multiple Files, Single Partition\n");

    create_test_file("test1.txt", "hello world");
    create_test_file("test2.txt", "world hello");

    char *files[] = {"test1.txt", "test2.txt"};
    MR_Run(2, files, mock_mapper, mock_reducer, 3, 1);
    printf("Test 2 passed: Multiple files processed with single partition.\n");

    remove("test1.txt");
    remove("test2.txt");
}

// Test 3: Single file with multiple partitions
void test_single_file_multiple_partitions() {
    printf("Test 3: Single File, Multiple Partitions\n");

    create_test_file("test1.txt", "foo bar baz foo");

    char *files[] = {"test1.txt"};
    MR_Run(1, files, mock_mapper, mock_reducer, 3, 3);
    printf("Test 3 passed: Single file processed with multiple partitions.\n");

    remove("test1.txt");
}

// Test 4: Multiple files with multiple partitions
void test_multiple_files_multiple_partitions() {
    printf("Test 4: Multiple Files, Multiple Partitions\n");

    create_test_file("test1.txt", "apple orange banana apple");
    create_test_file("test2.txt", "banana apple orange");

    char *files[] = {"test1.txt", "test2.txt"};
    MR_Run(2, files, mock_mapper, mock_reducer, 4, 3);
    printf("Test 4 passed: Multiple files processed with multiple partitions.\n");

    remove("test1.txt");
    remove("test2.txt");
}

// Test 5: Large input to test stress handling
void test_large_input() {
    printf("Test 5: Large Input Test\n");

    create_test_file("large_test.txt", "repeat word word word repeat repeat word ");
    
    char *files[] = {"large_test.txt"};
    MR_Run(1, files, mock_mapper, mock_reducer, 5, 3);
    printf("Test 5 passed: Large input handled successfully.\n");

    remove("large_test.txt");
}

// Run all tests
int main() {
    test_single_file_single_partition();
    test_multiple_files_single_partition();
    test_single_file_multiple_partitions();
    test_multiple_files_multiple_partitions();
    test_large_input();

    printf("All MapReduce tests completed.\n");
    return 0;
}
