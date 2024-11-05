#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "mapreduce.h"
#include "threadpool.h"

// Custom Mapper and Reducer functions for testing
void test_mapper(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    if (!fp) {
        perror("Error opening file");
        return;
    }

    char word[256];
    while (fscanf(fp, "%s", word) != EOF) {
        MR_Emit(word, "1");
    }

    fclose(fp);
}

typedef struct {
    char word[256];
    int count;
} WordCount;

// Global variable to store reduce results for verification
WordCount reduce_results[256];
int reduce_result_count = 0;

void test_reducer(char *key, unsigned int partition_idx) {
    int count = 0;
    char *value;
    while ((value = MR_GetNext(key, partition_idx)) != NULL) {
        count += atoi(value);
        free(value);
    }

    // Store result in global array for verification
    strcpy(reduce_results[reduce_result_count].word, key);
    reduce_results[reduce_result_count].count = count;
    reduce_result_count++;
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

// Helper function to verify reduce results
void verify_result(const char *word, int expected_count) {
    for (int i = 0; i < reduce_result_count; i++) {
        if (strcmp(reduce_results[i].word, word) == 0) {
            assert(reduce_results[i].count == expected_count);
            printf("Verified %s: count = %d\n", word, reduce_results[i].count);
            return;
        }
    }
    fprintf(stderr, "Word %s not found in results!\n", word);
    assert(0);  // Fail if word is not found
}

// Test 1: Single File, Single Partition
void test_single_file_single_partition() {
    printf("Test 1: Single File, Single Partition\n");

    create_test_file("test1.txt", "apple apple banana orange apple");

    char *files[] = {"test1.txt"};
    reduce_result_count = 0;  // Reset results

    // Run MapReduce with 1 partition
    MR_Run(1, files, test_mapper, test_reducer, 2, 1);

    // Verify results
    verify_result("apple", 3);
    verify_result("banana", 1);
    verify_result("orange", 1);

    // Cleanup
    remove("test1.txt");

    printf("Test 1 passed: Single file, single partition.\n");
}

// Test 2: Multiple Files, Single Partition
void test_multiple_files_single_partition() {
    printf("Test 2: Multiple Files, Single Partition\n");

    create_test_file("test2a.txt", "apple orange");
    create_test_file("test2b.txt", "orange banana apple");

    char *files[] = {"test2a.txt", "test2b.txt"};
    reduce_result_count = 0;

    // Run MapReduce with 1 partition
    MR_Run(2, files, test_mapper, test_reducer, 3, 1);

    // Verify results
    verify_result("apple", 2);
    verify_result("banana", 1);
    verify_result("orange", 2);

    // Cleanup
    remove("test2a.txt");
    remove("test2b.txt");

    printf("Test 2 passed: Multiple files, single partition.\n");
}

// Test 3: Single File, Multiple Partitions
void test_single_file_multiple_partitions() {
    printf("Test 3: Single File, Multiple Partitions\n");

    create_test_file("test3.txt", "foo bar baz foo foo baz");

    char *files[] = {"test3.txt"};
    reduce_result_count = 0;

    // Run MapReduce with multiple partitions
    MR_Run(1, files, test_mapper, test_reducer, 2, 3);

    // Verify results, allowing for partitioning differences
    verify_result("foo", 3);
    verify_result("bar", 1);
    verify_result("baz", 2);

    // Cleanup
    remove("test3.txt");

    printf("Test 3 passed: Single file, multiple partitions.\n");
}

// Test 4: Multiple Files, Multiple Partitions
void test_multiple_files_multiple_partitions() {
    printf("Test 4: Multiple Files, Multiple Partitions\n");

    create_test_file("test4a.txt", "dog cat");
    create_test_file("test4b.txt", "cat mouse dog");

    char *files[] = {"test4a.txt", "test4b.txt"};
    reduce_result_count = 0;

    // Run MapReduce with multiple partitions
    MR_Run(2, files, test_mapper, test_reducer, 3, 3);

    // Verify results
    verify_result("dog", 2);
    verify_result("cat", 2);
    verify_result("mouse", 1);

    // Cleanup
    remove("test4a.txt");
    remove("test4b.txt");

    printf("Test 4 passed: Multiple files, multiple partitions.\n");
}

// Test 5: Large Input Test
void test_large_input() {
    printf("Test 5: Large Input Test\n");

    create_test_file("test5.txt", "repeat word repeat word repeat word word");

    char *files[] = {"test5.txt"};
    reduce_result_count = 0;

    // Run MapReduce with multiple partitions
    MR_Run(1, files, test_mapper, test_reducer, 5, 3);

    // Verify results
    verify_result("repeat", 3);
    verify_result("word", 4);

    // Cleanup
    remove("test5.txt");

    printf("Test 5 passed: Large input handled.\n");
}

// Main function to run all tests
int main() {
    test_single_file_single_partition();
    test_multiple_files_single_partition();
    test_single_file_multiple_partitions();
    test_multiple_files_multiple_partitions();
    test_large_input();

    printf("All MapReduce tests completed.\n");
    return 0;
}