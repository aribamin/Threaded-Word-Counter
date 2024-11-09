#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"
#include "threadpool.h"

// Defines a structure for a key-value pair in each partition
typedef struct {
    char *key;
    char **values;
    unsigned int value_count;
    unsigned int value_capacity;
} KeyValuePair;

// Defines a structure for a partition, which holds multiple key-value pairs
typedef struct {
    KeyValuePair *pairs;
    unsigned int pair_count;
    unsigned int capacity;
    pthread_mutex_t lock;
} Partition;

// Global variables for the MapReduce framework
static Partition *partitions;
static unsigned int num_partitions;
static Mapper user_mapper;
static Reducer user_reducer;
static ThreadPool_t *thread_pool;

// Inserts a key-value pair into a specified partition
void insert_into_partition(unsigned int partition_idx, char *key, char *value) {
    Partition *partition = &partitions[partition_idx];
    pthread_mutex_lock(&partition->lock);

    // Check if the key already exists in the partition
    for (unsigned int i = 0; i < partition->pair_count; i++) {
        if (strcmp(partition->pairs[i].key, key) == 0) {
            // If key exists and value array is full, increase capacity
            if (partition->pairs[i].value_count == partition->pairs[i].value_capacity) {
                partition->pairs[i].value_capacity *= 2;
                partition->pairs[i].values = realloc(partition->pairs[i].values, partition->pairs[i].value_capacity * sizeof(char *));
            }
            // Add the new value to the key's value list
            partition->pairs[i].values[partition->pairs[i].value_count++] = strdup(value);
            pthread_mutex_unlock(&partition->lock);
            return;
        }
    }

    // Key does not exist; create a new key-value pair
    if (partition->pair_count == partition->capacity) {
        // If the partition is full, increase capacity
        partition->capacity *= 2;
        partition->pairs = realloc(partition->pairs, partition->capacity * sizeof(KeyValuePair));
    }

    // Initialize the new key-value pair
    partition->pairs[partition->pair_count].key = strdup(key);
    partition->pairs[partition->pair_count].values = malloc(10 * sizeof(char *));
    partition->pairs[partition->pair_count].values[0] = strdup(value);
    partition->pairs[partition->pair_count].value_count = 1;
    partition->pairs[partition->pair_count].value_capacity = 10;
    partition->pair_count++;

    pthread_mutex_unlock(&partition->lock);
}

// Hash function to determine which partition a key should be placed in
unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

// Emit function called by the Mapper to add a key-value pair to a partition
void MR_Emit(char *key, char *value) {
    if (key == NULL || strlen(key) == 0) {
        // printf("[MR_Emit] Skipping empty key.\n");
        return;
    }
    // Determine the partition index for the key
    unsigned int partition_idx = MR_Partitioner(key, num_partitions);
    // printf("[MR_Emit] Key: %s, Value: %s, Partition: %u\n", key, value, partition_idx);
    insert_into_partition(partition_idx, key, value);
}

// Retrieves the next value associated with a key from a partition
char *MR_GetNext(char *key, unsigned int partition_idx) {
    if (partition_idx >= num_partitions || !key) {
        // fprintf(stderr, "[MR_GetNext] Invalid partition index or null key.\n");
        return NULL;
    }

    Partition *partition = &partitions[partition_idx];
    pthread_mutex_lock(&partition->lock);

    char *value = NULL;
    for (unsigned int i = 0; i < partition->pair_count; i++) {
        if (partition->pairs[i].key && strcmp(partition->pairs[i].key, key) == 0) {
            if (partition->pairs[i].value_count > 0) {
                value = strdup(partition->pairs[i].values[--partition->pairs[i].value_count]);
            }
            break;
        }
    }

    pthread_mutex_unlock(&partition->lock);
    return value;
}

// Comparison function for sorting key-value pairs lexicographically
int compare_key_value_pairs(const void *a, const void *b) {
    KeyValuePair *pairA = (KeyValuePair *)a;
    KeyValuePair *pairB = (KeyValuePair *)b;
    return strcmp(pairA->key, pairB->key);
}

// Reducer task for each partition
void reduce_task(void *arg) {
    unsigned int partition_idx = *(unsigned int *)arg;
    free(arg);
    Partition *partition = &partitions[partition_idx];

    // Sort key-value pairs in lexicographic order
    qsort(partition->pairs, partition->pair_count, sizeof(KeyValuePair), compare_key_value_pairs);

    // For each key in the partition, call the user-defined reducer
    for (unsigned int i = 0; i < partition->pair_count; i++) {
        user_reducer(partition->pairs[i].key, partition_idx);
        
        // Free memory for the key and all associated values
        free(partition->pairs[i].key);
        for (unsigned int j = 0; j < partition->pairs[i].value_count; j++) {
            free(partition->pairs[i].values[j]);
        }
        free(partition->pairs[i].values);
    }
}

// Executes the MapReduce process, handling map and reduce phases
void MR_Run(unsigned int file_count, char *file_names[], Mapper mapper, Reducer reducer, unsigned int num_workers, unsigned int num_parts) {
    // Initialize partitions and mutexes
    num_partitions = num_parts;
    partitions = malloc(num_parts * sizeof(Partition));
    for (unsigned int i = 0; i < num_parts; i++) {
        partitions[i].pairs = malloc(10 * sizeof(KeyValuePair));
        partitions[i].pair_count = 0;
        partitions[i].capacity = 10;
        pthread_mutex_init(&partitions[i].lock, NULL);
    }

    // Set user-defined functions and create a thread pool for worker threads
    user_mapper = mapper;
    user_reducer = reducer;
    thread_pool = ThreadPool_create(num_workers);

    printf("Starting map phase...\n");

    // Map phase: Submit each file to be processed by the mapper
    for (unsigned int i = 0; i < file_count; i++) {
        int job_size = 10;
        ThreadPool_add_job(thread_pool, (thread_func_t)mapper, file_names[i], job_size);
    }
    ThreadPool_check(thread_pool);
    printf("Map phase completed.\n");

    printf("Starting reduce phase...\n");

    // Reduce phase: Submit a reduce task for each partition
    for (unsigned int i = 0; i < num_parts; i++) {
        unsigned int *partition_idx = malloc(sizeof(unsigned int));
        *partition_idx = i;
        int job_size = 20;
        ThreadPool_add_job(thread_pool, reduce_task, partition_idx, job_size);
    }
    ThreadPool_check(thread_pool);
    printf("Reduce phase completed.\n");

    // Clean up resources
    ThreadPool_destroy(thread_pool);
    for (unsigned int i = 0; i < num_parts; i++) {
        pthread_mutex_destroy(&partitions[i].lock);
        free(partitions[i].pairs);
    }
    free(partitions);

    printf("MapReduce run completed.\n");
}