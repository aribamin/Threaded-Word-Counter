#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"
#include "threadpool.h"

// Key-value pair structure
typedef struct {
    char *key;
    char **values;
    unsigned int value_count;
    unsigned int value_capacity;
} KeyValuePair;

// Partition structure containing multiple key-value pairs
typedef struct {
    KeyValuePair *pairs;
    unsigned int pair_count;
    unsigned int capacity;
    pthread_mutex_t lock;
} Partition;

// Global variables for partitions, user-defined map/reduce functions, and thread pool
static Partition *partitions;
static unsigned int num_partitions;
static Mapper user_mapper;
static Reducer user_reducer;
static ThreadPool_t *thread_pool;

// Insert a key-value pair into the specified partition
void insert_into_partition(unsigned int partition_idx, char *key, char *value) {
    Partition *partition = &partitions[partition_idx];
    pthread_mutex_lock(&partition->lock);

    // Check if the key already exists, append value if it does
    for (unsigned int i = 0; i < partition->pair_count; i++) {
        if (strcmp(partition->pairs[i].key, key) == 0) {
            if (partition->pairs[i].value_count == partition->pairs[i].value_capacity) {
                partition->pairs[i].value_capacity *= 2;
                partition->pairs[i].values = realloc(partition->pairs[i].values, partition->pairs[i].value_capacity * sizeof(char *));
            }
            partition->pairs[i].values[partition->pairs[i].value_count++] = strdup(value);
            pthread_mutex_unlock(&partition->lock);
            return;
        }
    }

    // Key does not exist, create new key-value pair entry
    if (partition->pair_count == partition->capacity) {
        partition->capacity *= 2;
        partition->pairs = realloc(partition->pairs, partition->capacity * sizeof(KeyValuePair));
    }

    partition->pairs[partition->pair_count].key = strdup(key);
    partition->pairs[partition->pair_count].values = malloc(10 * sizeof(char *));
    partition->pairs[partition->pair_count].values[0] = strdup(value);
    partition->pairs[partition->pair_count].value_count = 1;
    partition->pairs[partition->pair_count].value_capacity = 10;
    partition->pair_count++;

    pthread_mutex_unlock(&partition->lock);
}

// Hash function to determine partition index
unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

// Emit function to add a key-value pair to a partition
void MR_Emit(char *key, char *value) {
    if (key == NULL || strlen(key) == 0) {
        printf("[MR_Emit] Skipping empty key.\n");
        return;
    }
    unsigned int partition_idx = MR_Partitioner(key, num_partitions);
    printf("[MR_Emit] Key: %s, Value: %s, Partition: %u\n", key, value, partition_idx);
    insert_into_partition(partition_idx, key, value);
}

// Get the next value for a given key in a specified partition
char *MR_GetNext(char *key, unsigned int partition_idx) {
    Partition *partition = &partitions[partition_idx];
    pthread_mutex_lock(&partition->lock);

    for (unsigned int i = 0; i < partition->pair_count; i++) {
        if (strcmp(partition->pairs[i].key, key) == 0) {
            if (partition->pairs[i].value_count > 0) {
                char *value = partition->pairs[i].values[--partition->pairs[i].value_count];
                pthread_mutex_unlock(&partition->lock);
                return value;
            }
        }
    }

    pthread_mutex_unlock(&partition->lock);
    return NULL;
}

// Reducer task for each partition
void reduce_task(void *arg) {
    unsigned int partition_idx = *(unsigned int *)arg;
    free(arg);
    Partition *partition = &partitions[partition_idx];

    for (unsigned int i = 0; i < partition->pair_count; i++) {
        user_reducer(partition->pairs[i].key, partition_idx);
        free(partition->pairs[i].key);
        for (unsigned int j = 0; j < partition->pairs[i].value_count; j++) {
            free(partition->pairs[i].values[j]);
        }
        free(partition->pairs[i].values);
    }
}

// Run the entire MapReduce workflow
void MR_Run(unsigned int file_count, char *file_names[], Mapper mapper, Reducer reducer, unsigned int num_workers, unsigned int num_parts) {
    // Initialize global variables and partitions
    num_partitions = num_parts;
    partitions = malloc(num_parts * sizeof(Partition));

    for (unsigned int i = 0; i < num_parts; i++) {
        partitions[i].pairs = malloc(10 * sizeof(KeyValuePair));
        partitions[i].pair_count = 0;
        partitions[i].capacity = 10;
        pthread_mutex_init(&partitions[i].lock, NULL);
    }

    // Set user-defined functions and create a thread pool
    user_mapper = mapper;
    user_reducer = reducer;
    thread_pool = ThreadPool_create(num_workers);

    printf("Starting map phase...\n");

    // Map phase: Submit each file as a map job
    for (unsigned int i = 0; i < file_count; i++) {
        ThreadPool_add_job(thread_pool, (thread_func_t)mapper, file_names[i]);
    }
    ThreadPool_check(thread_pool);
    printf("Map phase completed.\n");

    printf("Starting reduce phase...\n");

    // Reduce phase: Create reduce tasks for each partition
    for (unsigned int i = 0; i < num_parts; i++) {
        unsigned int *partition_idx = malloc(sizeof(unsigned int));
        *partition_idx = i;
        ThreadPool_add_job(thread_pool, reduce_task, partition_idx);
    }
    ThreadPool_check(thread_pool);
    printf("Reduce phase completed.\n");

    // Cleanup resources
    ThreadPool_destroy(thread_pool);
    for (unsigned int i = 0; i < num_parts; i++) {
        pthread_mutex_destroy(&partitions[i].lock);
        free(partitions[i].pairs);
    }
    free(partitions);

    printf("MapReduce run completed.\n");
}
