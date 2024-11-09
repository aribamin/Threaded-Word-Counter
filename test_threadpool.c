#include "threadpool.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

// Global variables for testing
static int job_counter = 0;
pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;

// Test job function that increments the counter
void test_job(void *arg) {
    int job_num = *(int *)arg;
    printf("Executing job %d\n", job_num);

    // Simulate work with a small sleep to represent job duration
    usleep(100000); // 100 milliseconds

    // Increment job counter with mutex for thread safety
    pthread_mutex_lock(&counter_mutex);
    job_counter++;
    pthread_mutex_unlock(&counter_mutex);

    printf("Completed job %d\n", job_num); // Additional debug log
}

int main() {
    const int num_threads = 4;
    const int num_jobs = 10;

    // Step 1: Create the thread pool
    ThreadPool_t *pool = ThreadPool_create(num_threads);
    if (pool == NULL) {
        fprintf(stderr, "Failed to create thread pool\n");
        return EXIT_FAILURE;
    }
    printf("Thread pool created with %d threads.\n", num_threads);

    // Step 2: Add jobs to the pool
    int job_ids[num_jobs];
    for (int i = 0; i < num_jobs; i++) {
        job_ids[i] = i;
        int job_size = rand() % 100; // Random size for SJF simulation

        // Adding job to the thread pool with the random size
        if (!ThreadPool_add_job(pool, test_job, &job_ids[i], job_size)) {
            fprintf(stderr, "Failed to add job %d\n", i);
        } else {
            printf("Job %d with size %d added to the pool.\n", i, job_size);
        }
    }

    // Step 3: Wait for all jobs to complete
    printf("Waiting for all jobs to complete...\n");
    ThreadPool_check(pool); // This should ensure all jobs are completed before continuing

    // Step 4: Check if all jobs were completed
    pthread_mutex_lock(&counter_mutex);
    if (job_counter == num_jobs) {
        printf("All jobs completed successfully. Completed %d/%d jobs.\n", job_counter, num_jobs);
    } else {
        printf("Error: Not all jobs completed. Completed %d/%d jobs.\n", job_counter, num_jobs);
    }
    pthread_mutex_unlock(&counter_mutex);

    // Step 5: Clean up the thread pool
    printf("Destroying the thread pool...\n");
    ThreadPool_destroy(pool);
    printf("Thread pool destroyed.\n");

    return EXIT_SUCCESS;
}
