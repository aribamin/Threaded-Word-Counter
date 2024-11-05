#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "threadpool.h"

// Helper function to create a new job node
// Allocates memory for a job, assigns the function and argument, and returns the job
ThreadPool_job_t *create_job(thread_func_t func, void *arg) {
    ThreadPool_job_t *job = (ThreadPool_job_t *)malloc(sizeof(ThreadPool_job_t));
    if (!job) return NULL;
    job->func = func;
    job->arg = arg;
    job->next = NULL;
    return job;
}

// Initialize a thread pool with a specified number of worker threads
// Allocates and initializes the thread pool structure, job queue, mutexes, and condition variables
ThreadPool_t *ThreadPool_create(unsigned int num_threads) {
    ThreadPool_t *tp = (ThreadPool_t *)malloc(sizeof(ThreadPool_t));
    if (!tp) return NULL;

    // Allocate memory for thread handles
    tp->threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    if (!tp->threads) {
        free(tp);
        return NULL;
    }

    // Initialize job queue properties
    tp->jobs.size = 0;
    tp->jobs.total_jobs = 0;
    tp->jobs.completed_jobs = 0;
    tp->jobs.head = NULL;
    pthread_mutex_init(&tp->jobs.mutex, NULL);
    pthread_cond_init(&tp->jobs.cond, NULL);
    pthread_cond_init(&tp->jobs.all_jobs_done_cond, NULL);

    tp->num_threads = num_threads;
    tp->shutdown = false;

    // Create worker threads to execute jobs
    for (unsigned int i = 0; i < num_threads; i++) {
        pthread_create(&tp->threads[i], NULL, (void *(*)(void *))Thread_run, tp);
    }
    return tp;
}

// Destroy the thread pool and clean up resources
// Signals shutdown, waits for threads to finish, and deallocates all resources
void ThreadPool_destroy(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);
    tp->shutdown = true;
    pthread_cond_broadcast(&tp->jobs.cond);  // Wake up all threads
    pthread_mutex_unlock(&tp->jobs.mutex);

    // Join all threads to ensure they have completed execution
    for (unsigned int i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->threads[i], NULL);
    }
    printf("All threads joined successfully.\n");

    // Free any remaining jobs in the queue
    ThreadPool_job_t *job = tp->jobs.head;
    while (job) {
        ThreadPool_job_t *next = job->next;
        free(job);
        job = next;
    }

    // Free thread pool resources
    free(tp->threads);
    pthread_mutex_destroy(&tp->jobs.mutex);
    pthread_cond_destroy(&tp->jobs.cond);
    pthread_cond_destroy(&tp->jobs.all_jobs_done_cond);
    free(tp);
    printf("Thread pool destroyed\n");
}

// Add a job to the job queue in a Shortest Job First (SJF) manner
// Creates and enqueues a job if the pool is active, or returns false if shutdown
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    pthread_mutex_lock(&tp->jobs.mutex);
    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return false;
    }

    // Create the job node
    ThreadPool_job_t *job = create_job(func, arg);
    if (!job) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return false;
    }

    // Insert the job based on size (Shortest Job First)
    if (tp->jobs.head == NULL || job->size < tp->jobs.head->size) {
        // Insert at the head if it's the first job or if it's the smallest
        job->next = tp->jobs.head;
        tp->jobs.head = job;
    } else {
        // Insert at the correct position based on size
        ThreadPool_job_t *current = tp->jobs.head;
        while (current->next != NULL && current->next->size <= job->size) {
            current = current->next;
        }
        job->next = current->next;
        current->next = job;
    }

    // Update job queue metrics and notify waiting threads
    tp->jobs.size++;
    tp->jobs.total_jobs++;
    pthread_cond_signal(&tp->jobs.cond);
    pthread_mutex_unlock(&tp->jobs.mutex);
    return true;
}

// Retrieve the next job from the job queue
// Blocks until a job is available or until shutdown is signaled
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    // Wait for a job to become available or for the shutdown signal
    while (!tp->shutdown && tp->jobs.size == 0) {
        pthread_cond_wait(&tp->jobs.cond, &tp->jobs.mutex);
    }

    // Retrieve the shortest job (already the head due to sorted insertion)
    ThreadPool_job_t *job = NULL;
    if (tp->jobs.size > 0) {
        job = tp->jobs.head;
        tp->jobs.head = job->next;
        tp->jobs.size--;
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    return job;
}

// Thread routine for workers to fetch and execute jobs
// Continuously fetches jobs until the shutdown signal is given
void *Thread_run(ThreadPool_t *tp) {
    while (1) {
        pthread_mutex_lock(&tp->jobs.mutex);

        // Wait until jobs are available or shutdown is signaled
        while (tp->jobs.size == 0 && !tp->shutdown) {
            pthread_cond_wait(&tp->jobs.cond, &tp->jobs.mutex);
        }

        // If shutdown is set and there are no jobs left, exit the loop
        if (tp->shutdown && tp->jobs.size == 0) {
            pthread_cond_broadcast(&tp->jobs.all_jobs_done_cond);
            pthread_mutex_unlock(&tp->jobs.mutex);
            break;
        }

        // Retrieve the next job from the queue
        ThreadPool_job_t *job = tp->jobs.head;
        if (job) {
            tp->jobs.head = job->next;
            tp->jobs.size--;
        }
        pthread_mutex_unlock(&tp->jobs.mutex);

        // Execute the job if retrieved
        if (job) {
            job->func(job->arg);
            free(job);

            // Update completed job count and signal if all jobs are done
            pthread_mutex_lock(&tp->jobs.mutex);
            tp->jobs.completed_jobs++;
            if (tp->jobs.completed_jobs == tp->jobs.total_jobs && tp->jobs.size == 0) {
                pthread_cond_broadcast(&tp->jobs.all_jobs_done_cond);
            }
            pthread_mutex_unlock(&tp->jobs.mutex);
        }
    }
    return NULL;
}

// Wait for all jobs in the pool to complete before returning
// Blocks until the completed jobs count matches the total jobs count
void ThreadPool_check(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    // Wait until all jobs are marked as completed
    while (tp->jobs.completed_jobs < tp->jobs.total_jobs) {
        pthread_cond_wait(&tp->jobs.all_jobs_done_cond, &tp->jobs.mutex);
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    printf("All jobs completed.\n");
}