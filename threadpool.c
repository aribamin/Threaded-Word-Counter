#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "threadpool.h"

// Helper function to create a job node
ThreadPool_job_t *create_job(thread_func_t func, void *arg) {
    ThreadPool_job_t *job = (ThreadPool_job_t *)malloc(sizeof(ThreadPool_job_t));
    if (!job) return NULL;
    job->func = func;
    job->arg = arg;
    job->next = NULL;
    return job;
}

// Initialize the thread pool
ThreadPool_t *ThreadPool_create(unsigned int num_threads) {
    ThreadPool_t *tp = (ThreadPool_t *)malloc(sizeof(ThreadPool_t));
    if (!tp) return NULL;

    tp->threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    if (!tp->threads) {
        free(tp);
        return NULL;
    }

    tp->jobs.size = 0;
    tp->jobs.total_jobs = 0;
    tp->jobs.completed_jobs = 0;
    tp->jobs.head = NULL;
    pthread_mutex_init(&tp->jobs.mutex, NULL);
    pthread_cond_init(&tp->jobs.cond, NULL);
    pthread_cond_init(&tp->jobs.all_jobs_done_cond, NULL);

    tp->num_threads = num_threads;
    tp->shutdown = false;

    for (unsigned int i = 0; i < num_threads; i++) {
        pthread_create(&tp->threads[i], NULL, (void *(*)(void *))Thread_run, tp);
    }
    return tp;
}

// Destroy the thread pool and clean up resources
void ThreadPool_destroy(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);
    tp->shutdown = true;
    pthread_cond_broadcast(&tp->jobs.cond);  // Wake up all waiting threads
    pthread_mutex_unlock(&tp->jobs.mutex);

    // Join all threads to ensure they complete their work
    for (unsigned int i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->threads[i], NULL);
    }
    printf("All threads joined successfully.\n");

    // Free remaining jobs in the queue
    ThreadPool_job_t *job = tp->jobs.head;
    while (job) {
        ThreadPool_job_t *next = job->next;
        free(job);
        job = next;
    }

    // Clean up other resources
    free(tp->threads);
    pthread_mutex_destroy(&tp->jobs.mutex);
    pthread_cond_destroy(&tp->jobs.cond);
    pthread_cond_destroy(&tp->jobs.all_jobs_done_cond);
    free(tp);
    printf("Thread pool destroyed\n");
}

// Add a job to the end of the job queue (FCFS policy)
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    pthread_mutex_lock(&tp->jobs.mutex);
    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return false;
    }

    // Create the job
    ThreadPool_job_t *job = create_job(func, arg);
    if (!job) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return false;
    }

    // Insert job at the end of the queue
    if (tp->jobs.head == NULL) {
        tp->jobs.head = job;
    } else {
        ThreadPool_job_t *current = tp->jobs.head;
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = job;
    }

    tp->jobs.size++;
    tp->jobs.total_jobs++;
    pthread_cond_signal(&tp->jobs.cond);
    pthread_mutex_unlock(&tp->jobs.mutex);
    return true;
}

// Retrieve the next job from the queue
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    while (!tp->shutdown && tp->jobs.size == 0) {
        pthread_cond_wait(&tp->jobs.cond, &tp->jobs.mutex);
    }

    ThreadPool_job_t *job = NULL;
    if (tp->jobs.size > 0) {
        job = tp->jobs.head;
        tp->jobs.head = job->next;
        tp->jobs.size--;
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    return job;
}

// Thread routine that fetches and runs jobs
void *Thread_run(ThreadPool_t *tp) {
    while (1) {
        pthread_mutex_lock(&tp->jobs.mutex);

        while (tp->jobs.size == 0 && !tp->shutdown) {
            pthread_cond_wait(&tp->jobs.cond, &tp->jobs.mutex);
        }

        if (tp->shutdown && tp->jobs.size == 0) {
            pthread_cond_broadcast(&tp->jobs.all_jobs_done_cond);
            pthread_mutex_unlock(&tp->jobs.mutex);
            break;
        }

        ThreadPool_job_t *job = tp->jobs.head;
        if (job) {
            tp->jobs.head = job->next;
            tp->jobs.size--;
        }
        pthread_mutex_unlock(&tp->jobs.mutex);

        if (job) {
            job->func(job->arg);  // Run the job function
            free(job);

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

// Ensure all threads are idle and the job queue is empty before returning
void ThreadPool_check(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    while (tp->jobs.completed_jobs < tp->jobs.total_jobs) {
        pthread_cond_wait(&tp->jobs.all_jobs_done_cond, &tp->jobs.mutex);
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    printf("All jobs completed.\n");
}
