#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "threadpool.h"

// Helper function to create a new job node
ThreadPool_job_t *create_job(thread_func_t func, void *arg, int size) {
    ThreadPool_job_t *job = (ThreadPool_job_t *)malloc(sizeof(ThreadPool_job_t));
    if (!job) return NULL;
    job->func = func;
    job->arg = arg;
    job->size = size;  // Set the size for SJF ordering
    job->next = NULL;
    return job;
}

// Initialize a thread pool with a specified number of worker threads
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
    pthread_cond_broadcast(&tp->jobs.cond);
    pthread_mutex_unlock(&tp->jobs.mutex);

    for (unsigned int i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->threads[i], NULL);
    }
    printf("All threads joined successfully.\n");

    ThreadPool_job_t *job = tp->jobs.head;
    while (job) {
        ThreadPool_job_t *next = job->next;
        free(job);
        job = next;
    }

    free(tp->threads);
    pthread_mutex_destroy(&tp->jobs.mutex);
    pthread_cond_destroy(&tp->jobs.cond);
    pthread_cond_destroy(&tp->jobs.all_jobs_done_cond);
    free(tp);
    printf("Thread pool destroyed\n");
}

// Add a job to the job queue in a Shortest Job First (SJF) manner
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg, int size) {
    pthread_mutex_lock(&tp->jobs.mutex);
    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return false;
    }

    ThreadPool_job_t *job = create_job(func, arg, size);
    if (!job) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return false;
    }

    // Insert the job in SJF order (ascending order of size)
    if (tp->jobs.head == NULL || job->size < tp->jobs.head->size) {
        job->next = tp->jobs.head;
        tp->jobs.head = job;
    } else {
        ThreadPool_job_t *current = tp->jobs.head;
        while (current->next != NULL && current->next->size <= job->size) {
            current = current->next;
        }
        job->next = current->next;
        current->next = job;
    }

    tp->jobs.size++;
    tp->jobs.total_jobs++;
    pthread_cond_signal(&tp->jobs.cond);
    pthread_mutex_unlock(&tp->jobs.mutex);
    return true;
}

// Retrieve the next job from the job queue
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

// Thread routine for workers to fetch and execute jobs
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
            job->func(job->arg);
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

// Wait for all jobs in the pool to complete
void ThreadPool_check(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);

    while (tp->jobs.completed_jobs < tp->jobs.total_jobs) {
        pthread_cond_wait(&tp->jobs.all_jobs_done_cond, &tp->jobs.mutex);
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    printf("All jobs completed.\n");
}