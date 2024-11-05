#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>
#include <stdbool.h>

typedef void (*thread_func_t)(void *arg);

typedef struct ThreadPool_job_t {
    thread_func_t func;              // function pointer
    void *arg;                       // arguments for that function
    struct ThreadPool_job_t *next;   // pointer to the next job in the queue
    int size;                        // size of the job (for SJF)
} ThreadPool_job_t;

typedef struct {
    unsigned int size;               // number of jobs in the queue
    unsigned int total_jobs;         // total jobs submitted to the queue
    unsigned int completed_jobs;     // number of completed jobs
    ThreadPool_job_t *head;          // pointer to the first (shortest) job
    pthread_mutex_t mutex;           // mutex to protect job queue access
    pthread_cond_t cond;             // condition variable for job queue
    pthread_cond_t all_jobs_done_cond; // condition variable to signal all jobs done
} ThreadPool_job_queue_t;

typedef struct {
    pthread_t *threads;              // pointer to the array of thread handles
    ThreadPool_job_queue_t jobs;     // queue of jobs waiting for a thread to run
    unsigned int num_threads;        // number of threads in the pool
    bool shutdown;                   // flag for shutting down the pool
} ThreadPool_t;

// Function declarations
ThreadPool_t *ThreadPool_create(unsigned int num);
void ThreadPool_destroy(ThreadPool_t *tp);
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg);
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp);
void *Thread_run(ThreadPool_t *tp);
void ThreadPool_check(ThreadPool_t *tp);

#endif
