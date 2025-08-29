# - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Name : Arib Amin
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

This program counts words in a set of text files using a custom MapReduce approach. MapReduce is a method where we break down tasks (like counting words) into smaller parts so they can be handled in parallel, making the whole process faster. Here, we also use a thread pool to manage multiple threads that do the counting work.

MapReduce Library (mapreduce.c): Handles breaking up the word counting tasks (mapping) and combining the results (reducing).

Thread Pool Library (threadpool.c): Manages a group of threads that process the tasks, so we can count words in multiple files at the same time.

Main Program (distwc.c): Runs the word-counting process, taking in files from the sample_inputs directory.

Synchronization
Since multiple threads work on shared data, we need to keep them from interfering with each other. To do this, we use special tools:

Mutex Locks (pthread_mutex_t):
Used in the Thread Pool to prevent threads from clashing over the job queue. Each thread waits its turn to access the queue, keeping it orderly.
Also used in Partitions to make sure each thread safely adds or reads word counts in a shared space.

Condition Variables (pthread_cond_t):
In the Thread Pool, these help the threads wait until there’s work to do, or until all the work is finished.
Together, these tools keep the program safe from data errors and crashes by making sure threads don’t step on each other’s work.

Data Structures
1. Thread Pool Queue
Job Queue: A linked list of jobs (or tasks) for each thread to complete. Each job is just a function and its inputs.
Head Pointer: Points to the next job in line.
ThreadPool_job_queue_t: Keeps track of how many jobs there are, how many were completed, and if all jobs are done.
The size parameter needs to be added so that each job can have a "priority" value, allowing the thread pool to identify which jobs are "shorter" or "quicker." By knowing the size of each job, we can organize them in the queue to ensure that the shortest job is always picked first, implementing the Shortest Job First (SJF) scheduling. Without the size parameter, the pool wouldn't know which job is shorter, so it couldn’t prioritize jobs effectively.

2. MapReduce Partitions
Partition Array: Divides the data into sections (partitions) to spread the work across reducers.
Key-Value Pairs: Each partition has a list of words (keys) with their counts (values).
Each partition has a lock to keep multiple threads from changing its data at the same time.

Testing the Program
The program was tested under different conditions to make sure it works smoothly and gives accurate results.

Different Number of Threads:
Ran tests with different numbers of threads to make sure the thread pool manages tasks correctly and doesn’t lose track of any jobs.

Different Partition Counts:
Tested with different numbers of partitions to see how it affects the distribution of word counts. This ensures the MapReduce can split data across many sections.

Various Input Files:
We used a mix of big files, small files, and multiple files to check that the program counts words correctly no matter the input size or type.

Thread Safety Checks:
Stress-tested by running many threads at once to catch any data races or errors. This confirmed that all threads finished their work correctly.

How to Use

Build the Program:
make

Run the Program:
Make sure all files you want to count words in are in the sample_inputs folder.

Run the program with:
make run or ./wordcount sample_inputs/*
This will automatically read all files in sample_inputs without needing to type each file name.

Clean Up:
To remove any generated files, use:
make clean

Summary
This project uses a thread pool and a MapReduce method to count words efficiently in multiple files. The program can handle many files at once, and it’s designed to be flexible so it can work with different numbers of threads and partitions. It uses thread synchronization to make sure everything runs safely and accurately.
