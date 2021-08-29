#ifndef _THREADPOOL_H_
#define _THREADPOOL_H_

#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <deque>
#include <tuple>
#include "semaphore.h"
using namespace std;

class ThreadPool;
void worker(int, ThreadPool*);

class Job {
public:
  virtual void run(void*, void*) = 0;
};

class ThreadPool {
  int numThreads, queueSize;
  mutex mu;
  Semaphore empty, full;
  deque<tuple<Job*, void*, void*>> jobQueue;
  vector<thread> workers;
  friend void worker(int id, ThreadPool* pool);  
public:
  ThreadPool(int n, int q) {
    numThreads = n;
    queueSize = q;
    empty.init(queueSize);
    full.init(0);
  }

  void enqueue(Job *job, void *in, void *out) {
    empty.wait();
    mu.lock();
    jobQueue.push_back({job, in, out});
    mu.unlock();
    full.signal();
  }

  void start() {
    for (int i = 0; i < numThreads; i++) {
      workers.emplace_back(thread(worker, i + 1, this));
    }
  }

  void stop() {
    for (thread &t: workers) {
      t.join();
    }    
  }
};

void worker(int id, ThreadPool *pool) {
  while (true) {
    pool->full.wait();
    pool->mu.lock();

    Job *job;
    void *in = NULL, *out = NULL;
    
    tie(job, in, out) = pool->jobQueue.front();
    pool->jobQueue.pop_front();
    job->run(in, out);
    
    pool->mu.unlock();
    pool->empty.signal();
  }
}

#endif