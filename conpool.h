#ifndef _CONPOOL_H_
#define _CONPOOL_H_

#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <deque>
#include <tuple>
#include <cstring>
#include <chrono>
#include <memory>
#include <math.h>
#include "utils.h"
#include "semaphore.h"
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
using namespace std;

class Job;
class ConPool;
class Scheduler;
class FCFS;
void worker(int, ConPool*);

class Job {
public:
  int key = 0;
  virtual void run(sql::Connection*) = 0;
  virtual ~Job() {};
};

class Scheduler {
public:
  virtual void setPool(ConPool*) = 0;
  virtual void enqueue(Job*, size_t) = 0;
  virtual Job* next() = 0;
  virtual size_t size() = 0;
};

class ConPool {
  bool stopRequested, startRequested;
  int activeThreads, numThreads, queueSize;
  long long execTime;
  string user, passwd, address;
  mutex mu_queue, mu_driver, mu_stop;
  Semaphore empty, full;
  vector<thread> workers;
  unique_ptr<Scheduler> jobQueue;
  friend void worker(int id, ConPool* pool);
  friend class FCFS;  
  friend class PriorityScheduler;
  friend class AgeingPriorityScheduler;
public:
  ConPool(int n, int q, string a, string u, string p, unique_ptr<Scheduler> j) {
    assert(n > 0 && q > 0);
    execTime = 0;
    numThreads = n;
    activeThreads = numThreads;
    queueSize = q;
    empty.init(queueSize);
    full.init(0);
    /* check if unique_ptr is valid */
    assert(j); 
    j->setPool(this);
    jobQueue = move(j);
    address = a;
    user = u;
    passwd = p;
    stopRequested = false;
    startRequested = false;
  }

  void enqueue(Job *job, size_t sz) {
    jobQueue->enqueue(job, sz);
  }

  Job* next() {
    return jobQueue->next();
  }

  size_t tasksRemaining() {
    return jobQueue->size();
  }

  void start() {
    if (startRequested) {
      print("start() method already executed, call stop() before calling start()\n");
      return;
    }

    startRequested = true;
    
    for (int i = 0; i < numThreads; i++) {
      workers.emplace_back(thread(worker, i + 1, this));
    }
    sleep(1);
  }

  void stop() {
    if (!startRequested) {
      print("Invoke start() before stop()\n");
      return;
    }
    
    mu_stop.lock();
    stopRequested = true;
    mu_stop.unlock();
    
    print("stop() received, waiting for all jobs to be completed...\n");

    for (thread &t: workers) {
      t.join();
    }

    startRequested = false;   
  }
};

class FCFS: public Scheduler {
  ConPool *pool;
  deque<Job*> jobQueue;
public:
  FCFS() {
    pool = NULL;
  }
  void setPool(ConPool *p) {
    pool = p;
  }  
  void enqueue(Job* job, size_t sz) {
    assert(pool != NULL);
    pool->mu_stop.lock();
    if (pool->stopRequested) {
      print("Failed to queue task as stop() is requested...\n");
      pool->mu_stop.unlock();
      return;
    }
    pool->mu_stop.unlock();

    pool->empty.wait();
    pool->mu_queue.lock();
    Job *j = (Job*)malloc(sz);
    assert(j != NULL);
    memcpy(j, job, sz);
    jobQueue.push_back(j);
    pool->mu_queue.unlock();
    pool->full.signal();
  }
  Job* next() {
    if (jobQueue.empty()) {
      return NULL;
    }
    Job *job = jobQueue.front();
    jobQueue.pop_front();
    return job;
  }  
  size_t size() {
    return jobQueue.size();
  }
};

class PriorityScheduler: public Scheduler {
  int level;
  ConPool *pool;
  priority_queue<tuple<int, Job*>, vector<tuple<int, Job*>>, greater<tuple<int, Job*>>> jobQueue;
public:
  PriorityScheduler(int lvl=16) {
    assert(lvl > 0);
    level = lvl;
    pool = NULL;
  }
  void setPool(ConPool *p) {
    pool = p;
  }  
  void enqueue(Job* job, size_t sz) {
    assert(pool != NULL);
    assert(job->key >= 0 && job->key < level);
    pool->mu_stop.lock();
    if (pool->stopRequested) {
      print("Failed to queue task as stop() is requested...\n");
      pool->mu_stop.unlock();
      return;
    }
    pool->mu_stop.unlock();

    pool->empty.wait();
    pool->mu_queue.lock();
    Job *j = (Job*)malloc(sz);
    assert(j != NULL);
    memcpy(j, job, sz);
    jobQueue.push({j->key, j});
    pool->mu_queue.unlock();
    pool->full.signal();
  }
  Job* next() {
    if (jobQueue.empty()) {
      return NULL;
    }
    Job *job = get<1>(jobQueue.top());
    jobQueue.pop();
    return job;
  }  
  size_t size() {
    return jobQueue.size();
  }
};

class AgeingPriorityScheduler: public Scheduler {
  int level; /* levels of priority */
  int ageingFrequency; /* number of tasks that needs to be completed before ageing once */
  long long tasksFinished; /* number of tasks finished */
  ConPool *pool;
  priority_queue<
    tuple<double, long long, Job*>, 
    vector<tuple<double, long long, Job*>>, 
    greater<tuple<double, long long, Job*>>> jobQueue;
public:
  AgeingPriorityScheduler(int lvl=16, int af=1) {
    assert(lvl > 0 && af >= 1);
    level = lvl;
    pool = NULL;
    tasksFinished = 0LL;
    ageingFrequency = af;
  }
  void setPool(ConPool *p) {
    pool = p;
  }  
  void enqueue(Job* job, size_t sz) {
    assert(pool != NULL);
    assert(job->key >= 0 && job->key < level);
    pool->mu_stop.lock();
    if (pool->stopRequested) {
      print("Failed to queue task as stop() is requested...\n");
      pool->mu_stop.unlock();
      return;
    }
    pool->mu_stop.unlock();

    pool->empty.wait();
    pool->mu_queue.lock();
    Job *j = (Job*)malloc(sz);
    assert(j != NULL);
    memcpy(j, job, sz);
    jobQueue.push({j->key, tasksFinished, j});
    pool->mu_queue.unlock();
    pool->full.signal();
  }
  void age() {
    /* 
      new rank = min(level - 1, old rank + log10(tasksFinished - tasksFinished when task was inserted))
    */
    priority_queue<
      tuple<double, long long, Job*>, 
      vector<tuple<double, long long, Job*>>, 
      greater<tuple<double, long long, Job*>>> pq;
    while (!jobQueue.empty()) {
      double key = get<0>(jobQueue.top());
      long long insertTime = get<1>(jobQueue.top());
      Job *job = get<2>(jobQueue.top());
      jobQueue.pop();
      key = min(level - 1.0, key + log10(tasksFinished - insertTime));
      pq.push({key, insertTime, job});
    }
    jobQueue = pq;
  }
  Job* next() {
    if (jobQueue.empty()) {
      return NULL;
    }
    Job *job = get<2>(jobQueue.top());
    jobQueue.pop();
    tasksFinished++;
    /* check if we have to perform ageing */
    if (tasksFinished % ageingFrequency == 0) {
      age();  
    }
    return job;
  }  
  size_t size() {
    return jobQueue.size();
  }
};

void worker(int id, ConPool *pool) {
  bool mu_stop_locked, mu_driver_locked, mu_queue_locked;
  Job *job;
  sql::Driver *driver;
  sql::Connection *con;

  while (true) {
    job = NULL, driver = NULL, con = NULL;
    mu_stop_locked = false, mu_driver_locked = false, mu_queue_locked = false;

    try {
      /*
        get_driver_instance() is not thread-safe as mentioned in
        https://dev.mysql.com/doc/dev/connector-cpp/8.0/jdbc_ref.html
      */
      pool->mu_driver.lock();
      mu_driver_locked = true;
      driver = get_driver_instance();
      con = driver->connect(pool->address, pool->user, pool->passwd);
      mu_driver_locked = false;
      pool->mu_driver.unlock();

      print("Thread: ", id, " connected to mysql server...\n"); 

      while (true) {
        pool->mu_stop.lock();
        mu_stop_locked = true;
        /* if stop is requested, and number of thread is greater than job queue size */
        if (pool->stopRequested && pool->activeThreads > (int)pool->tasksRemaining()) {
          print ("Thread ", id, " terminated\n");
          delete con;
          pool->activeThreads--;
          pool->mu_stop.unlock();
          return;
        }
        mu_stop_locked = false;
        pool->mu_stop.unlock();

        pool->full.wait();
        pool->mu_queue.lock();
        mu_queue_locked = true;

        auto start = chrono::high_resolution_clock::now();
        
        job = pool->next();
        job->run(con);
        free(job);
        job = NULL;

        auto stop = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
        pool->execTime += duration.count();

        pool->mu_queue.unlock();
        mu_queue_locked = false;
        pool->empty.signal();

        // print("Total: ", pool->execTime, "\n");  
      }
    } catch (sql::SQLException &e) {
      if (job != NULL) {
        free(job); 
      }
      /* close the connection in case of any failure... */
      if (con != NULL) {
        print("Thread: ", id, " disconnected from mysql server...\n"); 
        delete con; 
      }
      if (mu_driver_locked) {
        pool->mu_driver.unlock();
      }
      if (mu_stop_locked) {
        pool->mu_stop.unlock();
      }
      if (mu_queue_locked) {
        pool->mu_queue.unlock();
      }
      pool->empty.signal();
      print("# ERR: SQLException in ", __FILE__, "\n");
      print("(",  __FUNCTION__, ") on line ", __LINE__, "\n");
      print("# ERR: ", e.what(), "\n");
      print(" (MySQL error code: ", e.getErrorCode(), "\n");
      print(", SQLState: ", e.getSQLState(), " )", "\n");    
    }
  }
}

#endif