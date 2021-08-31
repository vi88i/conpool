#ifndef _CONPOOL_H_
#define _CONPOOL_H_

#include <thread>
#include <mutex>
#include <vector>
#include <deque>
#include <tuple>
#include <cstring>
#include <chrono>
#include "utils.h"
#include "semaphore.h"
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
using namespace std;

class ConPool;
void worker(int, ConPool*);

class Job {
public:
  string db;
  virtual void run(sql::Connection*) = 0;
  virtual ~Job() {};
};

class ConPool {
  bool stopRequested, startRequested;
  long long execTime;
  int activeThreads, numThreads, queueSize;
  string user, passwd, address;
  mutex mu_queue, mu_driver, mu_stop;
  Semaphore empty, full;
  deque<Job*> jobQueue;
  vector<thread> workers;
  friend void worker(int id, ConPool* pool);  
public:
  ConPool(int n, int q, string a, string u, string p) {
    assert(n > 0 && q > 0);
    execTime = 0;
    numThreads = n;
    activeThreads = numThreads;
    queueSize = q;
    empty.init(queueSize);
    full.init(0);
    address = a;
    user = u;
    passwd = p;
    stopRequested = false;
    startRequested = false;
  }

  void enqueue(Job *job, size_t sz) {
    mu_stop.lock();
    if (stopRequested) {
      print("Failed to queue task as stop() is requested...\n");
      mu_stop.unlock();
      return;
    }
    mu_stop.unlock();

    empty.wait();
    mu_queue.lock();
    Job *j = (Job*)malloc(sz);
    assert(j != NULL);
    memcpy(j, job, sz);
    jobQueue.push_back(j);
    mu_queue.unlock();
    full.signal();
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

void worker(int id, ConPool *pool) {
  Job *job;
  sql::Driver *driver;
  sql::Connection *con;

  while (true) {
    job = NULL;
    driver = NULL;
    con = NULL;

    try {
      /*
        get_driver_instance() is not thread-safe as mentioned in
        https://dev.mysql.com/doc/dev/connector-cpp/8.0/jdbc_ref.html
      */
      pool->mu_driver.lock();
      driver = get_driver_instance();
      con = driver->connect(pool->address, pool->user, pool->passwd);
      pool->mu_driver.unlock();

      print("Thread: ", id, " connected to mysql server...\n"); 

      while (true) {
        pool->mu_stop.lock();
        /* if stop is requested, and number of thread is greater than job queue size */
        if (pool->stopRequested && pool->activeThreads > (int)pool->jobQueue.size()) {
          print ("Thread ", id, " terminated\n");
          delete con;
          pool->activeThreads--;
          pool->mu_stop.unlock();
          return;
        }
        pool->mu_stop.unlock();

        pool->full.wait();
        pool->mu_queue.lock();

        auto start = chrono::high_resolution_clock::now();
        
        job = pool->jobQueue.front();
        pool->jobQueue.pop_front();
        job->run(con);
        free(job);
        job = NULL;

        auto stop = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
        pool->execTime += duration.count();

        pool->mu_queue.unlock();
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
      print("# ERR: SQLException in ", __FILE__, "\n");
      print("(",  __FUNCTION__, ") on line ", __LINE__, "\n");
      print("# ERR: ", e.what(), "\n");
      print(" (MySQL error code: ", e.getErrorCode(), "\n");
      print(", SQLState: ", e.getSQLState(), " )", "\n");    
    }
  }
}

#endif