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
  long long execTime;
  int numThreads, queueSize;
  string user, passwd, address;
  mutex mu;
  Semaphore empty, full;
  deque<Job*> jobQueue;
  vector<thread> workers;
  friend void worker(int id, ConPool* pool);  
public:
  ConPool(int n, int q, string a, string u, string p) {
    assert(n > 0 && q > 0);
    execTime = 0;
    numThreads = n;
    queueSize = q;
    empty.init(queueSize);
    full.init(0);
    address = a;
    user = u;
    passwd = p;
  }

  void enqueue(Job *job, size_t sz) {
    empty.wait();
    mu.lock();
    Job *j = (Job*)malloc(sz);
    assert(j != NULL);
    memcpy(j, job, sz);
    jobQueue.push_back(j);
    mu.unlock();
    full.signal();
  }

  void start() {
    for (int i = 0; i < numThreads; i++) {
      workers.emplace_back(thread(worker, i + 1, this));
    }
    sleep(1);
  }

  void stop() {
    for (thread &t: workers) {
      t.join();
    }    
  }
};

void worker(int id, ConPool *pool) {
  Job *job = NULL;

  try {
    sql::Driver *driver;
    sql::Connection *con;

    driver = get_driver_instance();
    con = driver->connect(pool->address, pool->user, pool->passwd);    

    print("Thread: ", id, " connected to mysql server...\n"); 

    while (true) {
      pool->full.wait();
      pool->mu.lock();

      auto start = chrono::high_resolution_clock::now();
      
      job = pool->jobQueue.front();
      pool->jobQueue.pop_front();
      job->run(con);
      free(job);
      job = NULL;

      auto stop = chrono::high_resolution_clock::now();
      auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
      pool->execTime += duration.count();

      pool->mu.unlock();
      pool->empty.signal();

      // print("Total: ", pool->execTime, "\n");  
    }
  } catch (sql::SQLException &e) {
    free(job);
    print("# ERR: SQLException in ", __FILE__);
    print("(",  __FUNCTION__, ") on line ", __LINE__, "\n");
    print("# ERR: ", e.what());
    print(" (MySQL error code: ", e.getErrorCode());
    print(", SQLState: ", e.getSQLState(), " )", "\n");    
  }
}

#endif