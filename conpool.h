#ifndef _CONPOOL_H_
#define _CONPOOL_H_

#include <thread>
#include <mutex>
#include <vector>
#include <deque>
#include <tuple>
#include <cstring>
#include "utils.h"
#include "semaphore.h"
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
using namespace std;

class ConPool;
void worker(int, ConPool*);

class Job {
public:
  virtual void run(sql::Connection*) = 0;
  virtual ~Job() {};
};

class ConPool {
  int numThreads, queueSize;
  mutex mu;
  Semaphore empty, full;
  deque<Job*> jobQueue;
  vector<thread> workers;
  friend void worker(int id, ConPool* pool);  
public:
  ConPool(int n, int q) {
    numThreads = n;
    queueSize = q;
    empty.init(queueSize);
    full.init(0);
  }

  void enqueue(Job *job, size_t sz) {
    empty.wait();
    mu.lock();
    Job *j = (Job*)malloc(sz);
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
  try {
    sql::Driver *driver;
    sql::Connection *con;

    driver = get_driver_instance();
    con = driver->connect("tcp://127.0.0.1:3306", "root", "viggi2000");
    con->setSchema("test");    

    print("Thread: ", id, " connected to mysql server...\n"); 

    while (true) {
      pool->full.wait();
      pool->mu.lock();

      Job *job = pool->jobQueue.front();
      pool->jobQueue.pop_front();
      job->run(con);
      free(job);
      
      pool->mu.unlock();
      pool->empty.signal();
    }
  } catch (sql::SQLException &e) {
    print("# ERR: SQLException in ", __FILE__);
    print("(",  __FUNCTION__, ") on line ", __LINE__, "\n");
    print("# ERR: ", e.what());
    print(" (MySQL error code: ", e.getErrorCode());
    print(", SQLState: ", e.getSQLState(), " )", "\n");    
  }
}

#endif