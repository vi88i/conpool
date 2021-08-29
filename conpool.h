#ifndef _CONPOOL_H_
#define _CONPOOL_H_

#include <thread>
#include <mutex>
#include <vector>
#include <deque>
#include <tuple>
#include "semaphore.h"
#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
using namespace std;

class ConPool;
void worker(int, ConPool*);

class Job {
public:
  virtual void run(void*, void*) = 0;
};

class ConPool {
  int numThreads, queueSize;
  mutex mu;
  Semaphore empty, full;
  deque<tuple<Job*, void*, void*>> jobQueue;
  vector<thread> workers;
  friend void worker(int id, ConPool* pool);  
public:
  ConPool(int n, int q) {
    numThreads = n;
    queueSize = q;
    empty.init(queueSize);
    full.init(0);
  }

  void enqueue(Job *job, void *in, void *out) {
    empty.wait();
    mu.lock();
    jobQueue.push_back({job, in, out});
    cout << "added\n";
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

void worker(int id, ConPool *pool) {
  try {
    sql::Driver *driver;
    sql::Connection *con;
    sql::Statement *stmt;
    sql::ResultSet *res;
    sql::PreparedStatement *pstmt;

    driver = get_driver_instance();
    con = driver->connect("tcp://127.0.0.1:3306", "root", "root");
    con->setSchema("test");    

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
  } catch (sql::SQLException &e) {
    cout << "# ERR: SQLException in " << __FILE__;
    cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
    cout << "# ERR: " << e.what();
    cout << " (MySQL error code: " << e.getErrorCode();
    cout << ", SQLState: " << e.getSQLState() << " )" << endl;    
  }
}

#endif