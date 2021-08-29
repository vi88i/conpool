#ifndef _SEMAPHORE_H_
#define _SEMAPHORE_H_

#include <assert.h>
#include <thread>
#include <mutex>
#include <condition_variable>
using namespace std;

class Semaphore {
  int N;
  mutex mu;
  condition_variable cond;
public:
  Semaphore() {
    N = 1;
  }

  void init(int n) {
    assert(n >= 0);
    N = n;
  }

  void wait() {
    unique_lock<mutex> locker(mu);
    while (N == 0) {
      cond.wait(locker);  
    }
    N = N - 1;
    locker.unlock();
  }

  void signal() {
    mu.lock();
    N = N + 1;
    mu.unlock();
    cond.notify_one();
  }
};

#endif