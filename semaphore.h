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

  /*
    unique_lock is a RAII (locks the mutex by default)
    test the condition:
      if condition is true: continue in critical section
      else release the lock, and wait for signal on condition variable.
      When thread is signalled, lock mutex and check condition again, loop.

    Spinlock: (continously lock, check if queue is empty and unlock => More CPU cycles)
    Blocking: (lock, wait for signal on condition variable, unlock => less CPU cycles)  
  */
  void wait() {
    unique_lock<mutex> locker(mu);
    cond.wait(locker, [this] { return N > 0; });
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