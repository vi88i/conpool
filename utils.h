#ifndef _UTILS_H_
#define _UTILS_H_

#include <iostream>
#include <mutex>
using namespace std;

mutex stdout_mutex;

template <class T>
void print(T t) {
  stdout_mutex.lock();
  std::cout << t;
  stdout_mutex.unlock();
}

template <class T, class... Args>
void print(T t, Args... args) {
    stdout_mutex.lock();
    std::cout << t;
    stdout_mutex.unlock();
    print(args...);
}

#endif