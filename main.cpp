#include <iostream>
#include <vector>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <thread>
#include <mutex>
using namespace std;

mutex job_queue_mutex;

void worker(int tnum) {
  job_queue_mutex.lock();
  cout << "tnum: " << tnum << "\n";
  job_queue_mutex.unlock();
}

int main(int argc, char *argv[]) {
  int opt;
  extern char *optarg;
  extern int optopt;
  static char usage[] = "args: ";

  // get number of CPU cores
  int numThreads = thread::hardware_concurrency();

  // parse command line arguements
  while ((opt = getopt(argc, argv, "n:")) != -1) {
    switch(opt) {
      case 'n' : {
        int n = stoi(optarg);
        assert(n > 0);
        if (n > numThreads) {
          cout << "WARNING: Number of threads > CPU cores\n";
        }
        numThreads = n;
        break;
      }
      default: {
        cout << usage << "\n";
        return 0;
      }
    }
  }

  thread workers[numThreads];

  // spawn numThreads number of threads
  for (int i = 0; i < numThreads; i++) {
    workers[i] = thread(worker, i + 1);
  }

  // join all threads
  for (thread &t: workers) {
    t.join();
  }

  return 0;
}