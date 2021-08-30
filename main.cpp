#include <iostream>
#include <cstring>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include "conpool.h"
#include "utils.h"
using namespace std;

class QueryType1: public Job {
public:
  struct Input {
    int x;
  } input;  
  void run(sql::Connection *con) {
    print("a: ", input.x, "\n");
  }
};

class QueryType2: public Job {
public:
  struct Input {
    int x;
  } input;  
  void run(sql::Connection *con) {
    print("b: ", input.x, "\n");
  }
};

int main(int argc, char *argv[]) {
  int opt;
  extern char *optarg;
  static char usage[] = "args: -n numThreads -q queueSize";

  // get number of CPU cores
  int numThreads = thread::hardware_concurrency();
  int queueSize = 1024;

  // parse command line arguements
  while ((opt = getopt(argc, argv, "n:q:")) != -1) {
    switch(opt) {
      case 'n' : {
        int n = stoi(optarg);
        assert(n > 0);
        if (n > numThreads) {
          print("WARNING: Number of threads > CPU cores\n");
        }
        numThreads = n;
        break;
      }
      case 'q' : {
        int n = stoi(optarg);
        assert(n > 0);
        queueSize = n;        
        break;
      }
      default: {
        print(usage, "\n");
        return 0;
      }
    }
  }

  print(numThreads, " ", queueSize, "\n");
  ConPool pool(numThreads, queueSize);
  pool.start();

  for (int i = 1; i <= 10; i++) {
    QueryType1 q1;
    QueryType2 q2;
    q1.input.x = i * 2;
    q2.input.x = i * 3;
    pool.enqueue(&q1, sizeof(q1));
    pool.enqueue(&q2, sizeof(q2));
  }

  pool.stop();

  return 0;
}