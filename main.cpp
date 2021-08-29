#include <iostream>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include "conpool.h"
using namespace std;

struct fooInput {
  int x;
};

struct fooOutput {
  int x;
};

class foo: public Job {
public:
  void run(void *in, void *out) {
    fooInput *fin = (fooInput*)in;
    fooOutput *fout = (fooOutput*)out;  
    cout << "HI\n";
  }
};

int main(int argc, char *argv[]) {
  int opt;
  extern char *optarg;
  extern int optopt;
  static char usage[] = "args: -n numThreads -q queueSize";

  // get number of CPU cores
  int numThreads = thread::hardware_concurrency();
  int queueSize = 8;

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
      case 'q' : {
        int n = stoi(optarg);
        assert(n > 0);
        queueSize = n;        
        break;
      }
      default: {
        cout << usage << "\n";
        return 0;
      }
    }
  }

  ConPool pool(numThreads, queueSize);
  pool.start();

  for (int i = 0; i < 10; i++) {
    foo f;
    pool.enqueue(&f, NULL, NULL);
  }

  pool.stop();
  return 0;
}