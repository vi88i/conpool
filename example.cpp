#include <iostream>
#include <cstring>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include "conpool.h"
#include "utils.h"
#include <cppconn/prepared_statement.h>
using namespace std;

/*
Schema:
CREATE TABLE mytable (
  name VARCHAR(80),
  age in
);
*/

class QueryType1: public Job {
public:
  struct Input {
    char name[80];
    int age;
  } input;  
  void run(sql::Connection *con) {
    sql::PreparedStatement *prep_stmt = con->prepareStatement("INSERT INTO mytable VALUES (?, ?)");
    prep_stmt->setString(1, input.name);
    prep_stmt->setInt(2, input.age);
    prep_stmt->execute(); 
    delete prep_stmt;
  }
};

class QueryType2: public Job {
public:
  struct Input {
    char name[80];
    int age;
  } input;  
  void run(sql::Connection *con) {
    sql::PreparedStatement *prep_stmt = con->prepareStatement("INSERT INTO mytable VALUES (?, ?)");
    prep_stmt->setString(1, input.name);
    prep_stmt->setInt(2, input.age);
    prep_stmt->execute(); 
    delete prep_stmt;
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

  ConPool pool(numThreads, queueSize);
  pool.start();

  for (int i = 1; i <= 10; i++) {
    QueryType1 q1;
    QueryType2 q2;
    strcpy(q1.input.name, "Vighnesh Nayak S");
    q1.input.age = i * 2;
    strcpy(q2.input.name, "Vighnesh Nayak S");
    q2.input.age = i * 3;
    pool.enqueue(&q1, sizeof(q1));
    pool.enqueue(&q2, sizeof(q2));
  }

  pool.stop();

  return 0;
}