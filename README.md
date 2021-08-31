# conpool

mysql connection pool for cpp using thread pool design pattern.

## Why connection pool?
- mysql-connector-cpp uses tcp to establish connection with mysql server. Every query requires tcp connection establishment and termination, which can slow things down.
- conpool (Connection pool) uses thread pool design pattern. Here we spin <em>n</em> threads and each thread will establish tcp connection with mysql server during start up. conpool maintains a job queue of size <em>q</em> in which we can queue sql queries along with input data. Whenever a thread becomes available, it is assigned with a task from job queue. 
- As we can see, we don't terminate already exisiting tcp connection and we are trying to reuse it as much as possible, this improves concurrency and tasks can be completed faster. 
- More threads doesn't imply higher concurrency. Because the optimal number of threads is a function of number of CPU cores, which is available through <code>thread::hardware_concurrency</code>.


## Setup

```bash
$ chmod +x setup.sh
$ ./setup.sh
```

## Usage

```c++
$ g++ --std=c++17 -I/<path to conpool repo>/mysql-connector-cpp/include/jdbc <your cpp file> -pthread -Wall -lmysqlcppconn
```

## Example

### FCFS

```c++
#include <string>
#include <memory>
#include <cppconn/prepared_statement.h>
#include "conpool.h"
using namespace std;

/* 
  - create a class which inherits abstract Job class, and implement run(sql::Connection*) method
  - describe schema in Input struct
*/
class QueryType1: public Job {
public: 
  /* write the schema of the relation */
  struct Input {
    char name[80];
    int age;
  } input;  
  /* it is mandatory to override, as run(sql::Connection) pure virtual funcion */
  void run(sql::Connection *con) {
    /* mention the database name, else error is thrown as no database is selected by default */
    con->setSchema("test");
    sql::PreparedStatement *prep_stmt = con->prepareStatement("INSERT INTO mytable VALUES (?, ?)");
    prep_stmt->setString(1, input.name);
    prep_stmt->setInt(2, input.age);
    prep_stmt->execute(); 
    delete prep_stmt;
  }
};

...
/*
  Make sure database and relations are already setup on mysql server.
*/


/* Initialize FCFS thread pool */
ConPool pool(
  numThreads, 
  queueSize, 
  "tcp://127.0.0.1:3306", 
  "username", 
  "password", 
  unique_ptr<Scheduler>(new FCFS())
);

/* start the connection pool */
pool.start();

QueryType1 q1;
strcpy(q1.input.name, "Vighnesh Nayak S");
q1.input.age = 21;
pool.enqueue(&q1, sizeof(q1));
strcpy(q1.input.name, "Vighnesh Kamath");
q1.input.age = 22;
pool.enqueue(&q1, sizeof(q1));

/* gracefully stop the connection pool */
pool.stop();
```

### Priority scheduler

```c++
...

/* Initialize priority scheduler thread pool */
ConPool pool(
  numThreads, 
  queueSize, 
  "tcp://127.0.0.1:3306", 
  "username", 
  "password", 
  unique_ptr<Scheduler>(new PriorityScheduler(256)) // levels of priority = 256
);

/* start the connection pool */
pool.start();

QueryType1 q1;
strcpy(q1.input.name, "Vighnesh Nayak S");
q1.input.age = 21;
q1.key = 255;
pool.enqueue(&q1, sizeof(q1));
strcpy(q1.input.name, "Vighnesh Kamath");
q1.input.age = 22;
q1.key = 0;
pool.enqueue(&q1, sizeof(q1));

/* gracefully stop the connection pool */
pool.stop();
```

## TODO
1. Priority scheduler with aging