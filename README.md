# conpool

mysql connection pool for cpp using thread pool design pattern.

## Why connection pool?
- mysql-connector-cpp uses tcp to establish connection with mysql server. Every query requires tcp connection establishment and termination, which can slow things down.
- conpool (Connection pool) uses thread pool design pattern. Here we spin <em>n</em> threads and each thread will establish tcp connection with mysql server during start up. conpool maintains a job queue of size <em>q</em> in which we can queue sql queries along with input data. Whenever a thread becomes available, it is assigned with a task from job queue. 
- As we can see, we don't terminate already exisiting tcp connection and we are trying to reuse it as much as possible. 

## Setup

```bash
$ chmod +x setup.sh
$ ./setup.sh
$ g++ --std=c++17 -I/usr/include/mysql-cppconn-8 -I./mysql-connector-cpp/include/jdbc main.cpp -pthread -Wall && ./a.out -n 4 -q 8
```

## TODO
1. Graceful termination of thread pool.
2. Graceful termination of tcp connections.
3. Handling errors and avoiding deadlocks.
4. Priority scheduler with aging.