# Java Multiclient TCP Caching Server

A thread-safe, in-memory TCP caching server built from scratch in **Java**, featuring LRU eviction, TTL-based expiration, sliding-window rate limiting, concurrent client handling, and performance benchmarking.

The project demonstrates core backend engineering concepts, including socket programming, data structures, concurrency control, thread pools, and request-level rate limiting.

## Features

- **LRU Cache:** HashMap and doubly linked list supporting average O(1) GET, PUT, and eviction operations.
- **TTL Expiration:** Lazy expiration on access and scheduled background cleanup.
- **Thread Safety:** ReentrantLock protects shared cache state and ensures atomic updates.
- **Multiclient TCP Server:** Handles multiple client connections using Java ServerSocket.
- **Bounded Thread Pool:** Uses ThreadPoolExecutor to control concurrency and prevent unbounded task accumulation.
- **Sliding-Window Rate Limiting:** Enforces per-client IP request limits using ConcurrentHashMap and synchronized timestamp deques.
- **Cache Monitoring:** Tracks cache hits, misses, evictions, and expirations.
- **Performance Benchmarking:** Measures request throughput and p50, p95, and p99 latency.

## System Architecture

```text
                TCP Clients
                     |
                     v
              TCP ServerSocket
                     |
                     v
          Bounded ThreadPoolExecutor
                     |
                     v
              ClientHandler
                     |
                     v
          SlidingWindowRateLimiter
                /          \
          Rejected        Allowed
             |               |
        RATE_LIMITED         v
                      ConcurrentLRUCache
                             |
                  +----------+----------+
                  |                     |
                HashMap         Doubly Linked List
                  |                     |
             Key Lookup           LRU Ordering
                  |
             TTL Validation
                  |
                Response
```

All client handlers share the same cache instance. Cache operations are synchronized using ReentrantLock, while rate limiting uses independent per-client synchronization.

## Project Structure

```text
caching/
├── src/
│   ├── ConcurrentLRUCache.java
│   ├── SlidingWindowRateLimiter.java
│   ├── ClientHandler.java
│   ├── CacheServer.java
│   ├── ServerMetrics.java
│   └── BenchmarkClient.java
├── .gitignore
└── README.md
```

## Technologies Used

- Java
- TCP Sockets
- Java Concurrency API
- HashMap and Doubly Linked List
- ReentrantLock
- ConcurrentHashMap
- ThreadPoolExecutor
- ScheduledExecutorService
- Sliding-Window Rate Limiting

## Getting Started

### Prerequisites

- Java JDK installed
- Git
- Terminal or VS Code

### 1. Clone the repository

```bash
git clone https://github.com/Sachin-Rathore-1234/java-tcp-caching-server.git
cd java-tcp-caching-server
```

### 2. Compile the project

```bash
javac src/*.java
```

### 3. Start the TCP server

```bash
java -cp src CacheServer
```

The server listens on `127.0.0.1:6379`.

### 4. Connect a client

Open another terminal:

```bash
nc 127.0.0.1 6379
```

You can now send commands to the server.

## Supported TCP Commands

| Command | Description | Example |
|---|---|---|
| PING | Check server connectivity | `PING` |
| SET | Store a key-value pair with TTL | `SET user1 Sachin 60000` |
| GET | Retrieve a cached value | `GET user1` |
| DELETE | Remove a cached key | `DELETE user1` |
| STATS | Display cache statistics | `STATS` |
| QUIT | Close the client connection | `QUIT` |

TTL is specified in milliseconds. A TTL of `0` means the entry does not expire automatically.

### Example Interaction

```text
PING
PONG

SET user1 Sachin 60000
OK

GET user1
VALUE Sachin

DELETE user1
DELETED

GET user1
NOT_FOUND
```

When the configured request limit is exceeded, the server returns:

```text
RATE_LIMITED
```

## LRU Cache Design

The cache combines a HashMap and a doubly linked list.

- HashMap provides average O(1) access to cached entries.
- The doubly linked list maintains the least-recently-used to most-recently-used order.
- GET moves a valid accessed entry to the most-recently-used position.
- When capacity is reached, the least-recently-used entry is evicted.

### Example

```text
Capacity = 3

SET A
SET B
SET C

LRU -> A B C <- MRU

GET A

LRU -> B C A <- MRU

SET D

LRU -> C A D <- MRU
```

Key B is evicted because it was the least recently used.

## TTL Expiration

Each cache entry can have an expiration deadline.

The server uses two expiration mechanisms:

1. **Lazy expiration:** Expired entries are detected and removed when accessed.
2. **Background cleanup:** A scheduled task periodically scans the cache and removes expired entries.

System.nanoTime() is used to measure elapsed time for TTL calculations.

## Concurrency and Thread Safety

Multiple TCP clients can access the shared cache simultaneously.

A ReentrantLock protects operations that modify both the HashMap and doubly linked list.

This prevents race conditions and maintains consistency between the two data structures.

The server also uses a bounded ThreadPoolExecutor to control the number of concurrent connection-handling tasks.

## Sliding-Window Rate Limiting

The server implements a per-client IP sliding-window rate limiter.

For each client:

1. Maintain a deque of recent accepted request timestamps.
2. Remove timestamps outside the configured rolling window.
3. Check whether the request limit has been reached.
4. Accept the request or return `RATE_LIMITED`.

ConcurrentHashMap manages client windows, while synchronized blocks protect each client's deque.

The exact sliding-window approach enforces a maximum number of accepted requests within a rolling time interval.

## Performance Benchmark

The server was benchmarked locally on a MacBook Air using eight concurrent TCP clients.

### GET Workload Results

| Metric | Measured Result |
|---|---:|
| Throughput | **33,338.83 requests/sec** |
| p50 Latency | 0.214 ms |
| p95 Latency | 0.406 ms |
| p99 Latency | **0.707 ms** |
| Concurrent Clients | 8 |
| Total Requests | 800 |
| Validation Errors | 0 |
| Rate-Limited Requests | 0 |

### Benchmark Configuration

- Eight concurrent TCP clients
- 100 requests per client
- Persistent TCP connections
- Cached-key GET workload
- Localhost networking
- Rate-limit threshold increased for benchmarking

### Run the Benchmark

Start the server, populate the benchmark key, and run:

```bash
java -cp src BenchmarkClient
```

The benchmark reports throughput and request latency percentiles.

**Note:** These measurements represent a short localhost benchmark under the specified configuration. They should not be interpreted as sustained production throughput or distributed-network performance.

## Design Trade-offs

| Design Choice | Benefit | Trade-off |
|---|---|---|
| HashMap + Doubly Linked List | Average O(1) LRU operations | Additional memory for list nodes |
| ReentrantLock | Simple and consistent synchronization | Lock contention under high concurrency |
| Bounded Thread Pool | Controls resource usage | Requests can be rejected under overload |
| Sliding Window | Strict rolling-window request limit | Stores timestamps per client |
| Background TTL Cleanup | Reclaims expired entries | Periodic scanning overhead |
| In-Memory Storage | Low-latency access | Data is lost on server restart |

## Future Improvements

Potential enhancements include:

- Cache sharding to reduce lock contention
- Idle-client rate-limiter cleanup
- Redis-backed distributed rate limiting
- Structured logging and monitoring dashboards
- Docker deployment
- Additional automated concurrency and load tests

## Author

**Sachin Rathore**

GitHub: [Sachin-Rathore-1234](https://github.com/Sachin-Rathore-1234)

Built as a hands-on backend engineering project to explore caching systems, Java concurrency, TCP networking, and performance optimization.
