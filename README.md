## MiniMapReduce

A lightweight, Java-based MapReduce framework designed to demonstrate the core concepts of distributed data processing.
MiniMapReduce lets you run simple MapReduce jobs—such as word counts or data aggregations—on a single JVM using
parallelism via the ForkJoinPool, making it an ideal learning tool and foundation for experimenting with MapReduce
pipelines.

## Table of Contents

* [Key Features](#key-features)
* [Prerequisites](#prerequisites)
* [How It Works](#how-it-works)
* [License](#license)

## Key Features

* **Core MapReduce API**
  Provides `Mapper`, `Reducer`, and `Job` abstractions that mirror the classic MapReduce paradigm.
* **Parallel Processing**
  Utilizes Java’s `ForkJoinPool` to process map tasks concurrently for high throughput on multi-core machines.
* **Pluggable Tasks**
  Easily swap in your own mapper and reducer implementations to tackle custom data-processing jobs.
* **Lightweight & Portable**
  No external dependencies beyond JDK 24 and JUnit 4 — runs anywhere Java runs.

## Prerequisites

* **Java Development Kit (JDK) 24**
* **JUnit 4** (for unit and integration tests)

## How It Works

1. **Map Phase**

    * Input splits are read line-by-line.
    * Each line is submitted as a `MapTask` to the `ForkJoinPool`.
    * The user-provided `Mapper` processes input and emits intermediate key–value pairs.

2. **Shuffle & Sort**

    * Intermediate pairs from all map tasks are grouped by key.
    * Data is partitioned evenly among the specified number of reducers.

3. **Reduce Phase**

    * Each partition is handled by a `ReduceTask` in parallel.
    * The user-provided `Reducer` combines all values for a key into final results.

4. **Output**

    * Final key–value pairs are written to output files, one per reducer.

```mermaid
graph TB
    A[Input Files] --> B[Map Phase]
    B --> C1[Map Task 1]
    B --> C2[Map Task 2]
    B --> C3[Map Task 3]
    C1 --> D[Intermediate KV]
    C2 --> D
    C3 --> D
    D --> E[Shuffle & Sort]
    E --> F[Reduce Phase]
    F --> G1[Reduce Task 1]
    F --> G2[Reduce Task 2]
    G1 --> H[Output Files]
    G2 --> H
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
