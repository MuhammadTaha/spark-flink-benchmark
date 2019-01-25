# Parallel Batch Processing: Apache Spark and Apache Flink Benchmark
Benchmark tool to measure the throughput and runtime for Flink and Spark for given workloads. The major goal of this project is to find answer for the following research question:

> Measure throughput and runtime (for Group-By, Sorting, Aggregate) of Flink and Spark clusters on different input sizes and fixed structures (Book-Store) on fixed comparable systems and comparing the results.

The answer can be found [here](#documentation).

## Architecture
![Architecture](docs/Architecture.png)

The bucket which contains the JAR-files is a public AWS-S3-Bucket.

## Benchmark Client
TODO short description

### Input Parameters
TODO list of all parameters

## Workloads/Metrics
We defined the following fixed data structure (CSV file):

`id, userId, title, genre, author, pages, publisher, date, price`

This structure is used to perform the following workload jobs on each cluster type (Flink | Spark) for different input sizes:
- Group-By: genre, count per group | [Flink implementation](Metrics/Flink-GroupBy/Readme.md), [Spark implementation](Metrics/Spark-GroupBy/Readme.md)
- Sorting: number of pages (ascending) | [Flink implementation](Metrics/Flink-Sorting/Readme.md), [Spark implementation](Metrics/Spark-Sorting/Readme.md)
- Aggregation: price (max) | [Flink implementation](Metrics/Flink-Aggregation/Readme.md), [Spark implementation](Metrics/Spark-Aggregation/Readme.md)

When the jobs are done, the system will collect the runtime and throughput (=metrics) for each job.

## Documentation

[Report](https://github.com/flmu/ec2018-assignment3.git)

[Poster](docs/Poster.pdf)
