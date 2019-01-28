# Parallel Batch Processing: Apache Spark and Apache Flink Benchmark
Benchmark tool to measure the throughput and runtime for Flink and Spark for given workloads. The major goal of this project is to find answer for the following research question:

> Measure throughput and runtime (for Group-By, Sorting, Aggregate) of Flink and Spark clusters on different input sizes and fixed structures (Book-Store) on fixed comparable systems and comparing the results.

The answer can be found [here](#documentation).

## Architecture
![Architecture](docs/Architecture.png)

The bucket which contains the JAR-files is a public AWS-S3-Bucket.

## Benchmark Client
The Benchmark Client first validate the input data and returns an Error if the input data isn't correct.
Afterwards, if the input data was successfully verified data is generated randomly depending on the given size and
will be uploaded into the aws s3 Bucket. Then a benchmark test based on runtime and throughput is running depending of the given input. Finally, the measured runtime and throughput results are stored into a csv file.

### Execution
java -jar benchmarkclient-1.0.jar <aws_username> <aws_secret_key> <cluster_type> <metric_type> <s3_bucket_data_file> <s3_emr_log_dir> [data_size] [number_of_iterations]


### Input Parameters
| Input Field | Type | Required | Description | Examples |
|--- |--- |--- |--- |--- |
| `aws_username` | `String` | yes | Your aws username | `*****@win.tu-berlin.de` |
| `aws_secret_key` | `String` | Yes | Your aws secret key | `**********************262mUimD25jtF` |
| `cluster_type` | `String` | Yes | The cluster type which can be benchmarked. | `flink`, `spark` |
| `metric_type` | `String` | Yes | The metric type which can be benchmarked. | `aggregate`, `groupby`, `sorting`|
| `s3_bucket_data_file` | `String` | Yes | The path to the data.csv file of the aws s3 bucket. | `s3://test-emr-ws1819/data/data.csv`|
| `s3_emr_log_dir` | `String` | Yes | The path of the log directory in your aws s3 bucket. | `s3://test-emr-ws1819/log/`|
| `data_size` | `String` | No | The location of the VirtualBusStop. | `4m`, `1g`, `5g`|
| `number_of_iterations` | `int` | No | The location of the VirtualBusStop. | `1`, `5`, `10`|




## Workloads/Metrics
We defined the following fixed data structure (CSV file):

`id, userId, title, genre, author, pages, publisher, date, price`

This structure is used to perform the following workload jobs on each cluster type (Flink | Spark) for different input sizes:
- Group-By: genre, count per group | [Flink implementation](Metrics/Flink-GroupBy/Readme.md), [Spark implementation](Metrics/Spark-GroupBy/Readme.md)
- Sorting: number of pages (ascending) | [Flink implementation](Metrics/Flink-Sorting/Readme.md), [Spark implementation](Metrics/Spark-Sorting/Readme.md)
- Aggregation: price (max) | [Flink implementation](Metrics/Flink-Aggregation/Readme.md), [Spark implementation](Metrics/Spark-Aggregation/Readme.md)

When the jobs are done, the system will collect the runtime and throughput (=metrics) for each job.

## Documentation

[Report](https://www.overleaf.com/5617113597rhhbfccfpczh)

[Poster](docs/Poster.pptx)
