
# Spark Streaming with Kafka Integration

This project demonstrates a simple Spark Streaming application that consumes data from a Kafka topic, performs transformations and aggregations, and writes the results to the console. 

## Project Structure

- `spark-event-producer.py`: A Kafka producer that generates synthetic data using Faker and sends it to a Kafka topic.
- `spark-event-consumer.py`: A Spark Streaming application that consumes data from the Kafka topic, performs aggregations, and writes the results to the console.

## Error Description

### Error: `java.io.IOException: mkdir of file:/tmp/.../state/.../_metadata failed`

This error occurs when Spark is unable to create necessary directories for state storage during streaming operations. The specific issue is with creating directories in the `/tmp` filesystem for storing state data. This can be due to permission issues or the non-existence of the specified directories.

The error traceback typically looks like this:
java.io.IOException: mkdir of file:/tmp/temporary-.../state/.../_metadata failed
    at org.apache.hadoop.fs.FileSystem.primitiveMkdir(FileSystem.java:1357)
    at org.apache.hadoop.fs.DelegateToFileSystem.mkdir(DelegateToFileSystem.java:185)
    at org.apache.hadoop.fs.FilterFs.mkdir(FilterFs.java:219)
    at org.apache.hadoop.fs.FileContext$4.next(FileContext.java:809)
    ...
    at org.apache.spark.sql.execution.streaming.state.StateStoreRDD.compute(StateStoreRDD.scala:125)
    ...
    at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2672)
    at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2608)
    ...
Caused by: java.io.IOException: mkdir of file:/tmp/temporary-.../state/.../_metadata failed
    at org.apache.hadoop.fs.FileSystem.primitiveMkdir(FileSystem.java:1357)
    at org.apache.hadoop.fs.DelegateToFileSystem.mkdir(DelegateToFileSystem.java:185)
    ...
    at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
    at java.base/java.lang.Thread.run(Thread.java:833)

This issue prevents the Spark Streaming application from running successfully because it cannot manage its state data properly. The problem often arises due to the directory not being writable or accessible by the Spark application. I have tried various ways to solve it but have not found a solutio :(.

## Next Steps

Further investigation and adjustments are required to resolve this issue. This may include checking directory permissions, ensuring the directory exists, or specifying an alternative directory path for state storage.
