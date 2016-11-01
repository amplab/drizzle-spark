# Drizzle: Low Latency Execution for Apache Spark

Drizzle is a low latency execution engine for Apache Spark that is targetted
towards stream processing and iterative workloads. Currently, Spark uses a BSP
computation model, and notifies the scheduler at the end of each task. Invoking
the scheduler at the end of each task adds overheads and results in decreased
throughput and increased latency.

In Drizzle, we introduce group scheduling, where multiple batches (or a group) of iterations
are scheduled at once. This helps decouple the granularity of task execution from scheduling and 
amortize the costs of task serialization and launch.

## Drizzle Example
The current Drizzle prototype exposes a low level API using the `runJobs` method in [SparkContext](core/src/main/scala/org/apache/spark/SparkContext.scala). This method takes in a
`Seq` of RDDs and functions to execute on these RDDs. Examples of this API can be seen in [DrizzleSingleStageExample](examples/src/main/scala/org/apache/spark/examples/DrizzleSingleStageExample.scala) and [DrizzleRunningSum](examples/src/main/scala/org/apache/spark/examples/DrizzleRunningSum.scala).

To try out Drizzle, we first build Spark based on existing [instructions](http://spark.apache.org/docs/latest/building-spark.html).
We can run the `DrizzleRunningSum` example with 4 cores for 10 iterations with group size 10
```
  ./bin/run-example --master "local-cluster[4,1,1024]" org.apache.spark.examples.DrizzleRunningSum 10 10
```
To compare this with existing Spark, we can run the same 10 iterations but now with a group size of 1 
```
  ./bin/run-example --master "local-cluster[4,1,1024]" org.apache.spark.examples.DrizzleRunningSum 10 1
```

## Status
The source code in this repository is a research prototype and only implements the scheduling techniques described in our paper.
The existing Spark unit tests pass with our changes and we are actively working on adding more tests.
We are also working towards a Spark JIRA to discuss integrating Drizzle with the Apache Spark project. 

Finally we would like to note that extensions to integrate Structured Streaming and Spark ML will be implemented separately.

## For more details
For more details about the architecture of Drizzle please see our
[Spark Summit 2015 Talk](https://spark-summit.org/2016/events/low-latency-execution-for-apache-spark/)
and our [Technical Report](http://shivaram.org/drafts/drizzle.pdf)

## Acknowledgements
This is joint work with Aurojit Panda, Kay Ousterhout, Mike Franklin, Ali Ghodsi, Ben Recht and Ion
Stoica from the [AMPLab](http://amplab.cs.berkeley.edu) at UC Berkeley.
