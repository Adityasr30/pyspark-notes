# Pyspark Notes
## What is Pyspark?
- Analytical processing engine
- Parallel processing (multiple nodes)
- Powerful data processing and machine learning applications
- Python + spark = PySpark

## Who uses pyspark?
Data science and machine learning community

## When to use pandas and pyspark?
- For smaller dataset --> Pandas (single node)
- For large data --> Spark (distributed processing)

## What are the features of pyspark?
- In-memory computation
- Distributed processing 
- Can be used with many cluster managers (Spark, Yarn, Mesos etc)
- Fault tolerant
- Immutable (RDDs are immutable)
- Cache and persistent
- Lazy evaluation
- Supports ANSI SQL

## What is meant by in-memory computation?
Spark brings data into the memory (Ram). As we know it takes less time to fetch and process data from the memory as it is fast.

## The data that needs to be processed is very huge and the memory is limited. So how spark is able to process this data in memory?
- Data partitioning: splits data into chunks and process these chunks in distributed fashion across different machines in a cluster
- Lazy evaluation: doesn't immediately execute operations. stores execution plan in a DAG.
    DAG contains sequence of transformations. It gets executed once any action method is called.
- Data pipelining
- Using serialization: apache parquet
- Out of memory processing: in case, where data exceeds the memory, spark intelligently spills the data into the disks.

## What is DAG?
- Directed acylic graph
- Directed: graph is directed. operations are in a specific order/ sequence. Each operation depends upon the previos operation.
- Acyclic: there are no loops or cycles in the sequence operations

## What is meant by serialization?
- Serialization refers converting an object into byte stream.
- Serialized objects occupies less space in the disk/memory.
- They can be used to transfer data over a network or store in memory.

## What is apache parquet?
- It is a storage file format
- Columnar data structure: values of each column are stored together
- Compression: it support compression algorithms like gzip, snappy and LZO.
- Schema evolution:
    - Parquet supports schema evolution
    - If the schema of data evolves over time, we do not require to process the data again, it adapts to the schema.
- Predicate pushdown:
    - Parquet supports predicate pushdown
    - It pushes filtering operations down to the storage layer.
    - In other words, it filters out data before processing.
    - It tries to keep filtering conditions (conditions in WHERE clause) close to the data.
    - Advantage:
	 1. It allows parquet to skip reading rows or columns that do not satisfy the filter conditions
	 2. Thus, improving query performance by reducing the amount of data that needs to be read from disk.
- Not suitable for smaller data.

## What is column pruning?
- Optimization technique
- Identifies and eliminates unnecessary columns
- Also called as column elimination/ column projection

## What is meant by distributed processing?
- Spark jobs are executed in a cluster
- This cluster contains combination of many machines
- The job is divided into chunks and each of them is executed on seperate machines
- This kind of processing is called as distributed processing.

## What is a job?
A job can mean a set of instructions/code (transformations) written in python/scala that needs to be executed.

## What is a cluster manager?
- Responsible for managing available resources in a cluster
- Manages the execution of tasks in a cluster of machines
- Resources can be CPU, memory, storage and network
- Also used to schedule jobs
- Moniters health and status of the cluster, detects failures of nodes.
- Reschedules tasks if failed.
- Example: Mesos, kubernetes, Yarn
- Apache Spark also has a standalone cluster manager

## How spark provides fault tolerance?
- By storing lineage, Spark can reconstruct lost data partitions by recomputing partitions.
- Data replication: sparks replicate each partition on multiple nodes.
- Task failure: if a task gets failed on a node, spark assigns the task to another worker node
- Driver recovery: driver is the single point of failure in spark
- Cluster managers: they have built in fault tolerance features.

## What is cache and persist?
- Both are methods used to persist frequently accessed data on memory or disk, providing faster access.
- Cache is used to persist data in memory. It can also be written as persist(StorageLevel.MEMORY_ONLY)
- Persist allows to choose between many storage levels:
   - memory only
   - memory and disk
   - memory and disk ser
   - disk only
- Use persist() to persist the data
- Use unpersist() to unpersist the data

## What are the different storage levels?
- Memory only
    - stores data as deserialized objects
    - may result in data loss, if partitions are recomputed
- Memory and disk
    - If data doesn't fit in the memory, it spills it into the disk.
    - May take long time to fetch data, because it is stored in the disk
- Memory and disk ser
- Disk only
- On heap
    - memory allocated by the jvm
    - managed by garbage collector
    - provides faster access
    - no need to manage storage
- Off heap
    - storage allocated outside the jvm
    - may be used, if the user want control over how the data is stored
    - on heap storage can be limited, therefore it is efficient to store large data in off heap storage

## What are the advantages of pyspark?
- In memory, distributed processing
- Lightning fast
- Real time data processing

## Explain spark architecture.
![alt text](https://sparkbyexamples.com/wp-content/uploads/2020/02/spark-cluster-overview.png?ezimgfmt=ng:webp/ngcb1)
- Master-slave architecture
- Master - driver, slave - workers
- Spark context is the entry point to spark application
- SparkContext resides on driver program
- Cluster manager
    - Spark can work with various cluster managers:
	- Standalone mode: it's own cluster manager
	- Mesos
	- Yarn
	- Kubernetes
- Driver:
    - Entry point of spark application
    - Runs on a dedicated node
    - Divides and assigns tasks to worker nodes.
- Executors:
    - Runs the actual tasks
    - Excutes tasks in parallel, stores data in memory
    - Each executor runs in its jvm
    - Has certain amount of cpu cores allocated to it
- A worker node can have multiple executors.

## What is shuffle and data locality?
- Shuffle redistrivutes data accross partitions
- Occurs during aggregation or grouping
- Involves data movement
- Expensive process
- Shuffle operations can be optimized by using data locality, meaning data is processed on the same node where it is stored.

## What are the components of spark eco-system?
- Spark SQL
- Spark Streaming
- Spark MLlib
- GraphX

## Which languages supports spark?
- Python
- R
- Java
- Scala
- Sql

## What is checkpointing?
- Stores/persists intermediate rdds or dataframes to a reliable storage system.
- Fault tolerance
- Helps to recover in case of failure.
- Avoids recomputation of dataframes in case of failures.
- Lineage dependency: spark maintains a lineage, which includes series of transformations
- The reliable system can be: s3, hdfs etc. 
- Checkpointing incurs additional I/O overhead and storage requirements, so it should be used judiciously and selectively for RDDs or DataFrames that are expensive to recompute or have long lineages.
- Enable checkpointing: 
  1. sparkContext.setCheckpointDir()
  2. spark.conf.set("spark.sql.streaming.checkpointLocation","<checkpoint_directory>")
  3. checkpointing on RDDs: RDD.checkpoint()
  4. checkpointing on dataframes: DataFrame.writeStream().option("checkpointLocation","<checkpoint_directory>").start()

## What is the difference between DAG and lineage?
- DAG is a logical execution plan that represents the sequence of transformations and actions in a spark application
- Lineage is the history or record of transformations applied to an RDD or dataframe.

## Explain Spark UI.
- A spark job is divided into something called stages.
- Each stage contains some set of tasks.
- Generally, stages are divided where there is shuffling of data.
- Example:
   1. Adding data --> stage 1
   2. Collecting data --> stage 2
- Spark Architecture
   1. Driver
   2. Worker nodes
   3. Each worker nodes have executors or slots
   4. A worker node can have multiple executors
   5. A slot is nothing but a CPU.
   6. Each of the component runs on a separate JVM.
- A job represents a high-level unit of work triggered by an action operation.
- A job is divided into stages based on data dependencies and presence of shuffle operations.
- Stages are composed of tasks that can be executed in parallel, either without shuffling (narrow stages) or with data shuffling (wide stages).

## List challenges in spark.
1. **Spill**
    - Spill occurs when data requires more memory than available and when the data exceeds memory, it gets spill on to the disk.
    - Performance is reduced because I/O operations on disk are slower.
2. **Shuffle**
    - Redistributing data across partitions during groupBy, reduceByKey, join.
    - Shuffle includes transfer of data over the network which can be a performance bottleneck.
3. **Storage**
    - Poor storage management can lead to excessive memory usage, spilling to disk, or inefficient utilization of storage resources.
4. **Serilization**
    - Process of converting data into suitable format for storage or transmission.
    - Serialization can improve performance, memory usage, and network transfer efficiency.
5. **Skew**
    - Refers to the imbalanced distribution across the partitions.
    - Occurs during aggregations or joins
    - Can impact performance
- Solutions:
    - Optimizing data partitioning and repartitioning to reduce skewness
    - Adjusting spark configurations related to memory management, shuffle behavior, serialization, and strage levels.
    - Using broadcast joins, bucketing etc.
    - Using techniques like caching, checkpointing etc.

## List some of the important spark configurations.
1. Spark driver and executor memory:
    - spark.driver.memory and spark.executor.memory
    - Ensure sufficient memory to execute spark jobs.
2. Number of executors and executor cores:
    - spark.executor.instances
    - spark.executor.cores
3. Spark driver and executor memory overhead:
    - spark.driver.memoryOverhead and spark.executor.memoryOverhead
4. Spark task parallelism:
    - spark.default.parallelism, spark.sql.shuffle.partitions and spark.sql.files.maxPartitionBytes
5. Spark caching and storage levels:
    - spark.storage.memoryFraction, spark.storage.level
6. Serialization:
    - spark.serializer
7. Resource Management:
    - spark.dynamicAllocation.enabled, spark.shuffle.service.enabled

## Difference between repartition and coalesce.
- Repartition shuffles data across the cluster.
- Repartition can increase or  decrease number of partitions, involves a full shuffle, and ensures a balanced distribution.
- Coalescing can only decrease the number of partitions, minimizes data movement.

## How the number of partitions are decided in spark?
1. Input data source
2. Default parallelism: spark.default.parallelism
3. Transformation operations: repartition and coalesce
4. Custom partitioning: partitionBy

## How many partitions?
- Default properties:
    - spark.default.parallelism (default: Total no. of CPU cores)
    - spark.sql.files.maxPartitionBytes (default: 128 MB)
    - spark.sql.files.openCostInBytes (default: 4 MB)
- Number of partition calculation:
    - total_bytes = Sum of sizes of all data files + (No. of files * openCostInBytes)
    - bytesPerCore = total_bytes / default.parallelism
    - maxSplitBytes = Minimum(maxPartitionBytes, Maximum(bytesPerCore, openCostInBytes))
    - num_partitions = total_bytes / maxSplitBytes
- Example​:
    - 2 csv files of 62 MB each​
    - total_bytes = (2 * 62 * 1024 * 1024) + (2 * 4 * 1024 * 1024) = 138412032​
    - bytesPerCore = 138412032 / 7 = 19773147​
    - maxSplitBytes = Minimum(134217728, Maximum(19,773,147, 4,194,304)) = 19773147​
    - num_partitions = 138412032 / 19773147 = 6.49

## Explain broadcast join.
- Used when size of one dataset is smaller than the other.
- Used to optimize the join process.
Steps:
- One dataset, known as broadcast or small dataset is small enought to firt entirely in memory.
- The other dataset, known as non-broadcast or large dataset is comparatively larger in size.
- Smaller dataset is broadcasted or replicated to all worker nodes.
- Each worker node performs the join locally by combining the local copy of small dataset with corresponding partition of the large dataset.
- Results from each worker node are combined to produce the final result of the join operation.
- Advantages:
   - Reduced data shuffling
   - Local join excecution

## Difference between bucketing and partitionBy.
- Partitioning divides data into separate directories or files based on specific column values, while bucketing distributes data into fixed-size buckets based on a hash function applied to specific column(s).
- Partitioning enables data skipping at the partition level during query execution, reducing the amount of data processed. Bucketing does not provide data skipping but helps in even data distribution for join operations or reducing data skew.
- Partitioning creates separate directories or files for each partition, while bucketing stores buckets as separate files within a partition directory.

## What is dynamic pruning?
Dynamic pruning is an optimization technique used in query processing to selectively skip unnecessary data based on runtime information and query predicates. It aims to reduce the amount of data accessed and processed during query execution, leading to improved performance. Apache Spark supports dynamic pruning through various mechanisms, including predicate pushdown and filter pushdown optimizations.

## What is adaptive query execution?
Adaptive Query Execution (AQE) is a feature introduced in Apache Spark 3.0 to optimize query execution dynamically based on runtime feedback and data statistics. AQE aims to improve the performance and resource utilization of Spark applications by adapting the execution plan and making data-driven decisions during query processing. 

## Difference between client mode and cluster mode.
- Client mode runs the Spark driver program on the machine from which the application is submitted, allowing direct interaction and debugging.
- Cluster mode runs the Spark driver program on one of the cluster's worker nodes, managed by the cluster manager, making it suitable for production deployments.
- The choice between client mode and cluster mode depends on the use case, development needs, and the production environment. For development and debugging, client mode is preferred, while for production use, cluster mode is the standard deployment choice.

## Explain the command: spark-shell --deploy-mode client --master yarn --conf spark.dynamicAllocation.enabled=false --conf spark.sql.adaptive.enabled=false --executor-memory=32g --executor-cores=7 --driver-memory=16g --num-executors=2

1. spark-shell: This command launches the Spark interactive shell, allowing you to interactively execute Spark code and SQL queries.

2. --deploy-mode client: Specifies the deployment mode as "client". In client mode, the Spark driver program runs on the machine where the spark-shell command is executed.

3. --master yarn: Specifies the cluster manager as "yarn". This indicates that Spark will run on a YARN cluster, and YARN will manage the resources and execution of Spark applications.

4. --conf spark.dynamicAllocation.enabled=false: Sets the configuration property spark.dynamicAllocation.enabled to false. This disables dynamic allocation, meaning the number of executors remains fixed throughout the application's lifecycle.

5. --conf spark.sql.adaptive.enabled=false: Sets the configuration property spark.sql.adaptive.enabled to false. This disables adaptive query execution, preventing Spark from dynamically adjusting the execution plan based on runtime statistics and feedback.

6. --executor-memory=32g: Sets the memory per executor to 32 gigabytes (GB). Each executor will have 32GB of memory available for executing tasks.

7. --executor-cores=7: Sets the number of CPU cores per executor to 7. Each executor will be allocated 7 CPU cores for parallel processing.

8. --driver-memory=16g: Sets the memory for the Spark driver program to 16 gigabytes (GB). The driver program runs on the machine where the spark-shell command is executed.

9. --num-executors=2: Specifies the total number of executors to be used in the Spark application. In this case, the application will use 2 executors.

Overall, the command launches the Spark interactive shell in client mode, connecting to a YARN cluster manager with specific configurations. It sets the memory and CPU core allocation for the executors and the driver program, disables dynamic allocation and adaptive query execution, and specifies 2 executors to be used for the application. The Spark shell will be able to interactively run Spark code and SQL queries using the specified resources and configurations.

## What are some of the difficulties when working with csv files?

- No defined schema
- Nested data requires special handling 
- Encoding format limited

## What is Avro and ORC?

**Avro**:
Avro is another open-source data serialization system developed by the Apache Avro project. It is both a data format and a remote procedure call (RPC) framework used for data exchange between systems. Avro is designed to be language-neutral, compact, and fast, making it suitable for big data processing and communication across different programming languages and systems.
Key features of Avro include:

- Data serialization: Avro provides a compact binary data format for efficient data serialization and deserialization.
- Schema evolution: Like Parquet, Avro also supports schema evolution, allowing data schema changes without breaking backward compatibility.
- Dynamic typing: Avro supports dynamic typing, making it easy to work with dynamically changing data.
- Schema resolution: It provides robust schema resolution to handle schema mismatches during data exchange.

**ORC (Optimized Row Columnar)**:
ORC is a columnar storage format developed by the Apache ORC project. It was specifically designed to improve the performance of data processing in the Hadoop ecosystem, particularly for the Apache Hive data warehouse system. ORC aims to provide better compression and speed for read-heavy workloads and analytical queries.
Key features of ORC include:

- Lightweight compression: ORC uses lightweight compression algorithms, such as Zlib and Snappy, to achieve high compression ratios without sacrificing query performance.
- Predicate pushdown: ORC supports predicate pushdown, a technique that can significantly reduce the amount of data read during query execution.
- Type support: Similar to Parquet, ORC supports a wide range of data types, including nested data structures and complex types.
- Lightweight indexes: ORC supports lightweight indexes, which help accelerate the process of finding relevant data during query execution.

In summary, each of these formats has its own set of strengths, and the choice between Parquet, Avro, and ORC will depend on factors like the specific use case, data size, query patterns, and the tools or frameworks you are using in your big data processing pipeline.


## Why is MapReduce slow?
Before Apache Spark, MapReduce already existed. But there are multiple problems with it. 

### Problem 1
The main problem is that it writes every stage into HDFS storage which makes it super slow.  

Example:  

Step 1:
<img width="574" height="347" alt="image" src="https://github.com/user-attachments/assets/5bf70c33-5294-4941-aab1-6cb962904ded" />

Step 2:
<img width="620" height="327" alt="image" src="https://github.com/user-attachments/assets/1f2c0998-e83b-4731-b7b0-d07caecc571f" />

### Problem 2
- Rigid programming model
- Fixed steps - Map and Reduce
- If your tasks require various tasks like filter, joins, aggregations, we'll need to break them into multiple Map Reduce steps.

## How Spark resolved these problems?

1. In-memory computation (No writing of immediate steps into HDFS)
2. Directed Acyclic Graph (Resolves the problem of rigid programming model)
	- Divides into jobs and stages

<img width="792" height="462" alt="image" src="https://github.com/user-attachments/assets/e0737a56-0b2f-4dd9-8bcf-03b8906201d8" />

## Spark Architecture
- The cluster manager resides on the driver or master node.
- Widely used cluster managers are YARN and Kubernetes.
- In order to start the spark application, user needs to run a spark submit command along with the code and configurations.
- This spark submit request will go the the cluster manager.
- The cluster manager will first create an application master container. It decides to create it on which worker node.
- A container is an isolated environment within a node with fixed amount of CPUs and RAM. It is not allowed to use resources beyond what is allocated to it.
- The application master container will start an application driver. Application driver is a JVM process. It's job is to execute the main method of the spark application code.
<img width="648" height="475" alt="image" src="https://github.com/user-attachments/assets/99535166-642d-47f3-8f27-77fcb4b01b76" />

- JVM doesn't understand Python or PySpark. So a PySpark driver will spin up. The PySpark driver's main method will call the application driver.
- Spark core is written in Scala. To make it available for Python developers, it has a JAVA wrapper on top of it and in turn it has a python wrapper on top of it.
<img width="274" height="108" alt="image" src="https://github.com/user-attachments/assets/73788c62-33a6-4742-a39d-69bca69de555" />

- The application driver looks at the request and it goes the cluster manager to request for resources.
- Cluster manager gives the resources. For example, the request is for 3 executors, the cluster manager will randomly pick 3 workers and create executor containers.
- After allocation, it hands over the details to the application driver. The cluster manager tells the location of the executors for example, worker node 1, 2 and 4.
- Application driver then schedules the tasks. The executors execute the tasks and report the results back to the driver.
- If we create an UDF, then each executor container may also spin up a python process because UDF are created by user and not contained by default in spark.  

<img width="1323" height="777" alt="image" src="https://github.com/user-attachments/assets/5ca68a66-41cb-4319-bdc7-646831b0621e" />

<img width="1295" height="967" alt="image" src="https://github.com/user-attachments/assets/e81ac5fd-b456-482b-8d1f-c1d3e07acb08" />

### Deployment modes
- When the application driver runs on one of the worker node in the cluster, it is called as cluster mode.
- When spark submit runs on user machine, then it is called as client mode.

## Transformations
- Transformations are operations that produce a new dataset from an existing one.
- They are lazy operations, which means they are not executed immediately until an action is called.
- Select, filter

**Narrow Transformations**
- No shuffles are involved

**Wide Transformations**
- Shuffles are involved
- Data is redistributed across nodes

## Actions
- An action is an operation that triggers actual computation.
- Collect, show, count, write

## Plan
1. Unresolved logical plan
- This is the initial plan created from the query.
- Spark only understands the structure of the query here, but it has not yet verified anything.
- Spark checks later for:
	- Does the table exist?
	- Do the selected columns exist?
	- Are functions valid?
	- Are data types compatible?
- Example:
SELECT name, age  
FROM employees  
WHERE age > 30  

- At this stage Spark just sees:
	- Read table employees
	- Select name, age
	- Apply filter age > 30

- But it has not validated whether:
	- employees table exists
	- name and age columns are present

2. Logical plan
- Now Spark uses the Catalyst Analyzer to validate the query.
- Spark resolves:
	- Table metadata
	- Column references
	- Data types
	- Function names
- If something is wrong:
	- Spark throws errors like:
		- Table not found
		- Column not found
- Output
	- A fully validated logical representation of the query.

3. Catalyst optimizer
- Now Spark applies optimizations to improve performance.
- This is handled by the Catalyst Optimizer.
- Some important optimizations are:
- Filter Pushdown
	- Move filters closer to the data source.
	- Instead of:
 		- Read all data → Filter later
	- Spark tries:
 		- Filter while reading data
   	- This reduces:
   		- Disk I/O
		- Network transfer
   		- Memory usage
- Projection Pushdown
	- Read only required columns.
 	-  Instead of reading:
  		- name, age, salary, department
    -  Spark may read only:
    	- name, age
	- This reduces unnecessary data reading.
- Other Optimizations
	- Constant folding
	- Predicate simplification
	- Join reordering
	- Partition pruning

4. Physical plan
- Spark now converts the optimized logical plan into multiple possible execution strategies.
- Example:
	- Broadcast Hash Join
	- Sort Merge Join
	- Shuffle Hash Joi
- Spark evaluates these plans using:
	- Statistics
	- Cost model
	- Data size estimates
- Then Spark chooses the best physical plan.

5. DAG scheduler
- After finalizing the physical plan:
- Spark converts it into:
	- Jobs
	- Stages
	- Tasks
- This forms a DAG (Directed Acyclic Graph).

6. Final flow summary  
User Query  
   ↓  
Unresolved Logical Plan  
   ↓  
Analyzed / Resolved Logical Plan  
   ↓  
Optimized Logical Plan  
   ↓  
Physical Plan  
   ↓  
DAG Scheduler  
   ↓  
Stages → Tasks  
   ↓  
Execution on Cluster  

<img width="640" height="445" alt="image" src="https://github.com/user-attachments/assets/421cdd1f-ee1c-4a09-9cdb-60bbe474e1dc" />

## Important concepts
- **Job**
- Created for every action like:
	- show()
	- count()
	- collect()
	- write()
- **Stage**
- A job is divided into stages based on shuffle boundaries.
- Two types:
	- ShuffleMapStage
	- ResultStage
- **Task**
	- Each stage is divided into tasks.
	- Usually:
 		- One partition = One task
   - Tasks run in parallel on executors.

## Spark code example
<img width="1385" height="926" alt="image" src="https://github.com/user-attachments/assets/4a836a92-b31b-46ee-b8c8-4a535206ae20" />

- If the code involves N shuffles/ wide transformations then there will be N+1 stages.

## Spark Executor Memory
<img width="1410" height="783" alt="image" src="https://github.com/user-attachments/assets/48480717-057c-460f-9e95-7838d2b055ef" />

- It is divided into 3 main parts:
	- On-heap memory
 	- Off-heap memory
  	- Overhead

- **On-heap memory**
	- Main memory JVM interacts with
 	- Divided into 4 parts:
  		- Execution memory:
    		- This is where joins, shuffles, sorting etc are executed.
      	- Storage memory:
        	-  Cached objects, broadcast variables etc are stored here.
      	- User memory:
      		- User defined data structures, variables etc are stored here.
      	 - Reserved memory:
      	 	- 300 MB
      	  	- Spark keeps it for it's internal use
      	   		- Internal metadata, Memory tracking, Error handling, Internal objects
	- Execution memory and storage memory are together called unified memory.
 		- This is because both of these can borrow memory from each other.
   		- But priority is given to Execution memory
		- Out of the total on-heap memory, 0.6 fraction is allocated for this unified memory.
  		- Out of 0.6 fraction, 0.5 each fraction is allocated for each Execution and Storage memory.
    	- Example:
     		- executor-memory = 10 GB
       			- Unified memory = 6 GB
          			- Execution memory = 3 GB
             		- Storage memory = 3 GB
                 	- Reserved memory = 300 MB
                  	- User memory = 4 GB - 300 MB = 3.8 GB

- **Off-heap memory**
	- It is disabled by default. Enabled using spark setting.
 	- Useful when processing large data.
  	- Used mainly to reduce:
  		- JVM Garbage Collection (GC)
  	 	- Object overhead
  	  	- Serialization costs
	- GC becomes very expensive with huge datasets. Off-heap memory helps Spark bypass JVM limitations.
 	- Large heap = long garbage collection pauses. Off-heap avoids this problem.
 
- **Overhead**
	- Extra container/process memory required beyond JVM heap.
 	- Additional non-heap memory used for JVM overhead, Python processes, shuffle, networking, and native operations.

### Spill
- In Spark, spilling happens when execution memory used for operations like shuffle, sort, join, or aggregation becomes insufficient. Spark first tries to store intermediate data in heap execution memory and optionally off-heap memory (if enabled). When this memory is full, Spark writes the excess intermediate data to the executor node’s local disk, which is called spilling to disk. Overhead memory is not used for spilling; it is reserved for JVM/native overheads such as Python worker processes, thread stacks, networking, and internal executor operations.
