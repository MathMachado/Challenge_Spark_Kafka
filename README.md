# Use Case:
1. Create a Spark Structured Streaming (Python or Scala) Pipeline to publish some data to Kafka.
2. Using Spark Structured Streaming (Python or Scala) read the same data from Kafka and store it in HDFS Parquet - RAW Zone (use any sample XML with nested elements).
3. Reads data from RAW Zone using an Hourly scheduled Spark Batch process and loads the final parquet file – (Processed Zone) 


## Output Data Requirement:
* Create a sample Project Folder Structure for Code i.e. scripts, logs, etc. to show how it will be organized;
* Create sample scripts (pseudo code) and place them in corresponding folder;
* Consider following in the code:
    * Consume Kafka (Offset Maintenance and De-duplication)
    * XML Parsing and flattening
    * Data Validation
      - Dynamic data validation
        - Schema Validation
        - Data Type Validation
        - Data formatting (trim, etc.)
      - Fault Tolerance for Application:
        - Error Handling
        - Continuous Streaming
        - Checkpoint Restarts from a specific restart
    * Partition the data based on a date field in the final Parquet file (Processed Zone) 


## Expectation:
1. We expect to see pseudo code only
2. We would like to see the main Shell Script (spark submit) and the Python/Scala Spark program in the next meeting
3. Refer to github, google, etc. or any other Spark Source/documentation to complete this assignment.
4. We want to see the approach (pseudo code) and want you to do a code walk-through to understand what functions/methods you used and reasoning behind it. We are not expecting a working code example. 


## Other inputs for consideration/discussion

Q1. How will you make sure only delta/new records are pulled from RAW Zone to Processed Zone?

Q2. How will you move the old data from RAW Zone so that it does not become very large?

Q3. How will you run these programs – Cluster or Client

Q4. How will you decide how many Cores and Executors are needed for 1) Spark Stream Job 2) Hourly Spark Batch

Q5. How do you ensure we do not run into small file issue in the RAW and Processed Zone 

__________________
# Pseudo code to solve the exercise

## XML Data File (books.xml)

For the sake of ilustration, consider the following XML data structure:

```
<library>
  <book>
    <title>Spark in Action</title>
    <author>
      <name>John Doe</name>
      <country>USA</country>
    </author>
    <publication_year>2021</publication_year>
  </book>
  <book>
    <title>Data Science for Beginners</title>
    <author>
      <name>Jane Smith</name>
      <country>Canada</country>
    </author>
    <publication_year>2022</publication_year>
  </book>
  <!-- Add more books if needed -->
</library>
```

#### Example
* Book Record 1:
   * Title: Spark in Action
   * Author Name: John Doe
   * Author Country: USA
   * Publication Year: 2021

* Book Record 2:
   * Title: Data Science for Beginners
   * Author Name: Jane Smith
   * Author Country: Canada
   * Publication Year: 2022

* Book Record 3:
   * Title: Advanced Machine Learning
   * Author Name: Robert Johnson
   * Author Country: UK
   * Publication Year: 2020

## Create a sample Project Folder Structure for Code i.e. scripts, logs, etc. to show how it will be organized.
### Project Folder Structure

```
project_root/
|-- scripts/
|   |-- streaming_producer.py
|   |-- streaming_consumer.py
|   |-- batch_process.py
|   |-- run_spark_job.sh
|-- logs/
|   |-- streaming_producer.log
|   |-- streaming_consumer.log
|   |-- batch_process.log
|-- conf/
|   |-- spark_config.xml
|   |-- kafka_config.properties
|-- data/
|-- processed_zone
|-- raw_zone
|-- streaming_producer_checkpoint/
|-- streaming_consumer_checkpoint/
|-- batch_process_checkpoint/
```

#### Comments related to the file run_spark_job.sh
##### Placing Shell Script in scripts/ Folder

If the shell script is primarily focused on orchestrating the execution of Spark jobs, weu can place it in the scripts/ folder. **This is a common practice, especially if the script is responsible for setting up environment variables, dependencies, and submitting Spark jobs**.

```
project_root/
|-- scripts/
|   |-- run_spark_job.sh   --> Shell script in the folder script
|   |-- streaming_producer.py
|   |-- streaming_consumer.py
|   |-- batch_process.py
|-- logs/
|   |-- streaming_producer.log
|   |-- streaming_consumer.log
|   |-- batch_process.log
|-- conf/
|   |-- spark_config.xml
|   |-- kafka_config.properties
|-- data/
|-- processed_zone/
|-- raw_zone/
|-- streaming_producer_checkpoint/
|-- streaming_consumer_checkpoint/
|-- batch_process_checkpoint/
```

##### Placing Shell Script in conf/ Folder
If the shell script is more focused on managing configurations and dependencies, we might choose to place it in the conf/ folder. **This is less common but can be appropriate if the script is primarily concerned with setting up environment variables and configurations**.

```
project_root/
|-- scripts/
|   |-- streaming_producer.py
|   |-- streaming_consumer.py
|   |-- batch_process.py
|-- logs/
|   |-- streaming_producer.log
|   |-- streaming_consumer.log
|   |-- batch_process.log
|-- conf/
|   |-- run_spark_job.sh   --> Shell script in the folder conf
|   |-- spark_config.xml
|   |-- kafka_config.properties
|-- data/
|-- processed_zone/
|-- raw_zone/
|-- streaming_producer_checkpoint/
|-- streaming_consumer_checkpoint/
|-- batch_process_checkpoint/
```

**Explanation**
1. **scripts/**

* **streaming_producer.py**: Pseudo code (or the script) for the streaming data producer. This script generates or fetches XML data and publishes it to a Kafka topic.
* **streaming_consumer.py**: Pseudo code (or the script) for the streaming data consumer. This script reads data from a Kafka topic, processes XML data (parsing, validation, and flattening), and writes it to HDFS in the RAW Zone.
* **batch_process.py**: Pseudo code (or the script) for the batch process. This script reads data from the RAW Zone, performs additional data validation, partitions the data based on a date field, and writes it to the Processed Zone.

2. **logs/**
   
This folder is intended to store log files generated by Spark or/and my custom logging. We have separated logs for streaming and batch processes.

* **streaming_producer.log**: log file for streaming producer;
* **streaming_consumer.log**: log file for streaming consumer;
* **batch_process.log**: log file for batch process.

3. **conf/**

This folder contain configuration files for the Spark applications, Kafka configurations, and any other necessary configurations. Below, example of files that on this folder:

* **spark_config.properties**: This file contains common Spark configurations. Each line represents a key-value pair, where the key is the Spark configuration parameter, and the value is its corresponding setting. Below, a very simple configuration:

```
# Application Name
spark.app.name=MyStreamingApplication

# Master URL
spark.master=local[*]

# Kafka Configuration
spark.kafka.bootstrap.servers=my_kafka_bootstrap_servers

# HDFS Configuration
spark.hdfs.impl=org.apache.hadoop.hdfs.DistributedFileSystem
spark.hdfs.namenode=hdfs://my_hdfs_namenode

# Spark Streaming Configuration
spark.streaming.batch.duration=10

# Other Spark Configurations
# ...
```

* **kafka_config.properties**: This file contains configuration settings related to Apache Kafka. The kafka_config.properties file is then used in our Spark application to configure the Kafka integration, ensuring consistency and ease of maintenance across different components. Here's an example of what it might look like:

```
# Kafka Bootstrap Servers
bootstrap.servers=my_kafka_bootstrap_servers

# Kafka Consumer Group ID
group.id=my_consumer_group

# Kafka Auto Offset Reset
auto.offset.reset=earliest

# Kafka Key and Value Deserializers
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Kafka Topic to Consume From
input.topic=book_data_topic
```

**Explanation of the code above**:
* **bootstrap.servers**: Specifies the list of Kafka bootstrap servers.
* **group.id**: Specifies the Kafka consumer group ID.
* **auto.offset.reset**: Defines the strategy for resetting the offset when there is no initial offset or the current offset does not exist on the server (e.g., "earliest" or "latest").
* **key.deserializer** and **value.deserializer**: Specify the deserializer classes for the key and value of Kafka messages.
* **input.topic**: Specifies the Kafka topic from which the streaming consumer will consume data.

4. **data/**

This folder contain sample data to be used for testing or as input for my streaming producer. For example: sample_data.xml.

5. **raw_zone/**

This is the folder where the streaming consumer writes the raw data in Parquet format. The actual Parquet files and their structure would be stored here.

7. **processed_zone/**

This is the folder where the batch process writes the final processed data in Parquet format. 

8. **streaming_producer_checkpoint/**:

Checkpointing is a mechanism that allows Spark to save the state of a streaming or batch application to recover from failures. This folder will contain the metadata and state information related to the Spark Streaming producer application and it's automatically managed by Spark.

9. **streaming_consumer_checkpoint/**:

Checkpointing is a mechanism that allows Spark to save the state of a streaming or batch application to recover from failures. Similar to the producer, this folder will contain metadata and state information for the Spark Streaming consumer application and it's automatically managed by Spark.

10. **batch_process_checkpoint/**:

Checkpointing is a mechanism that allows Spark to save the state of a streaming or batch application to recover from failures and it's automatically managed by Spark.

**Important Notes about checkpoints:**
- The content inside these checkpoint folders is managed by Spark and should not be manually modified or deleted during application execution.
- These folders are crucial for ensuring fault tolerance, as Spark applications can recover from failures by using the checkpointed state.
- Adjust the paths in your Spark application code based on your actual project directory structure.

When a Spark application is executed, Spark will create and manage the checkpoint data inside these folders to keep track of the application's progress and state. If an application fails, Spark can use the checkpointed state to recover and resume processing from where it left off.


## Create sample scripts (pseudo code) and place them in corresponding folder
### Pseudo Code for Streaming Producer (streaming_producer.py):

```
# Import necessary libraries
from kafka import KafkaProducer
import time
import uuid

# Initialize Kafka producer
kafka_bootstrap_servers = "my_kafka_bootstrap_servers"
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
```

**Explanation of the code above**: In the first part, the code imports the required libraries (KafkaProducer, time, uuid). It initializes a Kafka producer with the specified bootstrap servers (the addresses of Kafka brokers). This producer will be used to send messages to the Kafka topic.


```
from faker import Faker
import random
import uuid

fake = Faker()

# Function to generate or fetch XML data
def generate_or_fetch_xml_data():
    # Generate fake data for a book
    def generate_fake_book():
        record_id = str(uuid.uuid4())   # --> unique identifier
        title = fake.title()
        author_name = fake.name()
        author_country = fake.country()
        publication_year = random.randint(2000, 2023)
        
        return f"""
        <book>
          <record_id>{record_id}</record_id>
          <title>{title}</title>
          <author>
            <name>{author_name}</name>
            <country>{author_country}</country>
          </author>
          <publication_year>{publication_year}</publication_year>
        </book>
        """

    # Generate a random number of books (between 1 and 5)
    num_books = random.randint(1, 5)
    
    # Assemble the XML data with multiple books
    xml_data = f"""
    <library>
      {''.join(generate_fake_book() for _ in range(num_books))}
    </library>
    """

    return xml_data
```

**Explanation of the code above**: The code defines the function generate_or_fetch_xml_data that generates or fetches XML data. This function is continuously generating fake XML data.

```
# Start a loop to simulate continuous data streaming
while True:
    # Generate or fetch XML data
    xml_data = generate_or_fetch_xml_data()

    # Generate a unique ID for deduplication
    message_id = str(uuid.uuid4())

    # Send XML data with unique ID to Kafka topic
    producer.send("book_data_topic", key=record_id, value=xml_data)

    # Sleep for a specified interval
    time.sleep(streaming_interval)
```

**Explanation of the code above**: 
* The code enters an infinite loop to simulate continuous data streaming. In each iteration, it generates or fetches XML data using the generate_or_fetch_xml_data function. It then generates a unique ID (message_id) using the uuid library for deduplication purposes.
* The XML data, along with the unique ID, is sent to the Kafka topic ("book_data_topic") using the producer.send method. This is where each XML message is published to the Kafka topic with a unique key (message_id).
* Finally, the loop sleeps for a specified interval (streaming_interval), simulating the streaming interval between data points.

**Important Details**:
* This code assumes the existence of a Kafka topic named "book_data_topic".
* The generate_or_fetch_xml_data function is a placeholder; in a real-world scenario, this function would acquire XML data dynamically.
message_id is used as the key for Kafka messages to enable deduplication.


### Pseudo Code for Streaming Consumer (streaming_consumer.py)

```
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, xpath_string
from pyspark.sql.streaming import OutputMode
from pyspark import SparkConf

# Read Spark configuration from external file (Assuming 'spark_config.properties' is in the 'conf/' folder)
spark_conf = SparkConf()
with open("conf/spark_config.properties") as spark_config_file:
    spark_conf.setAll(
        [("spark." + k, v) for k, v in map(str.strip, line.split('=')) for line in spark_config_file]
    )

# Initialize Spark Session
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
```

**Explanation of the code above**: The code starts by importing the required libraries (SparkSession, col, xpath_string, OutputMode). It then initializes a Spark session with the application name set to "KafkaConsumer".

```
# Kafka Configurations
kafka_bootstrap_servers = "my_kafka_bootstrap_servers"
kafka_topic = "book_data_topic"
kafka_group_id = "my_consumer_group"
```

**Explanation of the code above**: The code sets up Kafka configurations such as bootstrap servers, topic name, and consumer group ID.

```
# Read Kafka configuration from external file
kafka_config = {}
with open("conf/kafka_config.properties") as kafka_config_file:
    kafka_config = dict(map(str.strip, line.split('=')) for line in kafka_config_file)

# Extract Kafka configuration parameters
kafka_bootstrap_servers = kafka_config.get("kafka.bootstrap.servers", "")
kafka_topic = kafka_config.get("kafka.topic", "")
kafka_group_id = kafka_config.get("kafka.group.id", "")

# Read data from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("group.id", kafka_group_id)
    .option("checkpointLocation", "streaming_consumer_checkpoint/") \
    .load()
)
```

**Explanation of the code above**: The code uses the readStream API to create a streaming DataFrame (df) by reading data from Kafka. It specifies the Kafka bootstrap servers, topic to subscribe to, and the consumer group ID.

```
# Function to parse and flatten XML data
def parse_and_flatten_xml(df):
    # Parse XML and flatten nested elements
    flattened_df = df.selectExpr(
        "CAST(xpath_string(title) AS STRING) as title",
        "CAST(xpath_string(author.name) AS STRING) as author_name",
        "CAST(xpath_string(author.country) AS STRING) as author_country",
        "CAST(xpath_string(publication_year) AS STRING) as publication_year"
    )
    return flattened_df
```

**Explanation of the code above**: The code defines the function parse_and_flatten_xml that takes a DataFrame (df) as input and applies XML parsing and flattening. It uses the selectExpr method to extract values from XML elements using XPath expressions and casts them to STRING types.

```
# Function to validate XML data
def validate_data(df):
    # Implement data validation logic
    # For simplicity, let's assume all data is valid
    return df
```

**Explanation of the code above**: The code defines the function validate_data that takes a DataFrame (df) as input. This function is a placeholder for implementing data validation logic. In the provided example, it assumes that all data is valid.

```
# Function for actual data processing logic
def process_data(df):
    # Actual processing logic
    processed_df = df.select("title", "author_name", "author_country", "publication_year")
    return processed_df
```

**Explanation of the code above**: The code defines the function process_data that takes a DataFrame (df) as input and applies the actual processing logic. In this example, it selects specific columns ("title", "author_name", "author_country", "publication_year").

```
# Function to partition data based on a date field
def partition_data(df, date_field):
    # Implement partitioning logic based on the specified date field
    # For simplicity, let's assume partitioning by publication year
    partitioned_df = df.withColumn("year_partition", col(date_field))
    return partitioned_df
```

**Explanation of the code above**: The code defines the function partition_data that takes a DataFrame (df) and a date field as input. It applies partitioning logic based on the specified date field. In the provided example, it adds a new column "year_partition" based on the "publication_year" column.

```
# Apply processing pipeline
processed_df = (
    df
    .transform(parse_and_flatten_xml)
    .transform(validate_data)
    .transform(process_data)
    .transform(lambda df: partition_data(df, "publication_year"))
)
```

**Explanation of the code above**: The code applies a processing pipeline to the streaming DataFrame (df). It transforms the data by successively applying the functions parse_and_flatten_xml, validate_data, process_data, and partition_data.

```
# Write data to HDFS Parquet in the RAW Zone
query = (
    processed_df
    .writeStream.format("parquet")
    .outputMode(OutputMode.Append())
    .option("path", "hdfs://my_hdfs/raw_zone/")
    .start()
)
```

**Explanation of the code above**: The code writes the processed streaming DataFrame (processed_df) to HDFS in Parquet format. It uses the writeStream API with the "parquet" format, specifies the output mode as "Append", sets the HDFS path for the RAW Zone, and starts the streaming query.

```
# Await termination of the streaming query
query.awaitTermination()
```

**Explanation of the code above**: The code waits for the termination of the streaming query. This ensures that the application keeps running and processing data until it is manually terminated.


### Pseudo Code for Batch Process (batch_process.py)

```
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("BatchProcessor").getOrCreate()

# Read data from RAW Zone
raw_df = spark.read.parquet("hdfs://my_hdfs/raw_zone/")

# Retrieve the last processed record_id from external storage or checkpoint
last_processed_record_id = get_last_processed_record_id()

# Filter out records that have already been processed
new_records_df = raw_df.filter(col("record_id") > last_processed_record_id)

# Deduplicate based on unique identifiers
deduplicated_df = new_records_df.dropDuplicates(["record_id"])

# Function for actual data processing logic
def process_data(df):
    # Select relevant columns for processing
    processed_df = df.select("record_id", "title", "author_name", "author_country", "publication_year")
    return processed_df

# Apply data processing logic
processed_df = process_data(deduplicated_df)

# Function to partition data based on a date field
def partition_data(df, date_field):
    # For simplicity, let's assume partitioning by year and month
    partitioned_df = df.withColumn("year_month_partition", col(date_field))
    return partitioned_df

# Partition data based on date field
partitioned_df = partition_data(processed_df, "date_field")

# Write data to Processed Zone
partitioned_df.write.mode("append").parquet("hdfs://my_hdfs/processed_zone/")

# Store the last processed record_id for the next run
store_last_processed_record_id(processed_df.selectExpr("max(record_id)").first()[0])
```

**Explanation of the code above**:

* **Read Data from RAW Zone**: Read Parquet data from the RAW Zone, assuming it's stored in HDFS.
* **Retrieve Last Processed Record_id**: Retrieve the last processed record_id from external storage or a checkpoint to identify the starting point for processing new records.
* **Filter Records**: Filter out records that have already been processed, using the last processed record_id as a reference.
* **Deduplication**: Deduplicate the filtered records based on unique identifiers (e.g., record_id).
* **Data Processing Logic**: Define a function (process_data) to perform the actual data processing logic, selecting relevant columns.
* **Apply Data Processing Logic**: Apply the defined data processing logic to the deduplicated records.
* **Partition Data**: Define a function (partition_data) to partition data based on a specified date field (e.g., "date_field").
* **Write Data to Processed Zone**: Write the partitioned data to the Processed Zone in "append" mode.
* **Store Last Processed Record_id**: Store the last processed record_id for the next run to ensure processing only new records.


# Main Shell Script (spark submit) and the Python/Scala Spark program

* **Shell Script (Spark Submit)**: It's a script that includes the spark-submit command with the necessary parameters to run my Spark application. Typically, this script is used to set up any environment variables, configurations, or dependencies before launching the Spark job. The shell script might look something like this:

```
#!/bin/bash

# Set up any necessary environment variables or configurations
export SPARK_HOME=/path/to/spark
export HADOOP_HOME=/path/to/hadoop

# Run the Spark job using spark-submit
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 4g \
  --num-executors 10 \
  --py-files /path/to/my/python/scripts.zip \
  /path/to/my/python_script.py
```

# Questions and Solutions
## Q1. How will you make sure only delta/new records are pulled from RAW Zone to Processed Zone?

To ensure only delta/new records are pulled from the RAW Zone to the Processed Zone, we have applied the following steps:

a) **Unique Identifiers**:

* We are using a unique identifier (record_id) in our data.
* In the streaming producer, we included this identifier when sending data to Kafka.
* In the batch process, we used this identifier for deduplication.

b) **Maintain State**:

* Maintain state information about processed records.
* In Spark Structured Streaming, we can use checkpoints to store the streaming state.

c) **Process Only New Records in Batch**:

* In the batch process (batch_process.py), we are reading the RAW Zone and filtering out records that have already been processed using the maintained state.

## Q2. How will you move the old data from RAW Zone so that it does not become very large?

To address the concern of managing the size of the RAW Zone, we can implement a data retention strategy to periodically clean up or move old data. This strategy may involve archiving, deleting, or moving data to a long-term storage solution. Below is a pseudo code outlining a basic approach for managing the size of the RAW Zone:

```
# Pseudo Code for Managing RAW Zone Size

# Import necessary libraries
import datetime
import shutil

# Define a function to move old data to long-term storage
def move_old_data(raw_data_path, archive_path, retention_days):
    # Calculate the date threshold for old data
    threshold_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
    
    # Identify old data files
    old_data_files = get_old_data_files(raw_data_path, threshold_date)
    
    # Move old data files to the archive path
    move_files(old_data_files, archive_path)

# Define a function to get a list of old data files
def get_old_data_files(raw_data_path, threshold_date):
    # List all files in the RAW Zone
    all_files = list_files(raw_data_path)
    
    # Filter files based on modification date
    old_data_files = [file for file in all_files if get_file_modification_date(file) < threshold_date]
    
    return old_data_files

# Define a function to list all files in a directory
def list_files(directory_path):
    # Implement directory listing logic based on the storage system (e.g., HDFS, local file system)
    # Example: Use HDFS commands or os.listdir() for a local file system
    
    # Return a list of file paths
    return files_list

# Define a function to get the modification date of a file
def get_file_modification_date(file_path):
    # Implement logic to get the modification date based on the storage system
    # Example: Use HDFS commands or os.path.getmtime() for a local file system
    
    # Return the modification date
    return modification_date

# Define a function to move files to a new location
def move_files(files, destination_path):
    # Implement file movement logic based on the storage system
    # Example: Use HDFS commands or shutil.move() for a local file system
    
    # Move files to the destination path
    move_files_logic(files, destination_path)

# Example Usage
raw_data_path = "hdfs://my_hdfs/raw_zone/"
archive_path = "hdfs://my_hdfs/archive/"
retention_days = 30

# Move old data to the archive
move_old_data(raw_data_path, archive_path, retention_days)
```

**Explanation of the code above**

* **move_old_data**: This function calculates a date threshold for old data based on a specified retention period (retention_days). It then identifies old data files using the get_old_data_files function and moves them to an archive path using the move_files function.
* **get_old_data_files**: This function lists all files in the RAW Zone and filters them based on their modification date, considering the threshold date.
* **list_files**: This function lists all files in a given directory. The implementation may vary based on the storage system.
* **get_file_modification_date**: This function retrieves the modification date of a file. The implementation may vary based on the storage system.
* **move_files**: This function moves a list of files to a specified destination path. The implementation may vary based on the storage system.

## Q3. How will you run these programs – Cluster or Client

The decision to run Spark programs on a cluster or a client depends on the nature of the workload, data size, and available resources. Let's explore both options:

1. **Running on a Cluster**: In a cluster mode, Spark applications are submitted to a cluster manager, such as YARN or Apache Mesos. Spark will then allocate resources across the cluster for parallel processing. **This mode is suitable for large-scale data processing and distributed computing**.

* **Pros**:
  * **Scalability**: Easily scales to handle large datasets by utilizing resources across multiple nodes.
  * **Resource Management**: Efficiently utilizes cluster resources by distributing tasks.
  
* **Cons**:
  * **Complexity**: Setting up and managing a cluster might introduce additional complexity. That's why Databricks became so famous, because it somehow helps to prevent that complexity related to cluster management.
  * **Resource Overhead**: There could be some resource overhead in managing a distributed environment.

2. **Running on a Client**: In client mode, the Spark driver runs on the machine that submits the Spark application. The driver communicates with the cluster manager to acquire resources for execution. **This mode is suitable for smaller workloads and when the data can fit into the memory of a single machine**.

* **Pros**:
  * **Simplicity**: Easier to set up and manage as it doesn't require a dedicated cluster.
  * **Resource Isolation**: The client machine can be isolated for running Spark applications.

* **Cons**:
  * **Limited Scale**: May not be suitable for very large datasets or computationally intensive tasks.
  * **Resource Constraints**: The client machine needs sufficient resources to handle the workload.

**Decision Criteria**: 

* **Data Size**: If the data is relatively small and fits into the memory of a single machine, running in client mode might be sufficient. For large-scale data processing, using a cluster is preferable.

* **Computational Intensity**: If the workload is computationally intensive and benefits from parallel processing, a cluster is more suitable.

* **Resource Availability**: Consider the available resources on the client machine. If it has enough memory and processing power, running in client mode might be sufficient.

* **Infrastructure Complexity**: If setting up and managing a cluster introduces too much complexity, running in client mode could be a simpler alternative. Pseudo code for submitting Spark applications can be generic, as it typically involves using the spark-submit script. Below is a simplified example:

```
# Pseudo Code for Running Spark Application

# Submitting in Cluster Mode
spark-submit --class com.example.MySparkApp --master yarn --deploy-mode cluster my_spark_app.jar

# Submitting in Client Mode
spark-submit --class com.example.MySparkApp --master local[4] my_spark_app.jar
```

**Considerations:**
* Adjust the options (--master and --deploy-mode) based on the deployment choice.
* In our specific project structure, we may have a shell script in the scripts/ folder for submitting Spark jobs, and the actual Spark programs in Python or Scala in corresponding folders.

## Q4. How will you decide how many Cores and Executors are needed for 1) Spark Stream Job 2) Hourly Spark Batch

Deciding on the number of cores and executors for Spark Streaming and Spark Batch jobs involves considering factors like the available resources, the nature of the workload, and the desired parallelism. Below are general guidelines, and it's important to adjust these based on the specific use case and performance testing.

1) **Spark Streaming Job**: In a Spark Streaming job, we need to consider the rate at which data is ingested and processed. Key factors include the number of input Kafka partitions, the complexity of processing logic, and the desired throughput.

```
# Pseudo Code for Submitting Spark Streaming Job

spark-submit --class com.example.StreamingApp --master yarn \
  --deploy-mode cluster --num-executors 5 --executor-cores 2 \
  my_streaming_app.jar
```

**Considerations:**
* **Number of Executors**: Adjust based on the available resources and the desired parallelism. We can start with a reasonable number and scale up or down based on performance testing.
* **Executor Cores**: Depending on the complexity of the streaming logic, we can allocate an appropriate number of cores per executor. It's common to allocate 1-3 cores per executor.

2) **Hourly Spark Batch Job**: For Spark Batch jobs, the resource allocation depends on the size of the data to be processed and the complexity of the processing logic. Batch jobs often involve reading large datasets from storage and performing extensive computation.

```
# Pseudo Code for Submitting Hourly Spark Batch Job

spark-submit --class com.example.BatchApp --master yarn \
  --deploy-mode cluster --num-executors 10 --executor-cores 4 \
  my_hourly_batch_app.jar
```

**Considerations:**
* **Number of Executors**: We can increase the number of executors to parallelize the processing of large datasets. Then, we can test performance with different numbers.
* **Executor Cores**: For batch processing, we might allocate more cores per executor to handle larger computation tasks efficiently. We can allocate 3-5 cores per executor, depending on the workload.

**Additional Considerations**:
* **Resource Availability**: We need to consider the total available resources on the cluster and ensure that we are not overcommitting resources.
* **Dynamic Allocation**: Spark supports dynamic allocation, which allows it to adjust the number of executors based on workload. We may experiment with dynamic allocation for better resource utilization.
* **Performance Testing**: Conducting a performance testing with different configurations to identify the optimal number of cores and executors for the specific workload it suggested and it's a best practice.

## Q5. How do you ensure we do not run into small file issue in the RAW and Processed Zone 

To avoid the small file issue in the RAW and Processed Zones, we need to optimize the writing strategy for Spark. The small file problem arises when there are many small files stored on a distributed file system, leading to inefficiencies in storage, management, and processing. Here are some strategies to mitigate this issue:

1. **Coalesce or Repartition Data**: In both the RAW and Processed Zones, consider coalescing or repartitioning the data to reduce the number of output files.

```
# Coalesce or Repartition Data
coalesced_df = raw_df.coalesce(10)  # Adjust the number based on the data size
coalesced_df.write.parquet("hdfs://my_hdfs/raw_zone/")

# Or using repartition
repartitioned_df = processed_df.repartition(10)  # Adjust the number based on the data size
repartitioned_df.write.mode("append").parquet("hdfs://my_hdfs/processed_zone/")
```

**Considerations:**
* Adjust the number of partitions based on the size of the data. Experiment to find an optimal balance.

2. **Use Dynamic Partitioning**: For partitioned data, we recommend using dynamic partitioning to optimize the directory structure and file organization.

```
# Dynamic Partitioning
partitioned_df.write.mode("append").partitionBy("date_field").parquet("hdfs://my_hdfs/processed_zone/")
```

**Considerations:**
* Choose a meaningful partitioning column, such as the date field, to create a hierarchical directory structure.

3. **Tune File Size and Compression**: Configure the file size and compression options to optimize storage efficiency.

```
# Tune File Size and Compression
partitioned_df.write.mode("append").option("maxRecordsPerFile", 10000).parquet("hdfs://my_hdfs/processed_zone/")
```

**Considerations:**
* Adjust the maxRecordsPerFile parameter based on the data and storage preferences.
* Consider using compression (e.g., Snappy, Gzip) to reduce storage space.

4. **Periodic Data Compaction**: Implement periodic data compaction to merge small files and optimize storage.

```
# Example Shell Script for Compaction (Run as a cron job)
hadoop distcp hdfs://my_hdfs/processed_zone/ hdfs://my_hdfs/processed_zone_compacted/
hdfs dfs -rm -r hdfs://my_hdfs/processed_zone/
hdfs dfs -mv hdfs://my_hdfs/processed_zone_compacted/ hdfs://my_hdfs/processed_zone/
```

**Considerations:**
* Schedule compaction based on the data growth pattern. A cron job or a scheduled process can be used.

5. **Bucketing (Optional)**: For certain use cases, consider using bucketing to further optimize storage by grouping data based on specific columns.

```
# Bucketing (Optional)
processed_df.write.mode("append").bucketBy(10, "column_name").parquet("hdfs://my_hdfs/processed_zone/")
```

**Considerations:**
* Bucketing is more suitable for scenarios where data can be logically grouped by specific columns.

# Additional Considerations:
* **Optimal Configuration**: Adjust the parameters based on performance testing and the characteristics of the data.
* **Monitoring**: Monitor the file sizes and directory structure regularly to identify potential issues.
* **Automation**: Automate the compaction process to run at optimal intervals.


# Fault Tolerance

Fault tolerance in Apache Spark is crucial for ensuring that the data processing jobs can recover from failures. Some key mechanisms for achieving fault tolerance include:

1. **Checkpoints:**
   - Implementing periodic checkpoints helps save the metadata information to a reliable distributed file system.
   - Checkpoints allow Spark to recover lost data and continue processing from a consistent state.

2. **Replication:**
   - Spark automatically replicates resilient distributed datasets (RDDs) across multiple nodes.
   - In case of a node failure, Spark can recover the lost data from the replicated copies.

3. **Task Retry:**
   - Spark allows task retry in case of task failures. The number of retries can be configured.

# Error Handling

Effective error handling is essential for robust data processing applications. Consider the following strategies:

1. **Logging:**
   - Implement detailed logging in the Spark applications to capture information about errors.
   - Logs can be helpful for debugging and understanding the root cause of failures.

2. **Monitoring:**
   - Utilize monitoring tools to keep track of application metrics and detect any anomalies.
   - This includes tools like Spark's built-in metrics, monitoring systems, and logging aggregators.

3. **Alerting:**
   - Set up alerts based on specific error conditions or performance thresholds.
   - Receive notifications when unexpected issues occur.

# Continuous Streaming

Continuous streaming refers to the ability to process data continuously in real-time. For Spark Structured Streaming, the framework provides a high-level API for stream processing. Key considerations include:

1. **Micro-Batch Processing:**
   - Structured Streaming processes data in micro-batches, providing a higher-level abstraction for developers.

2. **Watermarking:**
   - Watermarking is a crucial concept in event time processing with stateful operations, for handling late data in event-time processing.
   - It helps define a threshold beyond which events are considered late and are not processed.

4. **Stateful Operations:**
   - Structured Streaming supports stateful operations, allowing you to maintain state across batches.
   - Stateful operations are useful for scenarios where you need to remember information over time.

5. **Checkpointing:**
   - Checkpointing is essential for fault tolerance in streaming applications.
   - It helps recover the streaming application from a consistent state in case of failures.

# Checkpoint Restarts from a Specific Point

Checkpointing facilitates restarting a Spark Streaming application from a specific point in case of failures. When restarting from a checkpoint, Spark will recover the necessary metadata and continue processing from where it left off.

Here's a simplified example:

```python
# Example of Structured Streaming with Checkpointing
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()

# Set up a streaming DataFrame with defined schema and source
streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "my_kafka_bootstrap_servers").option("subscribe", "my_kafka_topic").load()

# Define the streaming operations and transformations
# ...

# Define the checkpoint location
checkpoint_location = "hdfs://my_hdfs/checkpoints/"

# Configure the streaming query with checkpointing
query = (
    streaming_df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("checkpointLocation", checkpoint_location)
    .start()
)

# Await termination of the streaming query
query.awaitTermination()
```

In this example, the `checkpointLocation` option is set to a specific HDFS path. This path will store the checkpoint information. When restarting the application, Spark will use this checkpoint information to recover the streaming query's state and continue processing from the specified point.

Ensure you adjust the placeholders such as "my_kafka_bootstrap_servers," "my_kafka_topic," and provide an appropriate checkpoint location based on the environment and requirements.
