# Lambda architecture
# Data pipeline for Tweets analysis

A data pipeline allows the process of moving Tweet data from Tweeter API streaming to a query and reporting layer. 

The data flow process is built on a lambda architecture which has a speed layer ( Kafka/Storm/MongoDb) and a batch layer (HDFS/ MongoDb). 

The data analysis and data trasfomation will be handled by using PySpark.

Environments and required installations:

- kafka_2.12-2.3.0

- zookeeper-3.5.5 

- mongodb v4.2.1 

- storm 2.1.0

- maven 3.5.3

- jdk 11.0.1

- spark-2.4.4

- Anaconda3/python 3.7.4

- Ubuntu 18.04.1 LTS




![Alt text](/schema.png?raw=true "Lambda architecture")
