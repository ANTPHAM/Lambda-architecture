#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 19:19:10 2019

@author: antoine

# Migrate .avro files from  Hdfs to MongoDB (with a statistical analysis by using Spark)
"""
#%%
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from io import BytesIO
#import json
import fastavro
from pymongo import MongoClient
#%%

#%%
from datetime import datetime, timedelta
import time
import schedule
#%%

#%%
import findspark
findspark.init("/home/eric/spark-2.4.4")
#%%

#%%
conf = SparkConf().set('spark.driver.host','127.0.0.1')
sc = SparkContext(master='local', appName='myAppName',conf=conf)
spark = SparkSession.builder.getOrCreate()
#%%

#%%
def CountHashtags():
    # connect to  the database in MongoDb       
    try: 
        conn = MongoClient() 
        print("Connected successfully!!!") 
    except:   
        print("Could not connect to MongoDB") 
  
      
    db = conn.tweets 
    collection = db.batch_test2
    timemsToDate = datetime.now() - timedelta(hours = 1)
    
    if len(str(timemsToDate.day)) > 1:
        d = str(timemsToDate.day)
        
    else:
        d = str(0) + str(timemsToDate.day)
    
    if len(str(timemsToDate.month)) > 1:
        mon = str(timemsToDate.month)
        
    else:
        mon = str(0) + str(timemsToDate.month)
	
    
    if len(str(timemsToDate.hour)) > 1:
        h = str(timemsToDate.hour)
        
    else:
        h = str(0) + str(timemsToDate.hour)
        
    dateToString = d + mon + str(timemsToDate.year) + h
       
    # Connect to HDFS and read all files .avro
    rdd = sc.binaryFiles('hdfs://localhost:9000/data/tweet/master/sous-dataset-1/' + dateToString + '*.avro') 
    
    nodes = rdd.flatMap(lambda args: fastavro.reader(BytesIO(args[1])))

    # Convert to a resilient distributed dataset (RDD) of rows
    rows = nodes.map(lambda node: Row(**node))
    
    # Create a schema to define the type of each column
    schema_type=StructType([StructField("id", LongType(), False), StructField("created_time", StringType(),False),
                            StructField("time_ms", LongType(),True), StructField("hashtags", ArrayType(StringType()),False),
                            StructField("text", StringType(),False)])
    
    # Convert to a Spark dataframe
    df = spark.createDataFrame(rows, schema_type)
    
    # Cache data to avoid re-computing everything
    df.persist()
    # Doing some data analysis
    c = df.select(explode('hashtags').alias("value")).groupBy("value").count().orderBy(desc("count")).collect()
    D = {"name": dateToString}
    l = []
    for el in c:
        d = {}
        d['hashtags'] = el[0]
        d['count'] = el[1]
        l.append(d)
    D['hashktags'] = l
    
    # Insert the analyzed data to MongoDb    
    collection.insert_one(D)
    print("\n==============  Already put the file %s to the table 'batch_test2' on Mongodb" %dateToString)
    print('the function is executed at: %s ' %str(datetime.now()))

if __name__ == "__main__":
    schedule.every().hour.do(CountHashtags)
    while True: 
        schedule.run_pending() 
        time.sleep(1)  
        

