#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 17:53:19 2019

@author: antoine

consumerBatchlayer.py
"""


#import os
import hdfs
import fastavro
from datetime import date, datetime, 
import json
from kafka import KafkaConsumer

hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")

schema = {
        "namespace":"antoine.opentreetmap",
        "type": "record",
        "name": "Node",
        "fields": [
                {"name": "id", "type": "int"},
                {"name": "created_time", "type": "string"},
                {"name": "time_ms", "type": "int"},
                {"name": "hashtags", "type": {"type": "array", "items": "string"}
                },
                {"name": "text", "type": "string"},
                ]
        }

consumer = KafkaConsumer("tweet_analysis", bootstrap_servers=["localhost:9092"], group_id="hashtag_dashboard")


for message in consumer:
    tweet = json.loads(message.value.decode())
    ID = tweet["id"]
    text = tweet["text"]
    created_time = tweet["created_at"]
    time_ms = tweet["timestamp_ms"]
    hashtags = [h['text'] for h in tweet["entities"]["hashtags"]] 
    
    
    timemsToDate = datetime.fromtimestamp(int(time_ms[:10]))
    
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
        
    if len(str(timemsToDate.minute)) > 1:
        m = str(timemsToDate.minute)
        
    else:
        m = str(0) + str(timemsToDate.minute)
    
    dateToString = d + mon + str(timemsToDate.year) + h# + m
    print(dateToString + '-' + str(ID) + '.avro')
    print("Tweet ID: {}, created at: {}, timestamp: {}, hahtags: {}, text: {}".format(
            ID,
            created_time,
            time_ms,
            hashtags,
            text
                            
        ))
    tweets = []
    tweets.append({
               "id": int(ID),
               "created_time": str(created_time),
               "time_ms": int(time_ms),
               "hashtags": hashtags,
               "text": str(text)               
                })
    with hdfs_client.write("/data/tweet/master/sous-dataset-1/" + dateToString + '-' + str(ID) + '.avro', overwrite = True) as avro_file:
        fastavro.writer(avro_file, schema, tweets)



        
    
