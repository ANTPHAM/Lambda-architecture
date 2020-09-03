#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 09:39:20 2019

@author: antoine

producer.py
"""

import twitter 
import json
from kafka import KafkaProducer


def Producer():
    
    producer = KafkaProducer(bootstrap_servers= ["localhost:9092","localhost:9093"])
    
    consumer_key = 'YOUR_KEY'
    consumer_secret = 'YOUR_SECRET'
    token = 'YOUR_TOKEN'
    token_secret = 'YOUR_TOKEN_SECRET'
    oauth = twitter.OAuth(token, token_secret, consumer_key, consumer_secret)
    t = twitter.TwitterStream(auth=oauth)

    sample_tweets_in_english = t.statuses.sample(language="en")
    
    for tweet in sample_tweets_in_english:
       if "delete" in tweet:
           # Deleted tweet events do not have any associated text
           continue

       print("===================================")
       
       data = json.dumps(tweet)
       data = json.loads(data)
       try:
            #Tweet ID
            ID = tweet["id"]
            print("tweet ID:")
            print(ID)
            #Tweet text
            text = tweet["text"]
            print("this is a tweet:")
            print(text)
            created_time = tweet["created_at"]
            print("created at:")
            print(created_time)
            print("timestamp in ms:")
            time_ms = tweet["timestamp_ms"]
            print(time_ms)

            # Collect hashtags
            hashtags = [h['text'] for h in tweet["entities"]["hashtags"]]
            if len(hashtags) > 0:
                print("this is a hashtag:")
                print(hashtags)
                producer.send("tweet_analysis", json.dumps(data).encode(),key=str(ID).encode())
       except:
          pass

if __name__ == "__main__":
    Producer()
