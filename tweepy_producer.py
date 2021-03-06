# -*- coding: utf-8 -*-

import os


import sys


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaProducer

import time
import requests
import json
from datetime import datetime


class StdOutListener(StreamListener): #rewrite the tweepy socket class
    def __init__(self, star_time, kafka_producer,topic):

        StdOutListener.count = 0
        self.star_time = star_time  # record the program beginning time
        
        self.producer=kafka_producer
        self.count=0
        

    def on_data(self, data):  # here we deal with each streaming

        msg=data 
        self.count+=1
        print('time:::  ',time.strftime("%b %d %Y %H:%M:%S", time.gmtime()), 'total tweets:: ',self.count)
        #print(json.loads(msg))
        self.producer.send(topic, msg.encode('ascii'))# send to kafka producer and wait for receiver

        return 1

    def on_status(self, status):
        return 1

    def on_error(self, status):
        print(status)


def twitt_stream(kafka_producer,topic): # write tweepy function

    common_time = time.time()  # make a record of begining time we open the tweets
    listener = StdOutListener(int(common_time), kafka_producer,topic)
    auth = OAuthHandler("PQEim5Uq9jFq3YiMGF12CS7oz", "8gYnr83KbscFqaqE0I5vvGKIjehcVXwGvd43fvR7UL2iEpzhyE")
    auth.set_access_token("1065559266784878592-9VP0iOYDmVzkD84iaEKNVZHk0jb6fi",
                          "9qETNhtRPrN02QpG4yyTqZnj101HqYPQXViVO5veWm964")
    stream = Stream(auth, listener)

    stream.filter(languages=["en"], track=['btc', 'bitcoin', 'BitCoin', 'cryptocurrency'])
    

if __name__ == "__main__":
    topic=sys.argv[1]
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,1,0)) # create kafka producer instance
    twitt_stream(producer,topic)
    



