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

    

class StdOutListener(StreamListener):
    def __init__(self, star_time, kafka_producer,topic,batch=3):
        self.batch = batch
        
        StdOutListener.count = 0
        self.star_time = star_time  # record the program beginning time
        self.senti_collect = []  # each batch time's collection of sentimental comments
        self.producer=kafka_producer

    def on_data(self, data):  # here we deal with each streaming

        msg=data
        #print("\n********tweepy********流：： ",msg,'\n')
        
        #【试验区】
        tmp=json.loads(msg)
        cc=int(int(tmp['timestamp_ms'])/1000)
        kaka=datetime.fromtimestamp(cc)
        print('打印时间:::  ',kaka)
        
        
        self.producer.send(topic, msg.encode('ascii'))

        return 1

    def on_status(self, status):
        return 1

    def on_error(self, status):
        print(status)


def twitt_stream(kafka_producer,topic):

    common_time = time.time()  # beginning time
    interva = 3  # the time inverval updating tweets and trading decision
    listener = StdOutListener(int(common_time), kafka_producer,topic,interva)
    auth = OAuthHandler("PQEim5Uq9jFq3YiMGF12CS7oz", "8gYnr83KbscFqaqE0I5vvGKIjehcVXwGvd43fvR7UL2iEpzhyE")
    auth.set_access_token("1065559266784878592-9VP0iOYDmVzkD84iaEKNVZHk0jb6fi",
                          "9qETNhtRPrN02QpG4yyTqZnj101HqYPQXViVO5veWm964")
    stream = Stream(auth, listener)

    stream.filter(languages=["en"], track=['btc', 'bitcoin', 'BitCoin', 'cryptocurrency'])
    


if __name__ == "__main__":
    topic='my_tweets'
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,1,0))
    twitt_stream(producer,topic)
    



