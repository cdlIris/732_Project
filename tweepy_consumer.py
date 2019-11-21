# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 15:20:10 2019

@author: bt116
"""

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


import numpy as np
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'

#sys.path.append('C:\\大数据732-final project\\你的部分\\2019.11.20改动')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('streaming example').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')

import json
from datetime import datetime


from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
sid_obj = SentimentIntensityAnalyzer() 

def get_tweepy_time(line):
    tmp=json.loads(line)
    record_time=int(int(tmp['timestamp_ms'])/1000)
    return datetime.fromtimestamp(record_time).strftime("%Y-%m-%d %H:%M:%S")

def get_text(line):
    
    tmp=json.loads(line)
    return tmp['text']


def get_senti_score(line):
    
    sentiment_dict = sid_obj.polarity_scores(line)
   
    return str(sentiment_dict['compound'])+' '+str(sentiment_dict['neg'])+' '+str(sentiment_dict['neu'])+' '+str(sentiment_dict['pos'])
    #return [sentiment_dict['compound'],sentiment_dict['neg'],sentiment_dict['neu'],sentiment_dict['pos']]


udf_senti_score = functions.udf(get_senti_score)
udf_get_text=functions.udf(get_text)
udf_get_tweepy_time=functions.udf(get_tweepy_time)


def main(topic1):
    
    messages = spark.readStream.format('kafka').option('subscribe',topic).option('kafka.bootstrap.servers', 'localhost:9092').load()
    print('成功')
    values = messages.select(messages['value'].cast('string'))
    values=values.withColumn('text',udf_get_text(values['value']).cast('string'))
    values=values.withColumn('senti_score',functions.split(udf_senti_score(values['text']),' '))
    values=values.withColumn('compound',values['senti_score'][0].cast('float'))
    values=values.withColumn('neg',values['senti_score'][1].cast('float'))
    values=values.withColumn('neu',values['senti_score'][2].cast('float'))
    values=values.withColumn('pos',values['senti_score'][3].cast('float'))
    
    values=values.withColumn('str_timestamp',udf_get_tweepy_time(values['value']).cast('string'))
    values=values.withColumn('unix_timestamp',functions.unix_timestamp('str_timestamp', 'yyyy-MM-dd HH:mm:ss'))
    values=values.select('unix_timestamp','compound','neg','neu','pos')
    
    stream = values.writeStream.format('console').outputMode('append').start()
    stream.awaitTermination(600)

if __name__ == '__main__':
    topic='my_tweets'

    main(topic)