# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 15:20:10 2019

@author: bt116
"""

import sys
import os
from pyspark.sql.window import Window
import copy
from pyspark.ml import PipelineModel
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


import numpy as np
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.4,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('streaming example').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')

# spark.sparkContext.setCheckpointDir("/spark_checkpoint");

import json
from datetime import datetime
import time 
import requests


from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
sid_obj = SentimentIntensityAnalyzer() 

def get_tweepy_time(line): #data preprocessing to get the time term and make change
    tmp=json.loads(line)
    record_time=int(int(tmp['timestamp_ms'])/1000)
    return datetime.fromtimestamp(record_time).strftime("%Y-%m-%d %H:%M:%S")

def get_text(line): #data preprocessing to get the text column
    
    tmp=json.loads(line)
    return tmp['text']

def get_senti_score(line): # we use vaderSentiment to get sentimental scores
    
    sentiment_dict = sid_obj.polarity_scores(line)   
    return str(sentiment_dict['compound'])+' '+str(sentiment_dict['neg'])+' '+str(sentiment_dict['neu'])+' '+str(sentiment_dict['pos'])

udf_senti_score = functions.udf(get_senti_score)
udf_get_text=functions.udf(get_text)
udf_get_tweepy_time=functions.udf(get_tweepy_time)

model = PipelineModel.load("tweet_model")
def main(topic1):
    
    messages = spark.readStream.format('kafka').option('subscribe',topic).option('kafka.bootstrap.servers', 'localhost:9092').load()
    print('successful connect to get tweepy streaming........')
    values = messages.select(messages['value'].cast('string'))
    
    values=values.withColumn('text',udf_get_text(values['value']).cast('string'))
    values=values.withColumn('senti_score',functions.split(udf_senti_score(values['text']),' '))#convert to Array structure spark datatype
    values=values.withColumn('compound',values['senti_score'][0].cast('float'))
    values=values.withColumn('negative',values['senti_score'][1].cast('float'))
    values=values.withColumn('neutral',values['senti_score'][2].cast('float'))
    values=values.withColumn('positive',values['senti_score'][3].cast('float'))
    
    values=values.withColumn('str_timestamp',udf_get_tweepy_time(values['value']).cast('string'))
  
    values=values.withColumn('unix_timestamp',functions.unix_timestamp('str_timestamp', 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))
    values=values.select('str_timestamp','unix_timestamp','compound','negative','neutral','positive') # finally, we get these six columns

    def processRow(df, epoch_id):  #this is the BATCH function to convert structrued streaming to spark dataframe
        print("*********batch number:: ***********",epoch_id)
        #here we could save file to local or backup these logs to the remote service
        #by applying window function, we can split each minute to 5 interval with 12s each, this fits our training model requirement
        df=df.groupBy(functions.window("unix_timestamp", "12 seconds")).agg(functions.avg('compound').alias('compound'),functions.avg('negative').alias('negative'),functions.avg('neutral').alias('neutral'),functions.avg('positive').alias('positive'))
        df=df.select('window.end','compound','negative','neutral','positive').orderBy('end').limit(5)

        
        if df.count()>=5: #we have the previous 5 interval's output for each minutes,then do the prediction by using sentiment model
            print("Begin model prediction here...............")
            w = Window.partitionBy().orderBy(functions.col("end").cast('long'))
            
            for feature in ["negative", "neutral", "positive", "compound"]:
                for diff in range(1, 5):
                    name = feature + "_lag_{}".format(diff)
                    df = df.withColumn(name, functions.lag(df[feature], count=diff).over(w))
            df = df.na.drop()

            predictions = model.transform(df)
            predictions.select("prediction", "probability").show()
            # probability = [P(close-open<0), P(close-open>0)]
            url = 'http://127.0.0.1:5000/realtime/updateDecision'
            pred = predictions.select("prediction", "probability").collect()
            print(pred[0].prediction)
            prob = pred[0].probability
            # print("\n\n!!!!!!!!!", prob, pred[0].prediction)
            request_data = {'proportion': str(prob)}
            print(request_data)
            response = requests.post(url, data=request_data)

        # df.show(5,False)

    #I set trigger=60s to pass the streaming each 1 minutes 
    stream = values.writeStream.trigger(processingTime='60 seconds').outputMode('Append').foreachBatch(processRow).start()  
    stream.awaitTermination(6000)

if __name__ == '__main__':
    
    topic=sys.argv[1]
    main(topic)
