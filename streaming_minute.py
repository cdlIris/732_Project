from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession, types
from pyspark.sql import functions
from datetime import datetime
import requests, random

import sys, os, re, string

conf = SparkConf().setAppName('streaming')

sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

assert sc.version >= '2.3'  # make sure we have Spark 2.3+

spark = SparkSession.builder.appName('streaming').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
kafka = 'localhost:9092'
zookeeper = "localhost:2181"

DISPLAY_LEN = 3
realtime_prices = []
realtime_times = []

def foreach_batch_function(df, epoch_id):
    prices = [str(p.close_end) for p in df.select("close_end").collect()]
    time = [str(t.time_end) for t in df.select("time_end").collect()]
    print(realtime_prices)
    # assert len(prices)==1
    if len(prices)==0:
        pass
    elif len(realtime_prices)<DISPLAY_LEN:
        realtime_prices.append(prices[0])
        # realtime_times.append(convert_timestamp(time[0]))
        realtime_times.append(time[0])
    else:
        url = 'http://127.0.0.1:5000/realtime/updateData'
        rand_num = random.random()
        rand_list = [rand_num, 1-rand_num]
        del realtime_prices[0]
        del realtime_times[0]
        realtime_prices.append(prices[0])
        # realtime_times.append(convert_timestamp(time[0]))
        realtime_times.append(time[0])
        request_data = {'label': str(realtime_times), 'data': str(realtime_prices), 'proportion':str(rand_list)}
        print(request_data)
        response = requests.post(url, data=request_data)

@functions.udf(returnType=types.StringType())
def convert_timestamp(timestamp):
    return datetime.fromtimestamp(int(timestamp)).strftime("%Y/%m/%d %H:%M")

def main():
    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe','bitcoin_minute').load()
    values = messages.select(messages['value'].cast('string'))
    data = values.withColumn('timestart', functions.split(values['value'], ' ')[0]).withColumn('high_start',functions.split(values['value'],' ')[1]).withColumn('low_start',functions.split(values['value'],' ')[2]).withColumn('open_start',functions.split(values['value'],' ')[3]).withColumn('volumefrom_start',functions.split(values['value'],' ')[4]).withColumn('volumeto_start',functions.split(values['value'],' ')[5]).withColumn('close_start',functions.split(values['value'],' ')[6]).withColumn('timeend',functions.split(values['value'],' ')[7]).withColumn('high_end',functions.split(values['value'],' ')[8]).withColumn('low_end',functions.split(values['value'],' ')[6]).withColumn('open_end',functions.split(values['value'],' ')[7]).withColumn('volumefrom_end',functions.split(values['value'],' ')[8]).withColumn('volumeto_end',functions.split(values['value'],' ')[9]).withColumn('close_end',functions.split(values['value'],' ')[10])
    # convert timestamp to datetime YYYY-MM-DD HH:MM
    data = data.withColumn('time_end', convert_timestamp(data['timeend']))
    data =data.select('timestart','high_start','low_start','open_start','volumefrom_start','volumeto_start','close_start','time_end','high_end','low_end','open_end','volumefrom_end','volumeto_end','close_end')


    stream = data.writeStream.foreachBatch(foreach_batch_function).start()
    
    stream.awaitTermination(600)
    


if __name__ == '__main__':
    main()