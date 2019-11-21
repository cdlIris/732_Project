from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession, types
from pyspark.sql import functions

import sys, os, re, string

conf = SparkConf().setAppName('streaming')

sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

assert sc.version >= '2.3'  # make sure we have Spark 2.3+

spark = SparkSession.builder.appName('streaming').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
kafka = 'localhost:9092'
zookeeper = "localhost:2181"



def main():
    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe','bitcoin_minute').load()
    values = messages.select(messages['value'].cast('string'))
    data = values.withColumn('timestart', functions.split(values['value'], ' ')[0]).withColumn('high_start',functions.split(values['value'],' ')[1]).withColumn('low_start',functions.split(values['value'],' ')[2]).withColumn('open_start',functions.split(values['value'],' ')[3]).withColumn('volumefrom_start',functions.split(values['value'],' ')[4]).withColumn('volumeto_start',functions.split(values['value'],' ')[5]).withColumn('close_start',functions.split(values['value'],' ')[6]).withColumn('timeend',functions.split(values['value'],' ')[7]).withColumn('high_end',functions.split(values['value'],' ')[8]).withColumn('low_end',functions.split(values['value'],' ')[6]).withColumn('open_end',functions.split(values['value'],' ')[7]).withColumn('volumefrom_end',functions.split(values['value'],' ')[8]).withColumn('volumeto_end',functions.split(values['value'],' ')[9]).withColumn('close_end',functions.split(values['value'],' ')[10])
    data =data.select('timestart','high_start','low_start','open_start','volumefrom_start','volumeto_start','close_start','timeend','high_end','low_end','open_end','volumefrom_end','volumeto_end','close_end')
    #data1 = data.select('timestart',functions.unix_timestamp("", 'yyyy-MM-dd hh-aa-ss').cast("timestamp"))
    # uery=query.withColumn()
    #data = data.withColumn('timestart'.functions.)

    stream = data1.writeStream.format('console').outputMode('append').start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    main()