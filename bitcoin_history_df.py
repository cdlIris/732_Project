import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
import csv
from datetime import datetime
import uuid

spark = SparkSession.builder.appName('bitcoin history').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

bitcoin_schema = types.StructType([
    types.StructField('Date', types.StringType()),
    types.StructField('Symbol', types.StringType()),
    types.StructField('Open', types.FloatType()),
    types.StructField('High', types.FloatType()),
    types.StructField('Low', types.FloatType()),
    types.StructField('Close', types.FloatType()),
    types.StructField('Volume BTC', types.FloatType()),
    types.StructField('Volume USD', types.FloatType()),
])


# @functions.udf(returnType=types.StringType())
def reformat_date(date):
    pm_or_am = date[-2:]
    time = int(date[:-3].split(' ')[1])

    if pm_or_am == 'PM' and time != '12':
        time = time + 12
    if pm_or_am == 'AM' and time == '12':
        time = 0

    if 0 <= time <= 3:
        return 1
    elif 4 <= time <= 7:
        return 2
    elif 8 <= time <= 11:
        return 3
    elif 12 <= time <= 15:
        return 4
    elif 16 <= time <= 19:
        return 5
    else:
        return 6

def main(inputs):
    spark = SparkSession.builder.appName('Bitcoin dataframe').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext




if __name__ == '__main__':
    inputs = sys.argv[1]

    main(inputs)
