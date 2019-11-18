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
cluster = Cluster(['199.60.17.32', '199.60.17.65'])
session = cluster.connect('dca108')

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
    session.execute("DROP TABLE IF EXISTS bitcoin_history")
    session.execute(
        'CREATE TABLE bitcoin_history (id UUID, datetime TIMESTAMP, symbol TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume_btc  FLOAT, volume_usd FLOAT, time_period INT, delta FLOAT, PRIMARY KEY (id, datetime));')
    batch = BatchStatement()
    with open('Exmo_BTCUSD_1h.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        num = 0
        for row in spamreader:
            insert_sql = session.prepare(
                "INSERT INTO bitcoin_history (id, datetime, symbol, open, high, low, close, volume_btc, volume_usd, time_period, delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            (dt, symbol, op, high, low, close, volume_btc, volume_usd) = row
            time_period = reformat_date(dt)
            delta = float(close) - float(op)
            dt = datetime.strptime(dt, "%Y-%m-%d %H-%p")

            batch.add(insert_sql, (uuid.uuid1(), dt, symbol, float(op), float(high), float(low), float(close), float(volume_btc), float(volume_usd), time_period, delta))
            num += 1

            if num == 150:
                session.execute(batch)
                batch.clear()
                num = 0
    session.execute(batch)
    session.execute("SELECT * FROM bitcoin_history")





if __name__ == '__main__':
    inputs = sys.argv[1]

    main(inputs)
