from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession, types
from pyspark.sql import functions
from datetime import datetime, timedelta
import requests, random
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
import sys, os, re, string
from pyspark.sql.window import Window

conf = SparkConf().setAppName('streaming')

sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

assert sc.version >= '2.3'  # make sure we have Spark 2.3+

spark = SparkSession.builder.appName('streaming').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
kafka = 'localhost:9092'
zookeeper = "localhost:2181"

DISPLAY_LEN = 3
LIST_LEN = DISPLAY_LEN+1
realtime_prices = []
realtime_predictions = []

bitcoin_schema = types.StructType([
    types.StructField('timestamp', types.TimestampType()),
    types.StructField('Open', types.FloatType()),
    types.StructField('High', types.FloatType()),
    types.StructField('Low', types.FloatType()),
    types.StructField('Close', types.FloatType()),
    types.StructField('Volume USD', types.FloatType()),
    types.StructField('Volume BTC', types.FloatType()),
    types.StructField("Weighted", types.FloatType())
])

col_order = ["timestamp", "Open", "High", "Low", "Close", "Weighted"]
                            
model = PipelineModel.load("bitcoin_model_OHLCW")

def foreach_batch_function(df, epoch_id):
    data = [df.collect()]
    global realtime_prices


    if len(data[0]) == 0:
        pass
    elif len(realtime_prices) < DISPLAY_LEN:
        realtime_prices.append(data[0][0])
    else:
        url = 'http://127.0.0.1:5000/realtime/updateData'
        df = spark.createDataFrame(realtime_prices)
        # model = PipelineModel.load("bitcoin_model_OHLCW")
        df.show()
        w = Window.partitionBy().orderBy(functions.col("timestamp").cast('long'))

        for feature in col_order[1:]:
            for diff in range(1,3):
                name = feature + "_lag_{}".format(diff+1)
                df = df.withColumn(name, functions.lag(df[feature], count=diff).over(w))
        df = df.na.drop()
        
        predictions = model.transform(df)
        predictions.select("prediction").show()

        pred = predictions.select("prediction").collect()
        realtime_predictions.append(str(pred[0].prediction))
        diff = LIST_LEN-len(realtime_predictions)
        if diff > 0:
            pred_list = [None for i in range(diff)]+realtime_predictions
        else:
            pred_list = [num for num in realtime_predictions]
            del realtime_predictions[0]
        real_time = [str(item.timestamp) for item in realtime_prices] + [str(realtime_prices[-1].timestamp+timedelta(minutes=1))]
        real_prices = [str(item.Close) for item in realtime_prices] + [None]
        request_data = {'label': str(real_time), 'data': str(real_prices), 'prediction':str(pred_list)}
        print(request_data)
        response = requests.post(url, data=request_data)

        del realtime_prices[0]
        realtime_prices.append(data[0][0])
        assert(len(realtime_prices) == DISPLAY_LEN)
                             
      
@functions.udf(returnType=types.StringType())
def convert_timestamp(timestamp):
    return datetime.fromtimestamp(int(timestamp)).strftime("%Y/%m/%d %H:%M")

def main():
    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe','bitcoin_minute').load()
    values = messages.select(messages['value'].cast('string'))
    data = values.withColumn(
        'timestart', functions.split(values['value'], ' ')[0]).withColumn(
        'high_start',functions.split(values['value'],' ')[1]).withColumn(
        'low_start',functions.split(values['value'],' ')[2]).withColumn(
        'open_start',functions.split(values['value'],' ')[3]).withColumn(
        'Volume BTC',functions.split(values['value'],' ')[4]).withColumn(
        'Volume USD',functions.split(values['value'],' ')[5]).withColumn(
        'close_start',functions.split(values['value'],' ')[6]).withColumn(
        'timeend',functions.split(values['value'],' ')[7]).withColumn(
        'High', functions.split(values['value'],' ')[8]).withColumn(
        'Low', functions.split(values['value'],' ')[9]).withColumn(
        'Open', functions.split(values['value'],' ')[10]).withColumn(
        'Volumefrom_end', functions.split(values['value'],' ')[11]).withColumn(
        'Volumeto_end', functions.split(values['value'],' ')[12]).withColumn(
        'Close', functions.split(values['value'],' ')[13])

    # convert timestamp to datetime YYYY-MM-DD HH:MM
    data = data.withColumn('time_end', convert_timestamp(data['timeend'])).drop("timeend")
    data = data.withColumn('timestamp', functions.unix_timestamp("time_end", 'yyyy/MM/dd HH:mm').cast("timestamp")).drop("time_end")
    data = data.select(data["timestamp"],
                       data["Open"].cast("float"),
                       data["High"].cast("float"),
                       data["Low"].cast("float"),
                       data["Close"].cast("float"),
                       (data["Volume USD"] / (sys.float_info.min + data["Volume BTC"])).alias("Weighted"))
                      
    
    
    stream = data.writeStream.foreachBatch(foreach_batch_function).start()
    stream.awaitTermination(60000)
    


if __name__ == '__main__':
    main()
