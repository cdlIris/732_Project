from pyspark.sql import functions, types
import datetime


class Bitcoin:

    def get_data_on_date(self, date):
        data = self.data
        on_date = datetime.datetime.strptime(date, "%Y-%m-%d")
        result = data.select(data['timestamp'], data['Close']).where(data['timestamp']==on_date).orderBy('timestamp').collect()
        return get_label_value(result)


    def get_daily_avg(self):
        data = self.data
        daily_agg = data.withColumn('date', functions.date_trunc('dd', data['timestamp']))
        daily_avg = daily_agg.groupby('date').avg('Close')
        result = daily_avg.select("date", "avg(Close)").orderBy('date').collect()
        return get_label_value(result)

    def get_monthly_avg(self):
        data = self.data
        add_month = data.withColumn('month', functions.month(data['timestamp']))
        monthly_avg = add_month.groupby('month').avg('Close')
        result = monthly_avg.select('month', 'avg(Close)').orderBy('month').collect()
        return get_label_value(result)

    def get_dayofyear_avg(self):
        data = self.data
        day = data.withColumn('day', functions.dayofyear(data['timestamp']))
        day_avg = day.groupby('day').avg('Close')
        result = day_avg.select('day', 'avg(Close)').orderBy('day').collect()
        return get_label_value(result)

    def get_full_data(self):
        data = self.data
        full = data.select('timestamp', 'Close').orderBy('timestamp').collect()
        return get_label_value(full)

    def get_range_data(self, begin, end):
        data = self.data
        begin_date = datetime.datetime.strptime(begin, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end, "%Y-%m-%d")
        result = data.select('timestamp', 'Close').where((data['timestamp']>=begin_date)&(data['timestamp']<=end_date)).orderBy('timestamp').collect()
        return get_label_value(result)

    def __init__(self, spark):
        self.spark = spark
        file_path = "/Users/wendy/CSC/CMPT732/p/bitcoin-usd-hour.csv"
        self.schema = types.StructType([
            types.StructField('Date', types.StringType()),
            types.StructField('Symbol', types.StringType()),
            types.StructField('Open', types.FloatType()),
            types.StructField('High', types.FloatType()),
            types.StructField('Low', types.FloatType()),
            types.StructField('Close', types.FloatType()),
            types.StructField('Volume BTC', types.FloatType()),
            types.StructField('Volume USD', types.FloatType()),
        ])
        data = spark.read.csv(file_path, schema=self.schema).cache()
        self.data = data.withColumn("timestamp", functions.to_timestamp(data['Date'], 'yyyy-MM-dd hh-aa').alias("timestamp")).drop('Date')

@functions.udf(returnType=types.IntegerType())
def get_month(date):
    return date.month

# @functions.udf(returnType=types.IntegerType())
# def get_day(

def get_label_value(rows):
    label = []
    value = []
    for row in rows:
        label.append(row[0])
        value.append(row[1])
    return label, value


