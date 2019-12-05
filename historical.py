from pyspark.sql import functions, types
import datetime

"""
A Bitcoin model that contains a dataframe of historical information with 
methods to get aggregated information
"""
class Bitcoin:
    # Get average Close price for each hour of day
    def get_hourly_avg(self):
        data = self.data
        hourly_agg = data.withColumn('hour', functions.hour(data['timestamp']))
        
        hourly_avg = hourly_agg.groupby('hour').avg('Close')
        result = hourly_avg.select("hour", "avg(Close)").orderBy("hour")
        result.cache()
        return get_label_value(result.collect())
    
    # Get average Bitcoin volume for each hour of day 
    def get_hourly_avg_v(self):
        data = self.data
        hourly_agg = data.withColumn('hour', functions.hour(data['timestamp']))
        hourly_avg = hourly_agg.groupby('hour').avg('Volume BTC')
        result = hourly_avg.select("hour", "avg(Volume BTC)").orderBy("hour")
        result.cache()
        return get_label_value(result.collect())

    # Get daily average Close price
    def get_daily_avg(self):
        data = self.data
        daily_agg = data.withColumn('date', functions.date_trunc('dd', data['timestamp']))
        daily_avg = daily_agg.groupby('date').avg('Close')
        result = daily_avg.select("date", "avg(Close)").orderBy('date')
        result.cache()
        return get_label_value(result.collect())
    
    # Get daily average Bitcoin volume
    def get_daily_avg_v(self):
        data = self.data
        daily_agg = data.withColumn('date', functions.date_trunc('dd', data['timestamp']))
        daily_avg = daily_agg.groupby('date').avg('Volume BTC')
        result = daily_avg.select("date", "avg(Volume BTC)").orderBy('date')
        result.cache()
        return get_label_value(result.collect())

    # Get average Close price for each month of year
    def get_monthly_avg(self):
        data = self.data
        add_month = data.withColumn('month', functions.month(data['timestamp']))
        monthly_avg = add_month.groupby('month').avg('Close')
        result = monthly_avg.select('month', 'avg(Close)').orderBy('month')
        result.cache()
        return get_label_value(result.collect())
    
    # Get average Bitcoin volume for each month of year
    def get_monthly_avg_v(self):
        data = self.data
        add_month = data.withColumn('month', functions.month(data['timestamp']))
        monthly_avg = add_month.groupby('month').avg('Volume BTC')
        result = monthly_avg.select('month', 'avg(Volume BTC)').orderBy('month')
        result.cache()
        return get_label_value(result.collect())

    # Get average Close price for each day of year
    def get_dayofyear_avg(self):
        data = self.data
        day = data.withColumn('day', functions.dayofyear(data['timestamp']))
        day_avg = day.groupby('day').avg('Close')
        result = day_avg.select('day', 'avg(Close)').orderBy('day')
        result.cache()
        return get_label_value(result.collect())

    # Get average Bitcoin volume for each day of year
    def get_dayofyear_avg_v(self):
        data = self.data
        day = data.withColumn('day', functions.dayofyear(data['timestamp']))
        day_avg = day.groupby('day').avg('Volume BTC')
        result = day_avg.select('day', 'avg(Volume BTC)').orderBy('day')
        result.cache()
        return get_label_value(result.collect())

    # Get all Close price with timestamp
    def get_full_data(self):
        data = self.data
        full = data.select('timestamp', 'Close').orderBy('timestamp')
        full.cache()
        return get_label_value(full.collect())
    
    # Get all Bitcoin volume with timestamp
    def get_full_data_v(self):
        data = self.data
        full = data.select('timestamp', 'Volume BTC').orderBy('timestamp')
        full.cache()
        return get_label_value(full.collect())
    

    def __init__(self, spark):
        self.spark = spark
        
        file_path = "./bitcoin-usd-history.csv"
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
        data = spark.read.csv(file_path, schema=self.schema)
        data = data.withColumn("timestamp", functions.to_timestamp(data['Date'], 'yyyy-MM-dd hh-aa')).drop('Date').cache()
        self.data = data.where(data['timestamp'].isNotNull())

# Helper function to get the month from timestamp
@functions.udf(returnType=types.IntegerType())
def get_month(date):
    return date.month

# Helper function to format data
def get_label_value(rows):
    label = []
    value = []
    for row in rows:
        label.append(row[0])
        value.append(row[1])
    return label, value
