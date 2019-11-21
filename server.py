from app import create_app
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext



def init_spark_session():
    conf = SparkConf().setAppName('weather')
    
    sc = SparkContext(conf=conf, pyFiles=['historical.py', 'app.py'])
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 2)
 
    return sqlContext, ssc


if __name__ == "__main__":
    spark, ssc = init_spark_session()
    
    app = create_app(spark)
    
    
    app.run()
    

