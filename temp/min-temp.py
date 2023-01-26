
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Find Min Temp") \
    .getOrCreate()

df = spark.read.csv("./1800.csv")
df = df.select(col("_c0").alias('stationID'), col("_c2").alias('entryType'), col("_c3").alias("temperature"))
df = df.filter(df.entryType == "TMIN")
df = df.withColumn('temperature', round(df['temperature']).cast('integer'))

# df.show()
df.select(min('temperature').alias('temp')).show()
