from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Fake Friends") \
    .getOrCreate()

df = spark.read.csv("./fakefriends.csv")
df = df.withColumn('_c3', df['_c3'].cast('float'))

totals_by_age = df.groupBy('_c2').agg(avg('_c3').alias("avg_friends"))
totals_by_age = totals_by_age.withColumnRenamed('_c2', 'user_age')
totals_by_age = totals_by_age.withColumn('avg_friends', round(totals_by_age['avg_friends']).cast('integer'))
totals_by_age.show()
