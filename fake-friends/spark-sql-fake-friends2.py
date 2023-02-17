from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark SQL Practice").getOrCreate()

df = spark.read.options(header = 'True', inferSchema = 'True', delimiter = ',').csv("./fakefriends-header.csv")
# df.show()
# df.printSchema()

df.createOrReplaceTempView('friends')
sql_query = """
select
  age
  , sum(friends) as friends
from
  friends
group by age

"""

friends_by_age = spark.sql(sql_query)
friends_by_age.show()
