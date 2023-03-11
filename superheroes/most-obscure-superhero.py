from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import udf


spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("./Marvel-Names.txt")

lines = spark.read.text("./Marvel-Graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

leastPopular = connections.sort(func.col("connections").asc()).first()
leastPopularName = names.filter(func.col("id") == leastPopular[0]).select("name").first()

connections.createOrReplaceTempView("connections")
names.createOrReplaceTempView("names")
heroesOneConnection = spark.sql("select name from names where id in (select distinct id from connections where connections = 1)")
heroesOneConnection.show()

print(leastPopularName[0] + " is the least popular superhero with " + str(leastPopular[1]) + " co-appearances.")
