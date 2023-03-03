
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema
schema = StructType([
  StructField("userID", IntegerType(), True),
  StructField("movieID", IntegerType(), True),
  StructField("rating", IntegerType(), True),
  StructField("timestamp", LongType(), True)
])

# Load up movie data
moviesDF = spark.read.schema(schema).csv("./ml-latest-small/ratings.csv")

# aggregate by movie
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# grab the top 10
topMovieIDs.show(10)

# stop session
spark.stop()
