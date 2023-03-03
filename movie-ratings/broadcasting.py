
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
  movieNames = {}
  with codecs.open("./ml-latest-small/ratings.csv", 'r', encoding="ISO-8859-1", errors='ignore') as f:
    for line in f:
        fields = line.split(',')
        if fields[0] not in ["userId", "movieId", "rating", "timestamp\r\n"]:
          movieNames[int(fields[0])] = fields[1]
  return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()


# Broadcast movie names
nameDict = spark.sparkContext.broadcast(loadMovieNames())
print("nameDict Data", nameDict.value[1591])

# Create schema
schema = StructType([
  StructField("userID", IntegerType(), True),
  StructField("movieID", IntegerType(), True),
  StructField("rating", IntegerType(), True),
  StructField("timestamp", LongType(), True)
])


# Load up movie data as df
moviesDF = spark.read.schema(schema).csv("./ml-latest-small/ratings.csv")

# Group by and count ratings
movieCounts = moviesDF.groupBy("movieID").count()
movieCounts.show()

# Create a udf to look up movie names from our broadcasted dict
def lookupName(movieID):
   return nameDict.value[movieID]

lookupNameUDF = functions.udf(lookupName)

# # Add a movieTitle col using the new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(functions.col("movieID")))
moviesWithNames.show()

# # Sort the results
# sortedMoviesWithNames = moviesWithNames.orderBy(functions.desc("count"))
# sortedMoviesWithNames.show(10, False)
# spark.stop()
