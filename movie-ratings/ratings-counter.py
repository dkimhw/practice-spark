from pyspark.sql import SparkSession
import collections

# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sc = SparkContext(conf = conf)

spark = SparkSession \
    .builder \
    .appName("Basic CSV Reading") \
    .getOrCreate()

df = spark.read.csv("./ml-latest-small/ratings.csv")
df.show()

# ratings = lines.map(lambda x: x.split()[2])
# result = ratings.countByValue()

# sortedResults = collections.OrderedDict(sorted(result.items()))
# for key, value in sortedResults.items():
#     print("%s %i" % (key, value))
