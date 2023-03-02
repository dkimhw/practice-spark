from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemp").getOrCreate()
schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("itemID", IntegerType(), True),
    StructField("price", FloatType(), True)
])

# Read the file
df = spark.read.schema(schema).csv("./customer-orders.csv")
df.printSchema()

# Aggregate
ordersByCustomers = df.groupBy("customerID").agg(func.round(func.sum("price"), 2).alias("total_spent"))
ordersByCustomersSorted = ordersByCustomers.sort(ordersByCustomers.total_spent.desc())
ordersByCustomersSorted.show()
