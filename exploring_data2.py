import time

start_time = time.time()

# from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
# sc = SparkContext.getOrCreate()

spark = SparkSession.builder.appName("gauravchaapp").getOrCreate()

# print("started")

# print(type(spark))

# rdd = spark.sparkContext.parallelize([
#     Row(name="gaurav", age=25),
#     Row(name="brandon", age=40),
#     Row(name="sonal", age=22)
# ])
#
# df = spark.createDataFrame(rdd)
# df.show()

# print("app ended")

bggreviewsSchema = StructType([
    StructField("number", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("comment", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("name", StringType(), True)
])

bggreviewsDF = spark.read.csv("dataset/bgg-19m-reviews.csv", schema=bggreviewsSchema, header=True)

bggreviewsDF.show(10)
    # prints the line


# to get the number of rows
# num_rows = bggreviewsDF.count()
# print("number of rows is, ", num_rows)

# prints the structure (?) of the dataframe
# bggreviewsDF.printSchema()

# gives the summary of the dataframe - count, mean, min, max, etc
# not for string columns lol
# bggreviewsDF.describe('rating').show()

# user_comments_df = bggreviewsDF.select(bggreviewsDF['user'], bggreviewsDF['comment'])
# user_comments_df.show(2)






end_time = time.time()
print("execution time in seconds: ", end_time - start_time)
