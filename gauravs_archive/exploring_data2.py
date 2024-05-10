import time

from pyspark.sql.functions import monotonically_increasing_id

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
    StructField("rownum", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("comment", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("name", StringType(), True)
])

# bggreviewsDF = spark.read.csv("dataset/bgg-19m-reviews.csv", schema=bggreviewsSchema, header=True)

# bggreviewsDF.show(10)
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



# getting unique usernames
# unique_users = bggreviewsDF.select("user").distinct()
# unique_games = bggreviewsDF.select("ID").distinct()

# unique_users.show()
# print(unique_users.count())

# unique_games.show(20)
# print(unique_games.count())



bggreviewsDF = spark.read.csv("dataset/minidataset.csv", schema=bggreviewsSchema, header=True)


print(bggreviewsDF.count())
# getting unique usernames
# unique_users = bggreviewsDF.select("user").distinct()
# unique_users.show()

with_user_id = bggreviewsDF.withColumn("userid", monotonically_increasing_id())

with_user_id.show()
with_user_id.write.csv("dataset/complete")
trimmed_df = with_user_id.select("userid", "ID", "rating")

trimmed_df.show()

trimmed_df.write.csv("dataset/trimmed_mini_dataset2")


end_time = time.time()
print("execution time in seconds: ", end_time - start_time)
