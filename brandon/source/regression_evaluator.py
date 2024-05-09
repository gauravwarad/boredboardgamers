from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col, count, sum
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.window import Window

import findspark
findspark.init()

INPUT_FILE = "hdfs_datastore/csv_bggReviews"

spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "3g") \
    .appName("bgrecommender") \
    .getOrCreate()


bggreviewsSchema = StructType([
    StructField("userName", StringType(), True),
    StructField("gameId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("gameName", StringType(), True),
    StructField("userId", IntegerType(), True),


])

#df = spark.read.option("header", "true").csv(INPUT_FILE)
df = spark.read.csv(INPUT_FILE, schema=bggreviewsSchema, header=True)
df_count = df.count()
print(f"loaded file: {INPUT_FILE} ({df_count} rows)")
print("sample:")
df.show(20)

# null_count = df.filter(col("userID").isNull()).count()
# null_count2 = df.filter(col("gameId").isNull()).count()
# null_count3 = df.filter(col("rating").isNull()).count()

# print(f"nc1: {null_count}, nc2: {null_count2}, nc3: {null_count3})")


# df = df.filter((col("gameId").cast("int").isNotNull()) & (col("userID").cast("int").isNotNull()) & (col("rating").cast("float").isNotNull()))
# df_f1_count = df.count()
# print(f"filtered non-integers from [gameId] column ({df_count - df_f1_count} rows filtered)")
#
# df = df.filter((col("rating").cast("int") >= 0) & (col("rating").cast("int") <= 10))
# df_f2_count = df.count()
# print(f"filtered out of range scores from [ratings] column ({df_f1_count - df_f2_count} rows filtered)")



#TRAINING
(training, test) = df.randomSplit([0.8, 0.2])

als = ALS(userCol="userId", itemCol="gameId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                 predictionCol="prediction")

predictions.show(20)
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))





# total_ratings = df.filter( count()
#
# print(f"total_ratings: {total_ratings}")

# df_grouped = (df
#                 .groupBy("user")
#                 .agg(count("*").alias("num_ratings")
#                     , sum("rating").alias("sum_ratings"))
#                 .orderBy("num_ratings", ascending=False)
#               )
#
# print("df_grouped")
# df_grouped.show(20)
#
# windowSpec = Window.orderBy(col("num_ratings").asc())
# windowSpec2 = Window.partitionBy().orderBy()
# df_windowed = (df_grouped
#                .withColumn("running_sum", sum("num_ratings").over(windowSpec))
#                 .withColumn("running_perc" , col("running_sum") * 100.0 / total_ratings)
#                 .select("user", "num_ratings", "sum_ratings", "running_sum", "running_perc")
#                 .orderBy("num_ratings",ascending=False)
#                )
#
# print("df_windowed")
# df_windowed.show(20)
#
# df_grouped2 = (df_grouped
#                 .groupBy("num_ratings")
#                 .agg(count("*").alias("user_cnt")
#                     , sum("num_ratings").alias("total_ratings"))
#                .orderBy("user_cnt", ascending=False)
#               )
#
# df_grouped2.show()
#
# #

spark.stop()