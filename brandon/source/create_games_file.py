from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col, count, sum, avg, desc, row_number
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.window import Window

INPUT_FILE = "hdfs_datastore/csv_bgReviews"
OUTPUT_FILE = "hdfs_datastore/csv_games"

import findspark
findspark.init()

# If not, create a SparkSession
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "3g") \
    .appName("CSV Read and Write") \
    .getOrCreate()

bggReviewsSchema = StructType([
    StructField("userName", StringType(), True),
    StructField("gameId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("gameName", StringType(), True),
    StructField("userId", IntegerType(), True),


])

#df = spark.read.option("header", "true").csv(INPUT_FILE)
df = spark.read.csv(INPUT_FILE, schema=bggReviewsSchema, header=True)

df_grouped1 = (df
                .groupBy("gameId", "gameName")
                .agg(count("*").alias("cnt_reviews"), avg("rating").alias("avg_rating"))
                .filter("gameId > 0")
                )
print(df_grouped1.count())

window_spec = Window.partitionBy("gameId").orderBy(desc("cnt_reviews"))
df_grouped1_dup_row_nums = df_grouped1.withColumn("row_num", row_number().over(window_spec))
df_final = df_grouped1_dup_row_nums.filter("row_num = 1").select("gameId", "gameName", "cnt_reviews", "avg_rating")

final_count = df_final.count()

print(final_count)

print(f"Writing file: {OUTPUT_FILE} ({df_final} rows)")
df_final.write \
     .option("header", "true") \
     .csv(OUTPUT_FILE)

df_read_check = spark.read.option("header", "true").csv(OUTPUT_FILE)

df_read_check_count = df_read_check.count()
print(f"Read in file {OUTPUT_FILE} ({df_read_check_count} rows)")
df_read_check.show(20)




spark.stop()
