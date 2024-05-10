from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col, count, sum, avg, desc, row_number
from pyspark.sql.window import Window
import findspark

findspark.init()

INPUT_FILE = "hdfs_datastore/csv_bgReviews"
OUTPUT_FILE = "hdfs_datastore/csv_users"

# If not, create a SparkSession
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "3g") \
    .appName("getting users") \
    .getOrCreate()

bggReviewsSchema = StructType([
    StructField("userName", StringType(), True),
    StructField("gameId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("gameName", StringType(), True),
    StructField("userId", IntegerType(), True)
])

# df = spark.read.option("header", "true").csv(INPUT_FILE)
df = spark.read.csv(INPUT_FILE, schema=bggReviewsSchema, header=True)

# getting number of ratings by the user
user_ratings_count = df.groupBy("userId").agg(count("*").alias("num_ratings"))

# selecting userid and names for users with atleast 5 ratings
df_grouped_users = (user_ratings_count
                    .filter("num_ratings >= 5")
                    .join(df.select("userId", "userName").distinct(), "userId")
                    .orderBy("userId"))


print("number of users are ", df_grouped_users.count())

# window_spec = Window.partitionBy("gameId").orderBy(desc("cnt_reviews"))
# df_grouped1_dup_row_nums = df_grouped1.withColumn("row_num", row_number().over(window_spec))
# df_final = df_grouped1_dup_row_nums.filter("row_num = 1").select("gameId", "gameName", "cnt_reviews", "avg_rating")

df_final = df_grouped_users.select("userId", "userName")

final_count = df_final.count()

# print(final_count)

print(f"Writing file: {OUTPUT_FILE} ({final_count} rows)")
df_final.write \
    .option("header", "true") \
    .csv(OUTPUT_FILE)

df_read_check = spark.read.option("header", "true").csv(OUTPUT_FILE)

df_read_check_count = df_read_check.count()
print(f"Read in file {OUTPUT_FILE} ({df_read_check_count} rows)")
df_read_check.show(20)

spark.stop()
