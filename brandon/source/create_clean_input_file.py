from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col, count, sum
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.window import Window

INPUT_FILE = "dataset/bgg-19m-reviews.csv"
OUTPUT_FILE = "hdfs_datastore/csv_bgReviews"

import findspark
findspark.init()

# If not, create a SparkSession
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "3g") \
    .appName("CSV Read and Write") \
    .getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.option("header", "true").csv(INPUT_FILE)

df_count = df.count()
print(f"_df_count:{df_count}")

# Perform the required transformation
#df_transformed = df.withColumn("age_plus_10", col("age").cast("int") + 10)

# Select only the required columns for the output

df = df.filter( (col("ID").cast("int").isNotNull()) #game ids should convert to int
                & ~col("ID").rlike("\\D") #remove any with decimal points
                & (col("rating").cast("float").isNotNull()) #ratings should convert to float
                & (col("rating").cast("float") >= 0) #ratings should be between 0 and 10
                & (col("rating").cast("float") <= 10) #ratings should be between 0 and 10
               )

df_f1_count = df.count()
print(f"filtered non-numeric and out of range from [gameId,userId,rating] ({df_count - df_f1_count} rows filtered)")


df = df.select(col("user").alias("userName"), col("ID").alias("gameId"), "rating", col("name").alias("gameName") )
df.show(20)

stringindexer = (StringIndexer()
                    .setInputCol("userName")
                    .setOutputCol("userId"))
indexedDF = stringindexer.fit(df)
df = indexedDF.transform(df)
df = df.withColumn("userId", col("userId").cast("int"))
df.show(20)
print(f"Final record count: {df.count()}")

# df_nulls = df.filter(df.gameId.isNull())
# print(f"null gameId records: {df_nulls.count()}")

# df = df.filter((col("userName") == "Nekura"))
# df.show(100)





# Write the DataFrame to a single CSV file
#df_single_partition = df.repartition(1)
df.write \
     .option("header", "true") \
     .csv(OUTPUT_FILE)

_df2 = spark.read.option("header", "true").csv(OUTPUT_FILE)

_df2_count = _df2.count()
print(f"_df2_count:{_df2_count}")

spark.stop()

