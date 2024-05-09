from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col, count, sum
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.window import Window

import findspark
findspark.init()

INPUT_FILE = "hdfs_datastore/csv_bgReviews"
MODEL_PATH = "hdfs_datastore/model_bgPredictions"

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


df = spark.read.csv(INPUT_FILE, schema=bggreviewsSchema, header=True)
df_count = df.count()
print(f"loaded file: {INPUT_FILE} ({df_count} rows)")
print("sample:")
df.show(20)

#Create Model using ALS (alternating least squares method)
als = ALS(userCol="userId", itemCol="gameId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(df)

#Save Model in hdfs for reuse
model.save(MODEL_PATH)

spark.stop()