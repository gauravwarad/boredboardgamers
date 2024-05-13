
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import findspark
findspark.init()

DEBUG = True

REVIEWS_FILE = "hdfs_datastore/csv_bgReviews"
MODEL_PATH = "hdfs_datastore/model_bgPredictions"

RANK_OVERRIDE= 10  # Enter the Rank value from the crossvalidator "best model"
REGPARAM_OVERRIDE = 0.1  # Enter the RegParam value from the crossvalidator "best model"

REVIEWS_SCHEMA = StructType([
    StructField("userName", StringType(), True),
    StructField("gameId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("gameName", StringType(), True),
    StructField("userId", IntegerType(), True),
])


def load_csv_into_df(path, schema, spark):
    """LOAD GAMES"""
    df = spark.read.csv(path, schema=schema, header=True)
    return df


def start_spark_session():
    """START SPARK SESSION"""
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "3g") \
        .appName("single_user_predictor") \
        .getOrCreate()
    return spark


def main():
    """MAIN"""
    # start spark session
    print("starting spark session...")
    spark = start_spark_session()

    try:
        # load full reviews file
        print("loading raw reviews file...")
        df_reviews = load_csv_into_df(REVIEWS_FILE, REVIEWS_SCHEMA, spark)

        if DEBUG:
            df_count = df_reviews.count()
            print(f"loaded file: {REVIEWS_FILE} ({df_count} rows)")
            print("df_reviews:")
            df_reviews.show(20)

        #Create Model using ALS (alternating least squares method)
        print("creating als model...")
        als = ALS(rank=RANK_OVERRIDE, regParam=REGPARAM_OVERRIDE, userCol="userId", itemCol="gameId", ratingCol="rating", coldStartStrategy="drop")
        model = als.fit(df_reviews)

        #Save Model in hdfs for reuse
        print("saving als model...")
        model.save(MODEL_PATH)

    finally:
        # either way stop spark
        print("spark stop")
        spark.stop()


if __name__ == "__main__":
    main()