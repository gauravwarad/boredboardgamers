from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count,avg

import findspark
findspark.init()

DEBUG = True

REVIEWS_FILE = "hdfs_datastore/csv_bgReviews"
USERS_FILE = "hdfs_datastore/csv_users"

REVIEWS_SCHEMA = StructType([
            StructField("userName", StringType(), True),
            StructField("gameId", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("gameName", StringType(), True),
            StructField("userId", IntegerType(), True),
        ])

USERS_SCHEMA = StructType([
            StructField("userId", IntegerType(), True),
            StructField("userName", StringType(), True),
            StructField("cnt_reviews", IntegerType(), True),
            StructField("avg_rating", FloatType(), True),

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
        #load reviews file
        print("loading reviews file...")
        df_reviews = load_csv_into_df(REVIEWS_FILE, REVIEWS_SCHEMA, spark)

        if DEBUG:
            print("df_reviews:")
            df_reviews.show(10)

        # group by user
        print("grouping by userId/userName...")
        df_final = (df_reviews
                        .groupBy("userId", "userName")
                        .agg(count("*").alias("cnt_reviews"), avg("rating").alias("avg_rating"))
                        )

        # get final row count
        final_count = df_final.count()

        # write final games file
        print(f"Writing file: {USERS_FILE} ({final_count} rows)")
        df_final.write \
             .option("header", "true") \
             .csv(USERS_FILE)

        if DEBUG:
            # check the write by reading in the new file and counting/showing the rows
            df_read_check = load_csv_into_df(USERS_FILE,USERS_SCHEMA,spark)
            df_read_check_count = df_read_check.count()
            print(f"Read in file {USERS_FILE} ({df_read_check_count} rows)")
            df_read_check.show(20)

    finally:
        # either way stop spark
        print("spark stop")
        spark.stop()


if __name__ == "__main__":
    main()
