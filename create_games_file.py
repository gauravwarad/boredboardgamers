from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count, avg, desc, row_number
from pyspark.sql.window import Window

import findspark
findspark.init()

DEBUG = True

REVIEWS_FILE = "hdfs_datastore/csv_bgReviews"
GAMES_FILE = "hdfs_datastore/csv_games"

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
        print("loading reviews file...")
        df_reviews = load_csv_into_df(REVIEWS_FILE, REVIEWS_SCHEMA, spark)

        # group by game
        print("grouping by gameId/gameName...")
        df_grouped1 = (df_reviews
                        .groupBy("gameId", "gameName")
                        .agg(count("*").alias("cnt_reviews"), avg("rating").alias("avg_rating"))
                        .filter("gameId > 0")
                        )

        # remove duplicates
        print("using windowing functions to partition/rank, to remove dups...")
        window_spec = Window.partitionBy("gameId").orderBy(desc("cnt_reviews"))
        df_grouped1_dup_row_nums = df_grouped1.withColumn("row_num", row_number().over(window_spec))
        df_final = df_grouped1_dup_row_nums.filter("row_num = 1").select("gameId", "gameName", "cnt_reviews", "avg_rating")

        # get final row count
        final_count = df_final.count()

        # write final games file
        print(f"Writing file: {GAMES_FILE} ({final_count} rows)")
        df_final.write \
             .option("header", "true") \
             .csv(GAMES_FILE)

        if DEBUG:
            # check the write by reading in the new file and counting/showing the rows
            df_read_check = spark.read.option("header", "true").csv(GAMES_FILE)
            df_read_check_count = df_read_check.count()
            print(f"Read in file {GAMES_FILE} ({df_read_check_count} rows)")
            df_read_check.show(20)

    finally:
        # even if code fails, stop spark session
        print("stop spark")
        spark.stop()


# Code to prevent accidental execution
if __name__ == "__main__":
    main()
