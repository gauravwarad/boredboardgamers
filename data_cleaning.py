from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer

import findspark
findspark.init()

INPUT_FILE = "dataset/bgg-19m-reviews.csv"
REVIEWS_FILE = "hdfs_datastore/csv_bgReviews"

DEBUG = True


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

        # Read the CSV file into a DataFrame
        print("loading input file...")
        df = spark.read.option("header", "true").csv(INPUT_FILE)

        # count rows
        df_count = df.count()
        print(f"Input file rows count:{df_count}")

        # CLEAN input data,
        print("cleaning input data...")
        df = df.filter((col("ID").cast("int").isNotNull())  # game ids should convert to int
                        & ~col("ID").rlike("\\D")  # remove any with decimal points
                        & (col("rating").cast("float").isNotNull())  # ratings should convert to float
                        & (col("rating").cast("float") >= 0)  # ratings should be between 0 and 10
                        & (col("rating").cast("float") <= 10)  # ratings should be between 0 and 10
                       )

        # get new row count
        df_f1_count = df.count()
        print(f"filtered non-numeric and out of range from [gameId,userId,rating] ({df_count - df_f1_count} rows filtered)")

        # remove unused columns to improve I/O efficiency
        df = df.select(col("user").alias("userName"), col("ID").alias("gameId"), "rating", col("name").alias("gameName") )

        if DEBUG:
            df.show(20)

        # ADD UserId to our data set using StringIndexer class
        print("creating an userId index (needed for ALS)")
        stringindexer = (StringIndexer()
                            .setInputCol("userName")
                            .setOutputCol("userId"))
        indexedDF = stringindexer.fit(df)
        df = indexedDF.transform(df)
        df = df.withColumn("userId", col("userId").cast("int"))

        if DEBUG:
            df.show(20)

        print(f"Final record count: {df.count()}")

        # write to Pyspark file system
        df.write \
             .option("header", "true") \
             .csv(REVIEWS_FILE)

        # if DEBUG true, read back in the file to check for success
        if DEBUG:
            _df2 = spark.read.option("header", "true").csv(REVIEWS_FILE)

            _df2_count = _df2.count()
            print(f"_df2_count:{_df2_count}")

    finally:
        #either way stop the spark session
        print("stop spark")
        spark.stop()


# Code to prevent accidental execution
if __name__ == "__main__":
    main()

