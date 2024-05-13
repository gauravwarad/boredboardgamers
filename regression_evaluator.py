from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import findspark
findspark.init()

DEBUG = True
RANK_OVERRIDE= 10  # set the ALS model rank param (use 10 as Default)
REGPARAM_OVERRIDE = 0.1  # set the ALS model regParam (use 0.1 as Default)


REVIEWS_FILE = "hdfs_datastore/csv_bgReviews"
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
        df_count = df_reviews.count()
        print(f"loaded file: {REVIEWS_FILE} ({df_count} rows)")

        if DEBUG:
            print("df_reviews:")
            df_reviews.show(20)


        # TRAINING
        # create training/test DFs
        print("split reviews into TRAINING and TEST sets (80/20) ...")
        (training, test) = df_reviews.randomSplit([0.8, 0.2])

        # configure our ALS params
        als = ALS(rank=RANK_OVERRIDE,regParam=REGPARAM_OVERRIDE, userCol="userId", itemCol="gameId", ratingCol="rating", coldStartStrategy="drop")

        # create ALS model using training DF
        model = als.fit(training)

        # feed our "test" DF to the model to generate a prediction DF
        predictions = model.transform(test)

        # configure regression evaluator to score our prediction accuracy (rating vs prediction) using RMSE
        # save RSME (root mean square error) of our test predictions
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                         predictionCol="prediction")
        rmse = evaluator.evaluate(predictions) # get RSME over the entire test set
        print("Root-mean-square error (average prediction error)= " + str(rmse))

    finally:
        # either way stop spark
        print("spark stop")
        spark.stop()


if __name__ == "__main__":
    main()
