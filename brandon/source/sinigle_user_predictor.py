from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col, count, sum, avg, desc, row_number, lit, coalesce
import json

import findspark
findspark.init()

SHOW = False

REVIEWS_FILE = "hdfs_datastore/csv_bgReviews"
GAMES_FILE = "hdfs_datastore/csv_games"
MODEL_PATH = "hdfs_datastore/model_bgPredictions"

GAMES_SCHEMA = StructType([
            StructField("gameId", IntegerType(), False),
            StructField("gameName", StringType(), True),
            StructField("cnt_reviews", IntegerType(), True),
            StructField("avg_rating", FloatType(), True),
        ])

REVIEWS_SCHEMA = StructType([
            StructField("userName", StringType(), True),
            StructField("gameId", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("gameName", StringType(), True),
            StructField("userId", IntegerType(), True),
        ])



class UserPredictions:
    def __init__(self):
        self.userId = 66439
        self.userName = "ioneskylab"
        self.min_num_ratings = 200
        self.exclude_rated_games = False
        self.predictions = []

    def load_csv_into_df(self, path, schema, spark):
        """LOAD GAMES"""
        df = spark.read.csv(path, schema=schema, header=True)
        return df


    def generate_predictions(self, ):

        success_falg = False

        # If not, create a SparkSession
        spark = SparkSession.builder \
            .master('local[*]') \
            .config("spark.driver.memory", "3g") \
            .appName("CSV Read and Write") \
            .getOrCreate()

        try:
            print("loading games file ...")
            df_games = self.load_csv_into_df(GAMES_FILE, GAMES_SCHEMA, spark)
            df_games = df_games.filter(col("cnt_reviews") >= self.min_num_ratings)

            if SHOW:
                print("df_games:")
                df_games.show(10)


            print("loading reviews file ...")
            df_reviews = self.load_csv_into_df(REVIEWS_FILE, REVIEWS_SCHEMA, spark)
            df_reviews = df_reviews.filter(col("userID") == self.userId)

            if SHOW:
                print("df_reviews:")
                df_reviews.show(10)

            #join the two datasets
            print("joining games and reviews ...")
            df_joined = df_games.alias('g').join(df_reviews.alias('r'), col('g.gameId') == col('r.gameId'), 'left') \


            print("selecting columns for final df ...")
            # Select all columns from 'games' and 'rating' from 'reviews', replacing null with 0
            df_predict_user = (df_joined.select('g.*'
                                                , lit(self.userId).alias('userId')
                                                , lit(self.userName).alias('userName')
                                                , coalesce('r.rating', lit(-1)).alias('rating'))

                               )

            if self.exclude_rated_games:
                df_predict_user = df_predict_user.filter(col("rating") == -1)

            if SHOW:
                print("df_predict_user:")
                df_predict_user.show(10)

            # Load the ALS model
            print("loading ALS model ...")
            als_model = ALSModel.load(MODEL_PATH)

            # Generate predictions
            print("generating predictions ...")
            df_predictions = als_model.transform(df_predict_user)
            df_predictions = df_predictions.orderBy(desc("prediction"), "rating")

            if SHOW:
                print("df_predictions:")
                df_predictions.show(10)

            # Extract predictions
            print("collect predictions in to list")
            self.predictions = df_predictions.collect()

            success_falg = True

        finally:
            # Stop the SparkSession
            print("stop spark")
            spark.stop()

        return success_falg

    def get_predictions_JSON(self):
        return json.dumps(self.predictions)


# Example usage
if __name__ == "__main__":


    user = UserPredictions()
    user.userId = 66439
    user.userName = "ioneskylab" #(optional) just includes it in the output dataset (this is me BTW)

    if user.generate_predictions():
        for prediction in user.predictions[:20]:
            print(prediction)


