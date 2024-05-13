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
USERS_FILE = "hdfs_datastore/csv_users/*.csv"

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

USERS_SCHEMA = StructType([
            StructField("userId", IntegerType(), True),
            StructField("userName", StringType(), True),
            StructField("cnt_reviews", IntegerType(), True),
            StructField("avg_rating", FloatType(), True),

        ])



def get_users(per_page, page):
    """GET_USERS"""

    # create spark session
    print("creating spark session ... ")
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "3g") \
        .appName("CSV Read and Write") \
        .getOrCreate()

    # read in users file
    print("reading users files .. ")
    df_users = spark.read.csv(USERS_FILE, header=True)

    # find starting record for next page
    skip = per_page * (page - 1)

    # only pull records for the "next" page
    df_users = df_users.where(col('userId').between(skip, skip + per_page))

    if SHOW:
        print("df_users:")
        df_users.show()

    return df_users.toJSON().collect()


def search_users(search_key):
    """SEARCH_USERS"""

    # create spark session
    print("creating spark session ... ")
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "3g") \
        .appName("searching users") \
        .getOrCreate()

    # read in users file
    print("reading users files .. ")
    df_users = spark.read.csv(USERS_FILE, header=True)

    # filter on single userName
    results = df_users.filter(df_users['userName'] == search_key)

    if SHOW:
        print("results:")
        results.show()

    return results.toJSON().collect()


def load_csv_into_df(path, schema, spark):
    """LOAD GAMES"""
    df = spark.read.csv(path, schema=schema, header=True)
    return df

def start_spark_session():
    """START_SPARK_SESSION"""
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "3g") \
        .appName("single_user_predictor") \
        .getOrCreate()
    return spark


def lookup_userid_byusername(userName):
    '''LOOKUP_USERID_BYUSERNAME'''
    print("creating spark session: ")
    spark = start_spark_session()

    try:
        print("loading users file ...")
        df_users = load_csv_into_df(USERS_FILE, USERS_SCHEMA, spark)

        print(f"finding user {userName}...")
        first_row = df_users.filter(col("userName") == userName).first()
        userId = first_row['userId']

    finally:
        print("spark stop")
        spark.stop()

    return userId

class UserPredictions:
    def __init__(self):
        self.userId = 66439
        self.userName = "ioneskylab"
        self.min_num_ratings = 200
        self.max_predictions = 20
        self.exclude_rated_games = False
        self.predictions = []
        self.predictions_list = []


    def generate_predictions(self, ):
        """GENERATE PREDICTIONS"""
        success_flag = False

        # If not, create a SparkSession
        spark = SparkSession.builder \
            .master('local[*]') \
            .config("spark.driver.memory", "3g") \
            .appName("CSV Read and Write") \
            .getOrCreate()

        try:
            #load in games file
            print("loading games file ...")
            df_games = load_csv_into_df(GAMES_FILE, GAMES_SCHEMA, spark)

            # filter out games with too few reviews (eliminates obscure/new/unreliable games)
            df_games = df_games.filter(col("cnt_reviews") >= self.min_num_ratings)

            if SHOW:
                print("df_games:")
                df_games.show(10)

            # load reviews file
            print("loading reviews file ...")
            df_reviews = load_csv_into_df(REVIEWS_FILE, REVIEWS_SCHEMA, spark)

            # filter down to just our single user
            df_reviews = df_reviews.filter(col("userID") == self.userId)

            if SHOW:
                print("df_reviews:")
                df_reviews.show(10)

            # join the two datasets on gameId
            print("joining games and reviews ...")
            df_joined = df_games.alias('g').join(df_reviews.alias('r'), col('g.gameId') == col('r.gameId'), 'left') \


            # Add in our UserId column (the predictor needs it) and substitute -1 for missing ratings (non-rated games)
            print("selecting columns for final df ...")
            df_predict_user = (df_joined.select('g.*'
                                                , lit(self.userId).alias('userId')
                                                , lit(self.userName).alias('userName')
                                                , coalesce('r.rating', lit(-1)).alias('rating'))

                               )

            # remove games that have already been rated (default)
            if self.exclude_rated_games:
                df_predict_user = df_predict_user.filter(col("rating") == -1)

            if SHOW:
                print("df_predict_user:")
                df_predict_user.show(10)

            # Load the ALS model
            print("loading ALS model ...")
            als_model = ALSModel.load(MODEL_PATH)

            # Generate predictions. Order by prediction DESC (best predictions first)
            print("generating predictions ...")
            df_predictions = als_model.transform(df_predict_user)
            df_predictions = df_predictions.orderBy(desc("prediction"), "rating")

            if SHOW:
                print("df_predictions:")
                df_predictions.show(10)

            # Save prediction as list (for debug)
            self.predictions_list = df_predictions.collect()

            # Extract predictions (default top 20)
            print("collect predictions in to list")
            top_X_rows = df_predictions.limit(self.max_predictions)

            # Save predictions as JSON (for UI)
            self.predictions = top_X_rows.toJSON().collect()

            success_flag = True

        finally:
            # Stop the SparkSession
            print("stop spark")
            spark.stop()

        return success_flag

    def get_predictions_JSON(self):
        return json.dumps(self.predictions)


# Example usage
if __name__ == "__main__":

    userName = "ioneskylab"
    userId = lookup_userid_byusername(userName)

    user = UserPredictions()
    user.userId = userId
    user.userName = userName

    if user.generate_predictions():
        for prediction in user.predictions[:20]:
            print(prediction)

    # foo = get_users(10, 1)
    #foo = search_users(userName)
    #print(foo)

