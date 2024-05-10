from flask import jsonify
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *


spark = SparkSession.builder.appName("gauravchaapp").getOrCreate()


bggreviewsSchema = StructType([
    StructField("rowNum", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("comment", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("userId", IntegerType(), True)
])

bggreviewsDF = spark.read.csv("dataset/bgg-19m-reviews.csv", schema=bggreviewsSchema, header=True)

# getting unique usernames
unique_users = bggreviewsDF.select("user").distinct()


def get_users_list():
    bggreviewsDF = spark.read.csv("dataset/testdata3.csv", schema=bggreviewsSchema, header=True)

    # getting unique usernames
    unique_users_df = bggreviewsDF.select("userId", "user").distinct()

    # Collect the unique users as a list of dictionaries
    users_list = [{"userid": row.userId, "username": row.user} for row in unique_users_df.collect()]

    return users_list

def recommended_games(user_input):
    model_path = "C:\\Users\\gauravw\\Documents\\projects\\531_project\\models"

    als_model = ALSModel.load(model_path)

    # user_input = request.json
    # user_input
    # Preprocess user input (e.g., convert to DataFrame)
    # user_df = spark.createDataFrame([user_input])

    # fetch the user's previous reviews --
    predictUser = spark.read.csv("dataset/testdata4.csv", schema=bggreviewsSchema, header=True)

    # Use ALS model to make recommendations for the new user
    recommendations = als_model.transform(predictUser)
    recommendations.show()
    # Extract recommended games from the predictions
    # recommended_games = recommendations.select("recommendations.ID").collect()[0][0]
    recommendations_json = recommendations.toJSON().collect()

    # return jsonify({"recommended_games": recommended_games})
    return recommendations_json