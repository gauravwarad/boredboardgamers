from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import hour, col
from pyspark.ml.feature import OneHotEncoder, StringIndexer

import time
start_time = time.time()

spark = SparkSession.builder.appName("gauravchaapp").getOrCreate()
# rowNum,user,rating,comment,ID,name,userID
bggreviewsSchema = StructType([
    StructField("rowNum", StringType(), True),
    StructField("user", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("comment", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("userId", IntegerType(), True)

])

#bggreviewsDF = spark.read.csv("dataset/bgg-15m-reviews.csv", schema=bggreviewsSchema, header=True)
bggreviewsDF = spark.read.csv("dataset/testdata3.csv", schema=bggreviewsSchema, header=True)
predictUser = spark.read.csv("dataset/testdata4.csv", schema=bggreviewsSchema, header=True)
# print("predictUser")
# predictUser.show()
# predictUser.show()
# print("bggreviewsDF")
# bggreviewsDF.show()

newDF = bggreviewsDF
# .limit(10)

# print("newDF")
# newDF.show()



newDF.na.drop(subset=["ID","name","rating"])

stringindexer = (StringIndexer()
                    .setInputCol("name")
                    .setOutputCol("userId"))
indexedDF = stringindexer.fit(newDF)
newDF = indexedDF.transform(newDF)
#df.orderBy(col('userName').asc()).limit(10).show()

#bggreviewsDF.groupBy("userID").count().orderBy(col('count').desc()).show(10)


# lines = spark.read.text("dataset/bgg-19m-reviews.csv").rdd
# parts = lines.map(lambda row: row.value.split("::"))
# ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
#                                     rating=float(p[2]), timestamp=int(p[3])))
# ratings = spark.createDataFrame(ratingsRDD)
(training, test) = newDF.randomSplit([0.8, 0.2])
training.show(), test.show()

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
#als = ALS(maxIter=5, regParam=0.01, userCol="userNumber", itemCol="gameId", ratingCol="rating",
#          coldStartStrategy="drop")
als = ALS(userCol="userId", itemCol="ID", ratingCol="rating")
model = als.fit(training)


#
# # Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                 predictionCol="prediction")

# predictions.show(20)
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# predictions = model.transform(predictUser)
# predictions.show(30)
#
# # Generate top 10 movie recommendations for each user
# userRecs = model.recommendForAllUsers(10)
# # Generate top 10 user recommendations for each movie
# movieRecs = model.recommendForAllItems(10)
#
# # Generate top 10 movie recommendations for a specified set of users
# users = bggreviewsDF.select(als.getUserCol()).distinct().limit(3)
# userSubsetRecs = model.recommendForUserSubset(users, 10)
# # Generate top 10 user recommendations for a specified set of movies
# movies = bggreviewsDF.select(als.getItemCol()).distinct().limit(3)
# movieSubSetRecs = model.recommendForItemSubset(movies, 10)


model_path = "C:\\Users\\gauravw\\Documents\\projects\\531_project\\brandon\\demo2_singleuser_recs\\models"

# model.save(model_path)
model.write().overwrite().save(model_path)

end_time = time.time()
print("execution time in seconds: ", end_time - start_time)
