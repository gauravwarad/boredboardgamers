from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator

import findspark
findspark.init()

INPUT_FILE = "hdfs_datastore/csv_bgReviews"


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

#df = spark.read.option("header", "true").csv(INPUT_FILE)
df = spark.read.csv(INPUT_FILE, schema=bggreviewsSchema, header=True)
df_count = df.count()
print(f"loaded file: {INPUT_FILE} ({df_count} rows)")
print("sample:")
#df.show(20)


#TRAINING
print("split reviews into TRAINING and TEST sets (80/20) ...")
(training, test) = df.randomSplit([0.8, 0.2])

als = ALS(rank=30,regParam=0.01, userCol="userId", itemCol="gameId", ratingCol="rating", coldStartStrategy="drop")
#model = als.fit(training)

param_grid = ParamGridBuilder() \
    .addGrid(als.rank, [10, 20]) \
    .addGrid(als.regParam, [0.01, 0.1]) \
    .build()



#predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                 predictionCol="prediction")

crossval = CrossValidator(estimator=als,
                          estimatorParamMaps=param_grid,
                          evaluator=evaluator,
                          numFolds=3)

cv_model = crossval.fit(training)
best_model = cv_model.bestModel
predictions = best_model.transform(test)
rmse = evaluator.evaluate(predictions)



#predictions.show(20)
#rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

chosen_rank = best_model.rank
chosen_reg_param = best_model._java_obj.parent().getRegParam()

print(f"Chosen rank: {chosen_rank}")
print(f"Chosen regParam: {chosen_reg_param}")


spark.stop()