Step 1 make sure "dataset/bgg-19m-reviews.csv" exists
Step 2 make sure directory "hdfs_datastore/" exists
Step 3 run "create_clean_input_file.py" to create cleaned/indexed version of our raw dataset "hdfs_datastore/csv_bgReviews"
Step 4 (optional) run "regression_evaluator.py" to create an evaluation model.  RMSE value will be output in the terminal
Step 5 run "create_model.py" to create and save our model at "hdfs_datastore/model_bgPredictions".  This model is created using the whole data set

Step 6: run "create_games_file.py" to create a cleaned/deduped games list (unique gameID with gameName, cnt_reviews, avg_rating)

(new) Step 7: use class "UserPredictions" in file "sinigle_user_predictor.py'to create a predition list for a given user (by userId)
	by default, prediction only for games not rated by user, and only on games with >= 200 reviews 
	(keep in mind the userid in your database and mine wont match because they are just made when we load the raw data into hdfs)
	
	
(next) Im going to create a user lookup dataset and user lookup function