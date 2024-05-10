Step 1 make sure "dataset/bgg-19m-reviews.csv" exists
Step 2 make sure directory "hdfs_datastore/" exists
Step 3 run "create_clean_input_file.py" to create cleaned/indexed version of our raw dataset "hdfs_datastore/csv_bgReviews"
Step 4 (optional) run "regression_evaluator.py" to create an evaluation model.  RMSE value will be output in the terminal
Step 5 run "create_model.py" to create and save our model at "hdfs_datastore/model_bgPredictions".  This model is created using the whole data set

(new) Step 6: run "create_games_file.py" to create a cleaned/deduped games list (unique gameID with gameName, cnt_reviews, avg_rating)

Im working on creating a single user selector with missing games should have that in a bit