
To run:
    1. Make sure all required libraries are installed
        install pyspark
        install findspark
    2. Download file “bgg-19m-reviews.csv”  from Kaggle (https://www.kaggle.com/datasets/jvanelteren/boardgamegeek-reviews/data?select=games_detailed_info.csv). Place it in a directory called “dataset/”
    3. Create directory “hdfs_datastore/”
    2. run data_cleaning.py
    3. run create_games_file.py
    4. run create_users_file.py
    5. (optional) run regression_evaluator.py to view RMSE
    6. (optional) run crossvalidation_evaluator.py to determine best Rank/RegParam values
    7. run create_model.py (preferably using the values from the crossvalidation_evaluator)
    
