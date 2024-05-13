# Setup

1. Make sure all required libraries are installed:
    - `pip install pyspark`
    - `pip install findspark`
    - `pip install flask`
2. Download the file [bgg-19m-reviews.csv](https://www.kaggle.com/datasets/jvanelteren/boardgamegeek-reviews/data?select=games_detailed_info.csv) from Kaggle. Place it in a directory called `dataset/`.
3. Create directory `hdfs_datastore/`.
4. Run `data_cleaning.py`.
5. Run `create_games_file.py`.
6. Run `create_users_file.py`.
7. *(Optional)* Run `regression_evaluator.py` to view RMSE.
8. *(Optional)* Run `crossvalidation_evaluator.py` to determine the best Rank/RegParam values.
9. Run `create_model.py` (preferably using the values from the crossvalidation_evaluator).
10. *(Optional)* Run `user_predictions.py` directly (test code at the bottom of the file lets you run predictions for a single user by username).

# UI Deployment

1. After performing all setup steps, run the Flask server by executing `python routes.py` in the terminal.
2. Navigate to [http://127.0.0.1:5000/](http://127.0.0.1:5000/) in a browser window to access the webpage.

# UI Usage

1. The index page by default loads the `/users` API. Pagination can also be performed by giving browser query parameters of 'page' and 'per_page'.
2. After selecting the user on the users page, click on submit to view the recommended games.
