from flask import Flask, render_template
from ui_data_scripts import get_users_list, recommended_games

# Create a Flask app
app = Flask(__name__)


# Define a route to serve the index.html file
@app.route('/')
def index():
    # Example parameters
    # users = [{"userid": 1, "username": "gaurav"},
    #          {"userid": 2, "username": "brandon"},
    #          {"userid": 3, "username": "sonal"}]
    users = get_users_list()
    # Pass parameters to the index.html template
    return render_template('index.html', users=users)


@app.route('/games')
def get_games():
    return "hello banana"

@app.route('/recommended_games')
def get_recommended_games():
    # userid in request

    # fetch all rated games by the userid from database
    # create a dataframe with it

    recommendedgames = recommended_games("user_game_rating_dataframe")

    return render_template('recommended_games.html', json_data=recommendedgames)


# Run the app if this file is executed directly
if __name__ == '__main__':
    app.run(debug=True)
