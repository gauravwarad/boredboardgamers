from flask import Flask, render_template

from user_predictions import UserPredictions

# from ui_data_scripts import get_users_list, recommended_games

# Create a Flask app
app = Flask(__name__)


# Define a route to serve the index.html file
@app.route('/')
def index():
    # Example parameters
    # users = [{"userid": 1, "username": "gaurav"},
    #          {"userid": 2, "username": "brandon"},
    #          {"userid": 3, "username": "sonal"}]
    # users = get_users_list()
    # Pass parameters to the index.html template
    # return render_template('index.html', users=users)
    pass


@app.route('/games')
def get_games():
    return "hello banana"


@app.route('/recommended_games')
def get_recommended_games():

    user = UserPredictions()
    user.userId = 66439
    user.userName = "ioneskylab"
    json_op = ""
    if user.generate_predictions():
        return render_template('recommended_games.html', json_data=user.predictions)
    #     return user.predictions
        # for prediction in user.predictions[:20]:
        #     json_op = json_op + str(prediction.toJSON())
            # return(prediction)
    # return json_op
    return "oops, what did you do?"

    # userid in request

    # fetch all rated games by the userid from database
    # create a dataframe with it

    # recommendedgames = recommended_games("user_game_rating_dataframe")


# Run the app if this file is executed directly
if __name__ == '__main__':
    app.run(debug=True)
