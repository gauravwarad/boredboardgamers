import json

from flask import Flask, render_template, request

from user_predictions import UserPredictions, get_users, search_users

# from ui_data_scripts import get_users_list, recommended_games

# Create a Flask app
app = Flask(__name__)


# Define a route to serve the index.html file
@app.route('/', methods=['GET'])
@app.route('/users', methods=['GET'])
def index(per_page=10, page=1):

    # Example parameters
    # users = [{"userid": 1, "username": "gaurav"},
    #          {"userid": 2, "username": "brandon"}]
    # users = get_users_list()
    # Pass parameters to the index.html template
    args = request.args
    print("users api called with arguments - ", args)
    per_page = args.get("per_page", default=10)
    page = args.get("page", default=1)
    search_key = args.get("search_key", default=None)

    users = []
    if search_key:
        users = search_users(search_key)
    
    # per_page = 50
    # page = 3
    else:
        users = get_users(int(per_page), int(page))
    # print(users)
    # users = [
    #     "{\"userId\":\"100\",\"userName\":\"jgoyes\"}",
    #     "{\"userId\":\"101\",\"userName\":\"rainydaysteve\"}",
    #     "{\"userId\":\"102\",\"userName\":\"Sheylon\"}",
    #     "{\"userId\":\"103\",\"userName\":\"The Eraser\"}",
    #     "{\"userId\":\"104\",\"userName\":\"asterix50\"}",
    #     "{\"userId\":\"105\",\"userName\":\"UFo11\"}",
    #     "{\"userId\":\"106\",\"userName\":\"william4192\"}",
    #     "{\"userId\":\"107\",\"userName\":\"Dharquen\"}",
    #     "{\"userId\":\"108\",\"userName\":\"musicalanarchy\"}"]
    # Parse each string in users list and convert to JSON
    json_users = [json.loads(user) for user in users]

    return render_template('index.html', users=json_users)


@app.route('/games')
def get_games():
    return "hello banana"


@app.route('/recommended_games', methods=['GET'])
def get_recommended_games():
    args = request.args

    print("recommended_games api called with arguments - ", args)

    user = UserPredictions()
    user.userId = int(args.get("userid", default=66439))
    user.userName = args.get("username", default="ioneskylab")
    json_op = ""
    if user.generate_predictions():
        return render_template('recommended_games.html', json_data=user.predictions,
                               user=user.userName)
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
