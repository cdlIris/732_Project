from flask import Flask, request, Blueprint, jsonify, render_template
from flask_bootstrap import Bootstrap
from historical import Bitcoin
import json
import ast
from flask_wtf import Form
from wtforms import StringField, SubmitField

main = Blueprint('main', __name__)
bootstrap = Bootstrap()

# render index page
@main.route("/")
def index():
    return render_template('index.html')

# render all close price data in hourly-level
@main.route("/history", methods=["GET"])
def get_full_data():
    label, value = bitcoin_model.get_full_data()
    return render_template('history.html', title='Bitcoin price in USD', labels=label, values=value)

# render average close price for each day of year
@main.route("/history/day", methods=["GET"])
def get_dayofyear_avg():
    label, value = bitcoin_model.get_dayofyear_avg()
    return render_template('history.html', title='Bitcoin price in USD', labels=label, values=value)

# render daily average close price
@main.route("/history/daily", methods=["GET"])
def get_daily():
    label, value = bitcoin_model.get_daily_avg()
    return render_template('history.html', title='Bitcoin price in USD', labels=label, values=value)

# render average close price for each month of year
@main.route("/history/monthly", methods=["GET"])
def get_monthly():
    label, value = bitcoin_model.get_monthly_avg()
    return render_template('history.html', title='Bitcoin price in USD', labels=label, values=value)

# render average close price for each hour of day
@main.route("/history/hourly", methods=['GET'])
def get_hourly():
    label, value = bitcoin_model.get_hourly_avg()
    return render_template('history.html', title='Bitcoin price in USD', labels=label, values=value)

# render all Bitcoin Volume data in hourly-level
@main.route("/history/volume", methods=["GET"])
def get_full_data_v():
    label, value = bitcoin_model.get_full_data_v()
    return render_template('history_v.html', title='Amount of Bitcoin in Transaction', labels=label, values=value, mode='vol')

# render average Bitcoin Volume for each day of year
@main.route("/history/day/volume", methods=["GET"])
def get_dayofyear_avg_v():
    label, value = bitcoin_model.get_dayofyear_avg_v()
    return render_template('history_v.html', title='Amount of Bitcoin in Transaction', labels=label, values=value, mode='vol')

# render daily average Bitcoin Volume
@main.route("/history/daily/volume", methods=["GET"])
def get_daily_v():
    label, value = bitcoin_model.get_daily_avg_v()
    return render_template('history_v.html', title='Amount of Bitcoin in Transaction', labels=label, values=value, mode='vol')

# render average Bitcoin Volume for each month of year
@main.route("/history/monthly/volume", methods=["GET"])
def get_monthly_v():
    label, value = bitcoin_model.get_monthly_avg_v()
    return render_template('history_v.html', title='Amount of Bitcoin in Transaction', labels=label, values=value, mode='vol')

# render average Bitcoin Volume for each hour of day
@main.route("/history/hourly/volume", methods=['GET'])
def get_hourly_v():
    label, value = bitcoin_model.get_hourly_avg_v()
    return render_template('history_v.html', title='Amount of Bitcoin in Transaction', labels=label, values=value, mode='vol')



labels = []
values = []
props = []
predictions = []
# render real-time prediction page
@main.route("/realtime")
def get_chart_page():
    global labels,values,props,predictions
    labels = []
    values = []
    props = []
    predictions = []
    return render_template('realtime.html', values=values, labels=labels, props=props, predictions=predictions)

# update price prediction graph with newly received data
@main.route('/realtime/refreshData')
def refresh_graph_data():
    global labels, values, predictions
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    print("pred now: " + str(predictions))
    return jsonify(sLabel=labels, sData=values, sPred=predictions)

# receive new price data from backend
@main.route('/realtime/updateData', methods=['POST'])
def update_data():
    global labels, values, predictions
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    predictions = ast.literal_eval(request.form['prediction'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    print("pred received: " + str(predictions))
    return "success", 201

# update decision graph with newly received data
@main.route('/realtime/refreshDecision')
def refresh_graph_decision():
    global props
    print("props now: " + str(props))
    return jsonify(sProp=props)

# receive new decision data from backend
@main.route('/realtime/updateDecision', methods=['POST'])
def update_decision():
    global props
    if not request.form or 'proportion' not in request.form:
        return "error", 400
    props = ast.literal_eval(request.form['proportion'])
    print("props received: " + str(props))
    return "success", 201


def create_app(spark):
    global bitcoin_model
    bitcoin_model = Bitcoin(spark)

    app = Flask(__name__)
    app.register_blueprint(main)
    bootstrap.init_app(app)
    return app