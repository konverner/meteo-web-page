# meteo-web-page/src/meteo_web_page/main.py
from flask import Flask, jsonify, render_template
import threading
import time

from db.database import fetch_data
from api.data_update import update_database

app = Flask(__name__)

@app.route('/data')
def data():
    data = fetch_data()
    return jsonify(data)

@app.route('/')
def index():
    return render_template('index.html')

def update_database_periodically():
    while True:
        update_database()
        time.sleep(30)  # Sleep for 30 seconds

if __name__ == "__main__":
    update_thread = threading.Thread(target=update_database_periodically, daemon=True)
    update_thread.start()
    app.run(host='0.0.0.0', debug=True)
