# meteo-web-page/src/meteo_web_page/main.py
import logging

from flask import Flask, jsonify, render_template
import threading
import time

from db.database import init_db, fetch_data, drop_table
from api.data_update import update_database

app = Flask(__name__)

# define logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.route('/data')
def data():
    data = fetch_data()
    logger.info(f"Data fetched from the database: {data}")
    return jsonify(data)

@app.route('/')
def index():
    return render_template('index.html')

def update_database_periodically():
    while True:
        update_database()
        time.sleep(30)

if __name__ == "__main__":
    drop_table("meteo_observations")
    init_db()
    update_thread = threading.Thread(target=update_database_periodically, daemon=True)
    update_thread.start()
    app.run(host='0.0.0.0', debug=True)
