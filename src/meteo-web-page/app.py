from flask import Flask, jsonify, render_template
import mysql.connector
from mysql.connector import Error
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_data():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='server182.hosting.reg.ru',
            user='u1956639_level',
            password='GNM-f5P-78w-S5g',
            database='u1956639_level'
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute('SELECT time, level FROM documents ORDER BY time DESC LIMIT 20')
            rows = cursor.fetchall()
            if rows:
                times = [row[0] for row in rows]
                min_time = min(times)
                max_time = max(times)
                logger.info(f"Data fetched from the database successfully. Period: {min_time} to {max_time}")
            else:
                logger.info("No data fetched from the database.")
            return rows
    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("Database connection closed.")
    return []

@app.route('/data')
def data():
    data = fetch_data()
    return jsonify(data)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)
