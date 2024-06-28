from flask import Flask, jsonify, render_template
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

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
            cursor.execute('SELECT time, level FROM documents')
            rows = cursor.fetchall()
            return rows
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
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
