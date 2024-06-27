from flask import Flask, jsonify, render_template
import sqlite3

app = Flask(__name__)

def fetch_data():
    conn = sqlite3.connect('meteo_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT time, level FROM documents')
    rows = cursor.fetchall()
    conn.close()
    return rows

@app.route('/data')
def data():
    data = fetch_data()
    return jsonify(data)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)
