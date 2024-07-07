from flask import Flask, jsonify, render_template

from db.database import fetch_data

app = Flask(__name__)

@app.route('/data')
def data():
    data = fetch_data()
    return jsonify(data)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)
