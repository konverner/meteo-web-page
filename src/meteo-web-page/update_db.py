import requests
import mysql.connector
from mysql.connector import Error
import time

API_KEY = "C1B221F5B67ACA22D9874D36C57799F4"
BASE_URL = 'https://api.meteo.nw.ru/api'

# Function to get the list of document IDs
def get_doc_ids(api_key):
    url = f"{BASE_URL}/apigetqueue?api_key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['docs']

# Function to get the document content by ID
def get_doc_content(api_key, doc_id):
    url = f"{BASE_URL}/apigetdoc?api_key={api_key}&doc={doc_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def delete_doc(api_key, doc_id):
    url = f"{BASE_URL}/apideldoc?api_key={api_key}&doc={doc_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()
    

# Function to initialize the database
def init_db():
    try:
        connection = mysql.connector.connect(
            host='server182.hosting.reg.ru',
            user='u1956639_level',
            password='GNM-f5P-78w-S5g',
            database='u1956639_level'
        )
        if connection and connection.is_connected():
            cursor = connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS documents (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    created DATETIME,
                    about TEXT,
                    title TEXT,
                    location TEXT,
                    time DATETIME,
                    `localtime` DATETIME,
                    level FLOAT,
                    level_units TEXT,
                    object_id INT,
                    object_title TEXT,
                    object_type TEXT
                )
            ''')


            connection.commit()
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

# Function to save document content to the database
def save_to_db(conn, doc_id, doc_content):
    if conn and conn.is_connected():
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO documents (
                    id, created, about, title, location, time, `localtime`, level, level_units, object_id, object_title, object_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                doc_id,
                doc_content['head']['created'],
                doc_content['head']['about'],
                doc_content['head']['title'],
                doc_content['content']['30520'],
                doc_content['content']['time'],
                doc_content['content']['localtime'],
                doc_content['content']['level'],
                doc_content['content']['level_units'],
                doc_content['content']['object'],
                doc_content['content']['object_title'],
                doc_content['content']['object_type']
            ))
            conn.commit()
        except mysql.connector.IntegrityError:
            print(f"Document with id {doc_id} already exists in the database. Skipping.")

def main():
    api_key = API_KEY
    conn = init_db()

    if conn is None:
        print("Failed to connect to the database. Exiting.")
        return

    try:
        doc_ids = get_doc_ids(api_key)
        for doc_id in doc_ids:
            print(doc_id)
            doc_content = get_doc_content(api_key, doc_id)
            save_to_db(conn, doc_id, doc_content)
            delete_doc(api_key, doc_id)
            time.sleep(1)  # To avoid hitting the API rate limit
    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    main()
