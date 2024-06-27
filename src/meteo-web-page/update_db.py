import requests
import sqlite3
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


# Function to initialize the database
def init_db():
    conn = sqlite3.connect('meteo_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY,
            created TEXT,
            about TEXT,
            title TEXT,
            location TEXT,
            time TEXT,
            localtime TEXT,
            level REAL,
            level_units TEXT,
            object_id INTEGER,
            object_title TEXT,
            object_type TEXT
        )
    ''')
    conn.commit()
    return conn


# Function to save document content to the database
def save_to_db(conn, doc_id, doc_content):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO documents (
                id, created, about, title, location, time, localtime, level, level_units, object_id, object_title, object_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    except sqlite3.IntegrityError:
        print(f"Document with id {doc_content['head']['id']} already exists in the database. Skipping.")


def main():
    api_key = API_KEY
    conn = init_db()

    try:
        doc_ids = get_doc_ids(api_key)
        for doc_id in doc_ids:
            print(doc_id)
            doc_content = get_doc_content(api_key, doc_id)
            save_to_db(conn, doc_id, doc_content)
            time.sleep(1)  # To avoid hitting the API rate limit
    finally:
        conn.close()


if __name__ == "__main__":
    main()