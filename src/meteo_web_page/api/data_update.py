import os
import requests
import time
import logging
from dotenv import load_dotenv

from db.database import init_db, save_to_db

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def update_database():
    api_key = API_KEY
    conn = init_db()

    if conn is None:
        logger.error("Failed to connect to the database. Exiting.")
        return
    try:
        doc_ids = get_doc_ids(api_key)
        for doc_id in doc_ids:
            doc_content = get_doc_content(api_key, doc_id)
            if doc_content["head"]["title"] == 'р. Большая Нева – Горный институт':
                save_to_db(conn, doc_id, doc_content)
            delete_doc(api_key, doc_id)
            logger.info(f"Document with id {doc_id} processed and deleted.")
            time.sleep(0.2)  # To avoid hitting the API rate limit
    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    update_database()