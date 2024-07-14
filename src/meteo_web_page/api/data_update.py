import os
import requests
import time
import logging
from datetime import datetime
from typing import List

import pandas as pd
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


def prepare_batch(doc_ids: List[str], api_key: str) -> pd.DataFrame:
    """Run over docs with different measures and prepare batch group by time up to minutes
    Args:
        doc_ids: list of document ids
        api_key: API key for document retrieval
    Returns:
        pd.DataFrame: prepared data
    """
    data = []
    for doc_id in doc_ids:
        doc_content = get_doc_content(api_key, doc_id)
        if not doc_content:
            continue

        delete_doc(api_key, doc_id)
        logger.info(f"Document with id {doc_id} processed and deleted.")
        time.sleep(0.2)  # To avoid hitting the API rate limit

        head = doc_content.get("head", {})
        content = doc_content.get("content", {})

        time_str = content.get("time", "")  # 2024-07-14 11:00:00+00
        if time_str:
            time_value = datetime.strptime(time_str.replace("+00", ""), "%Y-%m-%d %H:%M:%S")
            time_value = time_value.replace(second=0, microsecond=0)  # Group by time up to minutes
        else:
            continue

        level = content.get("level")
        wind_speed = content.get("wind_speed")
        wind_direction = content.get("wind_direction")

        data.append({
            "datetime": time_value,
            "level": level,
            "wind_speed": wind_speed,
            "wind_direction": wind_direction
        })
    if data:
        # Create a DataFrame
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(columns=['datetime', 'level', 'wind_speed', 'wind_direction'])

    # Group by time up to minutes and aggregate the data
    df_grouped = df.groupby('datetime').agg({
        'level': 'first',
        'wind_speed': 'first',
        'wind_direction': 'first'
    }).reset_index()

    return df_grouped

def update_database():
    api_key = API_KEY
    conn = init_db()

    if conn is None:
        logger.error("Failed to connect to the database. Exiting.")
        return
    try:
        doc_ids = get_doc_ids(api_key)
        batch = prepare_batch(doc_ids, api_key)
        save_to_db(conn, batch)

    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    update_database()
