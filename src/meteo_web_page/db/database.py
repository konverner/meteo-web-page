import os
import logging

import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to initialize the database
def init_db():
    logger.info(f"Connecting to database {os.getenv('DB_DATABASE')} with host {os.getenv('DB_HOST')}")
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
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
            logger.info("Database initialized successfully.")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
        return None

# Function to fetch data from the database
def fetch_data():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
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
            logger.info(f"Document with id {doc_id} saved to the database.")
        except mysql.connector.IntegrityError:
            logger.warning(f"Document with id {doc_id} already exists in the database. Skipping.")
