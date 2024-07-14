import logging
import os

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from mysql.connector import Error

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
                CREATE TABLE IF NOT EXISTS meteo_observations (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    datetime TEXT,
                    level REAL,
                    wind_speed REAL,
                    wind_direction REAL
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
            # Update the query to select all required fields
            cursor.execute('''
                SELECT datetime, level, wind_speed, wind_direction
                FROM meteo_observations
                ORDER BY datetime DESC
                LIMIT 20
            ''')
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
def save_to_db(conn, df: pd.DataFrame):
    if conn and conn.is_connected():
        #df.to_sql('meteo_observations', conn, if_exists='replace', index=False)
        cursor = conn.cursor()
        for row in df.itertuples(index=False):
            cursor.execute('''
                INSERT INTO meteo_observations (
                    datetime,
                    level,
                    wind_speed,
                    wind_direction
                ) VALUES (%s, %s, %s, %s)
            ''', (
                row.datetime,
                row.level,
                row.wind_speed,
                row.wind_direction
            ))
        conn.commit()
        logger.info(f"{df.shape[0]} observations saved to the database.")


def drop_table(table_name: str):
    """Drop the specified table from the database.

    Args:
        table_name (str): The name of the table to drop.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        if connection and connection.is_connected():
            cursor = connection.cursor()
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_query)
            connection.commit()
            logger.info(f"Table '{table_name}' dropped successfully.")
    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("Database connection closed.")
