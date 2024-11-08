# db_connection.py

import psycopg2
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logger = logging.getLogger("<Bilstein SLExA ETL>")


class Database:
    def __init__(self):
        """Initialize database connection parameters."""
        self.conn = self.connect()

    def connect(self):
        """Establish a connection to the PostgreSQL database using environment variables."""
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", 5432),
            )
            logger.info("Successfully connected to the PostgreSQL database.")
            return conn
        except Exception as e:
            logger.error("Failed to connect to the database", exc_info=True)
            raise e

    def query(self, sql_query):
        """Execute a SQL query and return the results."""
        if not self.conn:
            print("No database connection.")
            return []

        with self.conn.cursor() as cursor:
            cursor.execute(sql_query)
            return cursor.fetchall()

    def fetch_data(self, table, fields, conditions=None):
        """
        Fetch data from a specified table with optional conditions.

        Args:
            table (str): Table name to query.
            fields (list): List of fields to retrieve.
            conditions (str, optional): SQL conditions to apply to the query.

        Returns:
            list: Query results as a list of dictionaries.
        """
        query = f"SELECT {', '.join(fields)} FROM {table}"
        if conditions:
            query += f" WHERE {conditions}"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing query on {table}: {e}")
            return []

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")
