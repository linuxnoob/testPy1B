import logging
import time

import csv
import pymysql  # Import pymysql for MySQL connection
import polars as pl

logger = logging.getLogger()


def load_dataset(start: float, path: str, mysql_config: dict) -> float:
    """
    Loads dataset from CSV to MySQL database.

    Args:
        start: Float representing starting time.
        path: String representing path to CSV file.
        mysql_config: Dictionary containing MySQL connection details (host, user, password, database).

    Returns:
        Float representing current time after processing.
    """

    rows = []
    logger.info(f"reading csv. elapsed={time.time() - start}")
    start = time.time()
    with open(path, "r") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            rows.append((row[0], float(row[1])))

    # Connect to MySQL database
    connection = pymysql.connect(**mysql_config)
    cursor = connection.cursor()

    # Create table "dataset" (adjust data types if needed)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dataset (
            location text,
            temperature double
        )
    """)

    logger.info(f"dumping output. elapsed={time.time() - start}")
    start = time.time()
    # Insert data using executemany
    cursor.executemany("INSERT INTO dataset VALUES(?, ?)", rows)
    connection.commit()

    connection.close()
    return start


def process_dataset(start: float, mysql_config: dict) -> float:
    """
    Processes dataset from MySQL and saves results as Parquet file.

    Args:
        start: Float representing starting time.
        mysql_config: Dictionary containing MySQL connection details (host, user, password, database).

    Returns:
        Float representing current time after processing.
    """

    logger.info(f"processing dataset. elapsed: {time.time() - start}")
    start = time.time()

    # Connect to MySQL database
    connection = pymysql.connect(**mysql_config)
    cursor = connection.cursor()

    # Read data using cursor and build query string
    cursor.execute("""
    SELECT location,
           avg(temperature) as temperature_mean,
           max(temperature) as temperature_max,
           min(temperature) as temperature_min 
    FROM dataset
    GROUP BY location
    ORDER BY location
    """)

    data = cursor.fetchall()  # Fetch all results

    connection.close()

    # Create Polars DataFrame from fetched data
    df = pl.DataFrame(data, columns=["location", "temperature_mean", "temperature_max", "temperature_min"])

    logger.info(f"dumping output. elapsed: {time.time() - start}")
    start = time.time()
    df.write_parquet("result_mysql.parquet")
    return start


# Example usage (replace with your actual MySQL config)
mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "arkand",
    "database": "world",
}

# Assuming load_dataset is called first
start_time = time.time()
start_time = load_dataset(start_time, "your_data.csv", mysql_config)
process_dataset(start_time, mysql_config)

print(f"Total processing time: {time.time() - start_time:.2f} seconds")
