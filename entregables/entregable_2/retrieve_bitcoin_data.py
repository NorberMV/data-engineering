
import os
import sys

import requests
import time
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError, ResourceClosedError
import pandas as pd
from pathlib import Path
import yfinance as yf
import logging
from dotenv import load_dotenv
load_dotenv()

# Event logging system Config.
logging.basicConfig(
    format='[%(name)s] %(asctime)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(name='Bitcoin Data ETL')
# This file is used by dotenv to load the db credentials
env_file = Path('.').resolve() / '.env'
# sql queries path
sql_root = Path('.').resolve() / 'sql'
create_db_sql = sql_root / 'create_db.sql'
populate_db_sql = sql_root / 'populate_db.sql'

# DB configuration environment variables
user = os.getenv('USERNAME')
passw = os.getenv('PASSW')
host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DB_NAME')
schema = "norbermv_dev_coderhouse"

def build_conn_string(
    user: str,
    passw: str,
    host: str,
    port: str,
    dbname: str
) -> URL:
    conn_string = URL.create(
        drivername="postgresql",
        username=user,
        password=passw,
        host=host,
        port=port,
        database=dbname
    )
    return conn_string

def conn_to_db(conn_str: URL) -> tuple :
    # Create an engine with the connection string
    engine = create_engine(conn_str)
    try:
        # Connect to the database
        conn = engine.connect()
        return conn, engine
    except SQLAlchemyError as e:
        logger.error(f"Error connecting to the database: {e}")
        return None, None

def get_bitcoin_data(vs_currency='usd', days='30', interval='daily'):
    """
    Make the request to retrieve Bitcoin data from the CoinGecko API.

    :param vs_currency: The target currency of market data (default 'usd').
    :param days: The number of days of data to retrieve (default '30').
    :param interval: The data update interval (default 'daily').
    :returns: A dictionary containing prices, market caps, and total volumes.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    parameters = {
        'vs_currency': vs_currency,
        'days': days,
        'interval': interval,
    }
    try:
        response = requests.get(url, params=parameters)
    except Exception as e:
        err_msg = (
            f"Something went wrong with the request. "
            f"Find the traceback below:\n{e}"
        )
        logger.error(err_msg)
        sys.exit(1)
    data = response.json()
    logger.debug(f"Retrieved {len(data)} items successfully from the API...")
    return data

def process_data_into_df(data):
    """
    Process the Bitcoin data into a pandas DataFrame.

    Parameters:
    :param data: The Bitcoin market data as a dictionary.

    :returns: A pandas DataFrame with 'timestamp' as index and columns for 'prices',
             'market_caps', and 'total_volumes'.
    """
    prices_data = data['prices']
    market_caps_data = data['market_caps']
    total_volumes_data = data['total_volumes']

    # Convert to DataFrames
    df_prices = pd.DataFrame(prices_data, columns=['timestamp', 'prices'])
    df_market_caps = pd.DataFrame(market_caps_data, columns=['timestamp', 'market_caps'])
    df_total_volumes = pd.DataFrame(total_volumes_data, columns=['timestamp', 'total_volumes'])

    # Convert timestamp to datetime
    for df in [df_prices, df_market_caps, df_total_volumes]:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Merge the DataFrames on timestamp
    df = pd.merge(df_prices, df_market_caps, on='timestamp', how='outer')
    df = pd.merge(df, df_total_volumes, on='timestamp', how='outer')
    df.set_index('timestamp', inplace=True)

    return df


if __name__=="__main__":

    data = get_bitcoin_data()
    df = process_data_into_df(data)
    # So far we got something like the following DataFrame:
    """
                      prices   market_caps  total_volumes
    timestamp
    2024-01-15  41800.932822  8.229071e+11   1.696896e+10
    2024-01-16  42587.336038  8.352260e+11   2.263453e+10
    2024-01-17  43148.001643  8.457709e+11   2.202312e+10
    2024-01-18  42713.859187  8.369880e+11   2.129906e+10
    2024-01-19  41261.394798  8.088458e+11   2.516043e+10
    """

    full_schema = f"{schema}.bitcoin_data"
    # Build the connection string, and connect to the DB
    conn_str = build_conn_string(
        user,
        passw,
        host,
        port,
        dbname
    )
    # Get connection and engine
    conn, engine = conn_to_db(conn_str)
    # Create the table 'bitcoin_data'
    logger.debug(f"Creating the Redshift table {full_schema!r}")
    if conn is not None:
        try:
            # Read the SQL file
            with open(create_db_sql.as_posix(), 'r') as file:
                create_db_sql = file.read()
            formatted_create_sql = create_db_sql.format(full_schema=full_schema)
            with conn.begin() as trans:
                conn.execute(formatted_create_sql)
                trans.commit()
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            conn.close()
    else:
        logger.debug("Failed to connect to the database.")

    # Let's wait for a 3 seconds to populate the table
    logger.debug('Sleeping for 3 seconds...')
    time.sleep(3)
    # Get connection and engine again
    conn, engine = conn_to_db(conn_str)
    logger.debug('Populating the Redshift table...')

    # Populate the table with the DataFrame
    if conn is not None:
        try:
            # Read the SQL file
            with open(populate_db_sql.as_posix(), 'r') as file:
                populate_db_sql = file.read()
            # Format the SQL template with the full_schema
            populate_db_sql_formatted = populate_db_sql.format(full_schema=full_schema)
            with conn.begin() as trans:
                for index, row in df.iterrows():
                    conn.execute(
                        populate_db_sql_formatted,
                        (
                            index,
                            row['prices'],
                            row['market_caps'],
                            row['total_volumes']
                        )
                    )
                # Commit the transaction
                trans.commit()
        except Exception as e:
            if isinstance(e, ResourceClosedError):
                logger.error("Failed to connect to the database");
            else:
                logger.error(f"An error occurred: {e}")
        finally:
            logger.debug("Closing the Redshift DB connection...")
            conn.close()
