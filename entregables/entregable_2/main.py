
import os
import requests
import pprint
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError, ResourceClosedError
import pandas as pd
from pathlib import Path
import yfinance as yf
from dotenv import load_dotenv
load_dotenv()


# This file is used by dotenv to load the db credentials
env_file = Path('.').resolve() / '.env'

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
    engine = sa.create_engine(conn_str)
    try:
        # Connect to the database
        conn = engine.connect()
        return conn, engine
    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")
        return None, None


if __name__=="__main__":
    # Build the connection string, and connect to the DB
    conn_str = build_conn_string(
        user,
        passw,
        host,
        port,
        dbname
    )
    try:
        conn, engine = conn_to_db(conn_str)
        pprint.pprint(f"Connecting to {conn_str!r}...")
    except Exception as e:
        print(f"Connection to {conn_str!r} failed, see the error below:\n{e!r}")

    else:
        print(f"Connected successfully: {conn!r}")
    finally:
        conn.close()

    # Retrieve Bitcoin data from the API
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    parameters = {
        'vs_currency': 'usd',
        'days': '30',
        'interval': 'daily',  
    }
    response = requests.get(url, params=parameters)
    data = response.json()

    prices_data = data['prices']
    market_caps_data = data['market_caps']
    total_volumes_data = data['total_volumes']

    # Convert to DataFrames
    df_prices = pd.DataFrame(prices_data, columns=['timestamp', 'prices'])
    df_market_caps = pd.DataFrame(market_caps_data, columns=['timestamp', 'market_caps'])
    df_total_volumes = pd.DataFrame(total_volumes_data, columns=['timestamp', 'total_volumes'])

    # Convert timestamp to datetime
    df_prices['timestamp'] = pd.to_datetime(df_prices['timestamp'], unit='ms')
    df_market_caps['timestamp'] = pd.to_datetime(df_market_caps['timestamp'], unit='ms')
    df_total_volumes['timestamp'] = pd.to_datetime(df_total_volumes['timestamp'], unit='ms')

    # Merge the DataFrames on timestamp
    df = pd.merge(df_prices, df_market_caps, on='timestamp', how='outer')
    df = pd.merge(df, df_total_volumes, on='timestamp', how='outer')

    # Set timestamp as the index
    df.set_index('timestamp', inplace=True)

    print(df.head(5))
