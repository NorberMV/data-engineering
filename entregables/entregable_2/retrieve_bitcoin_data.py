
import os
import requests
import time
from configparser import ConfigParser
from sqlalchemy import create_engine
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
    engine = create_engine(conn_str)
    try:
        # Connect to the database
        conn = engine.connect()
        return conn, engine
    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")
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
        print(err_msg)
        raise e
    data = response.json()
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
    if conn is not None:
        try:
            with conn.begin() as trans:
                conn.execute(
                    f"""
                    DROP TABLE IF EXISTS {full_schema};
                    CREATE TABLE {full_schema} (
                        Date TIMESTAMP,
                        prices FLOAT,
                        market_caps FLOAT,
                        total_volumes FLOAT
                    );
                    """
                )
                trans.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            conn.close()
    else:
        print("Failed to connect to the database.")

    # Let's wait for a 3 seconds to populate the table
    print('\n\nsleeping for 3 seconds...')
    time.sleep(3)
    # Get connection and engine again
    conn, engine = conn_to_db(conn_str)
    print('\n\nConnecting again...')

    # Populate the table with the DataFrame
    if conn is not None:
        try:
            with conn.begin() as trans:
                for index, row in df.iterrows():
                    sql = f"""
                    INSERT INTO {full_schema} (Date, prices, market_caps, total_volumes)
                    VALUES (%s, %s, %s, %s);
                    """
                    conn.execute(sql, (index, row['prices'], row['market_caps'], row['total_volumes']))
                # Commit the transaction
                trans.commit()
        except Exception as e:
            if isinstance(e, ResourceClosedError):
                print("Failed to connect to the database");
            else:
                print(f"An error occurred: {e}")
        finally:
            conn.close()
