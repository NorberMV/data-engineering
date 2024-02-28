from data import get_bitcoin_data, process_data_into_df
from utils import (
    load_and_format_sql,
    _populate_db,
    _retrieve_api_data,
    FULL_SCHEMA
)



def insert_to_redshift(df):
    """..."""
    # 1. Retrieve api data
    df = _retrieve_api_data()
    # 2. Load and format the SQL from the sql file
    str_query = load_and_format_sql(FULL_SCHEMA)
    # 3. Populate the Redshift DB table
    _populate_db(df, str_query=str_query)


