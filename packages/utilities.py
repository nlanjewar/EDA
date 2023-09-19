
"""Importing required libraries"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
import pandas as pd
from sqlalchemy import create_engine


class NoDataException(Exception):
    """Raises a No Data Exception when data for next timestamp is unavailable"""


def create_logger():
    """Create a logger with default configurations."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d, %(levelname)s,%(filename)s,%(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    handler = RotatingFileHandler(
        "logs\\log.csv", maxBytes=5 * 1024 * 1024, backupCount=5
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def check_files(file_list):
    """Check required files before execution.
    ---------------------------------------------------------
    Input : file_list [list(str)] = List of required files to check before execution.
    Output : None if all files exist, else will break."""
    for file in file_list:
        if os.path.exists(file):
            pass
        else:
            print("Error while reading config files.")
            logging.error("Error while reading config files. Exiting...")
            print("Exiting...")
            sys.exit()



def db_conn(host, user, pwd, schema):
    """Creates a database connection using provided credentials.
    ---------------------------------------------------------
    Input : host [str], user [str], pwd [str], schema [str]
    Output : connection engine
    """
    db_engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{schema}")
    return db_engine


def read_input(
    exec_mode,
    taglist=None,
    alias_table=None,
    connection=None,
    input_path=None,
    time_range=None,
    time_freq=None,
    rolling_window=None,
):
    """Reads input data either from database or csv based on the selected mode.
    --------------------------------------------------------------------------
    Input : exec_mode [str] = 'connect' or 'noconnect'.\n
            input_table [str] = Table name for reading input data.\n
    """

    final_df = pd.DataFrame()

    # Reading Time.csv File
    time_df = pd.read_csv(r"config/time.csv")
    taglist_df = pd.read_csv(r"config/taglist.csv")
    table_names = taglist_df['table_name'].unique()

    if taglist is None:
        taglist = taglist_df['alias'].tolist()  # Use all aliases if not provided

    if alias_table is None:
        alias_table = "your_alias_table_name"  # Provide a default value or raise an error

    for table_name in table_names:
        alias_name = taglist_df.loc[taglist_df['table_name'] == table_name]['alias'].to_list()
        # define start_time and end_time and frequency
        # time_range_df = time_df.loc[time_df['table_name'] == table_name]
        start_time = pd.to_datetime(time_df['start_time'].iloc[0])
        end_time = pd.to_datetime(time_df['end_time'].iloc[0])
       
        taglist = alias_name
        input_table = table_name

        if time_range:
            if start_time > time_range[1] or end_time < time_range[0]:
                continue  # Skip this table if the time range is outside of available data
            else:
                start_time = max(start_time, time_range[0])
                end_time = min(end_time, time_range[1])
                
        if exec_mode == "noconnect":
            if connection is None:
                raise ValueError("Database connection is required in 'noconnect' mode")

            # Get tag IDs from database based on alias names
            taglist_str = ",".join([f"'{alias}'" for alias in taglist])
            query = f"SELECT id FROM {alias_table} WHERE name IN ({taglist_str});"
            tag_id_df = pd.read_sql(query, con=connection.connect())

            taglist = tag_id_df['id'].tolist()  # tag_ID

            query = f"SELECT * FROM {input_table} WHERE tag IN ({','.join(map(str, taglist))}) AND timestamp >= '{start_time}' AND timestamp <= '{end_time}';"
            data = pd.read_sql(query, con=connection.connect())
            
            final_df = pd.concat([final_df, data], ignore_index=True)

        else:
            if input_path is None:
                raise ValueError("Input path is required in non-'noconnect' mode")

            input_file = os.listdir(input_path)
            data = pd.read_csv(
                input_path + input_file[0],
                parse_dates=["timestamp"],
                usecols=["timestamp", "tag", "value"],
            )

            final_df = pd.concat([final_df, data], ignore_index=True)
            
    pd.concat([final_df,data],ignore_index=True)
    pivot_data = final_df.pivot(index='timestamp', columns='tag', values='value')
    pivot_data = pivot_data.reset_index()
    
    return pivot_data
        
    
