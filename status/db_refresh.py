import pandas as pd
import numpy as np
import os
from config.config import path_to_NAS, pair


def refresh_db(db_name):

    """This function takes in the name of a DB (trades or orders) and refreshes it to include all downloaded values.
    As of SEP24, values can either be 'Trades' or 'Orders'."""

    directory = f'{path_to_NAS}{pair.split('/')[0]}/{db_name}'

    dfs = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv') and filename != f'all_{db_name}.csv':
            # Read csv files into a DataFrame
            df = pd.read_csv(os.path.join(directory, filename), index_col=0)
            # Clean first col
            # df.drop(df.columns[0],axis=1, inplace=True)
            # Append the DataFrame to the list
            dfs.append(df)

    # Concatenate all DataFrames in the list into a single DataFrame
    trades = pd.concat(dfs)
    trades.drop_duplicates(inplace=True, subset='id')
    trades.to_csv(f'{directory}all_{db_name}.csv')

