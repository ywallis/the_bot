import os
import sys
import ccxt
from datetime import datetime

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from services.status_clients import pair, all_clients
from services.sql_connector import send_sql_query
from services.sql_queries import *
from services.accounting_boiler import prepare_items_for_pg, export_to_sql


from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

print('Looking to place orphans with:')
for client in all_clients:
    print(client.name)

data = send_sql_query(pg_config, orphans, raw=False)

time = datetime.now()

try:
    for index, row in data.iterrows():

        for client in all_clients:
            if row['exchange'] == client.name:
                print(f'Identified order matching client {client.name}')
                print(row['order_id'])
                try:
                    order = client.fetch_order(symbol=pair, id=row['order_id'])
                    print(order)
                    prepared_order = prepare_items_for_pg(client, order)
                    export_to_sql(prepared_order, pg_config, 'orders')
                except ccxt.ExchangeError:
                    print('Could not fetch order, logging')
                    with open(f'../Logs/Orphans {time}', 'a') as file:
                        file.write(f'\n {row}')


                # Break to stop looking if client found
                break


    print('All done!')
except AttributeError:
    print('No data returned.')