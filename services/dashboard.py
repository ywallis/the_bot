import os
import sys
import ccxt
from datetime import datetime

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from services.sql_connector import send_sql_query
from services.sql_queries import *


from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

daily_inv = send_sql_query(pg_config, daily_inventory)
print('Daily shift in inventory')
print(daily_inv)
daily_u = send_sql_query(pg_config, daily_usdt)
print('Daily shift in USDT')
print(daily_u)