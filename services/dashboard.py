import os
import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from services.sql_connector import send_sql_query
from services.query_loader import QueryLoader

from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

test_sym = 'ALPH/USDT'

# Initialize QueryLoader

query_loader = QueryLoader()
query_loader.load_queries()
daily_overview = query_loader.get_query('daily_overview')
monthly_overview = query_loader.get_query('monthly_overview')

daily_performance = send_sql_query(pg_config, daily_overview, False, {'symbol': test_sym})
#daily_performance = send_sql_query(pg_config, daily_overview, False)
print('Daily performance:')
print(daily_performance)
monthly_performance = send_sql_query(pg_config, monthly_overview)
print('Monthly performance:')
print(monthly_performance)