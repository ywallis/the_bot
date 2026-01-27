"""
This module generates a daily and monthly dashboard overview of the bot's performance.
It executes SQL queries to aggregate trading data and prints the results.
"""

from psycopg import sql

from apps.accountant.src.sql_connector import QueryLoader, send_sql_query
from apps.accountant.src.utils import load_pg_config
from apps.shared.src.utils import pairs

if __name__ == "__main__":
    pg_config = load_pg_config()
    query_loader = QueryLoader()
    query_loader.load_queries()
    daily_string = query_loader.get_query("daily_overview")
    if daily_string is None:
        raise Exception("Query could not be loaded")
    daily_overview = sql.SQL(daily_string).format(
        symbol=sql.Placeholder("symbol"), range=sql.Placeholder("range")
    )
    monthly_string = query_loader.get_query("monthly_overview")
    if monthly_string is None:
        raise Exception("Query could not be loaded")
    monthly_overview = sql.SQL(monthly_string).format(
        symbol=sql.Placeholder("symbol"), range=sql.Placeholder("range")
    )

    for sym in pairs:
        daily_performance = send_sql_query(
            pg_config, daily_overview, False, {"symbol": sym, "range": 3}
        )
        print("Daily performance:")
        print(daily_performance)
        monthly_performance = send_sql_query(
            pg_config, monthly_overview, False, {"symbol": sym, "range": 3}
        )
        print("Monthly performance:")
        print(monthly_performance)
