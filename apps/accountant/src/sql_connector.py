"""
This module handles connections to the PostgreSQL database and query execution.
It provides a `send_sql_query` function for executing raw SQL and a `QueryLoader` class
to manage SQL query files.
"""

import os

import pandas as pd
import psycopg
from psycopg.sql import SQL, Composed


def send_sql_query(
    pg_config: dict,
    sql_query: SQL | Composed,
    raw: bool = False,
    parameters: dict[str, str | int] | tuple | None = None,
):
    """
    Sends an SQL query to a PostgreSQL database and returns the result.

    Parameters
    ----------
    pg_config : dict
        Configuration for connecting to PostgreSQL.
    sql_query : SQL | Composed
        SQL query to execute.
    raw : bool, optional
        If True, returns raw rows and column names; otherwise, returns a DataFrame. Default is False.
    parameters : dict[str, str | int] | tuple | None, optional
        Variables for the query.

    Returns
    -------
    pd.DataFrame | tuple | None
        Query result as a DataFrame or raw rows and column names.
    """
    if parameters is None:
        parameters = ()
    try:
        # Establish a connection to the database
        with psycopg.connect(
            f"dbname={pg_config['POSTGRES_DB']} user={pg_config['POSTGRES_USER']} "
            f"password={pg_config['POSTGRES_PASSWORD']} host={pg_config.get('HOST', 'localhost')} "
            f"port={pg_config['POSTGRES_PORT']}"
        ) as conn:
            # Create a cursor object
            with conn.cursor() as cur:
                # Execute the SQL query
                cur.execute(sql_query, parameters)

                # Fetch all rows
                rows = cur.fetchall()
                if not rows or cur.description is None:
                    return None

                # Get column names
                col_names = [desc.name for desc in cur.description]

                if raw:
                    return rows, col_names

                # Convert to a DataFrame
                df = pd.DataFrame(rows, columns=col_names)  # pyright: ignore
                return df
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return None


class QueryLoader:
    """
    A helper class to load SQL queries from files.
    """
    def __init__(self, query_dir="apps/accountant/src/queries"):
        self.query_dir = query_dir
        self.queries = {}

    def load_queries(self):
        """Load all SQL queries from the query directory."""
        for root, _, files in os.walk(self.query_dir):
            for file in files:
                if file.endswith(".sql"):
                    query_name = os.path.splitext(file)[0]  # e.g., get_user_by_id
                    file_path = os.path.join(root, file)
                    with open(file_path, "r") as f:
                        self.queries[query_name] = f.read()

    def get_query(self, query_name):
        """
        Retrieve a query by its name.

        Parameters
        ----------
        query_name : str
            The name of the query file (without extension).

        Returns
        -------
        str | None
            The SQL query string, or None if not found.
        """
        return self.queries.get(query_name, None)
