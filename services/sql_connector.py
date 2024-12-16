import psycopg
import pandas as pd


def send_sql_query(pg_config: dict, sql_query: str, raw: bool = False):
    """
    Sends an SQL query to a PostgreSQL database and returns the result.

    Args:
        pg_config (dict): Configuration for connecting to PostgreSQL.
        sql_query (str): SQL query to execute.
        raw (bool): If True, returns raw rows and column names; otherwise, returns a DataFrame.

    Returns:
        pd.DataFrame or tuple: Query result as a DataFrame or raw rows and column names.
    """
    try:
        # Establish a connection to the database
        with psycopg.connect(
            f"dbname={pg_config['POSTGRES_DB']} user={pg_config['POSTGRES_USER']} "
            f"password={pg_config['POSTGRES_PASSWORD']} host={pg_config.get('HOST', 'localhost')} "
            f"port={pg_config.get('PORT', '5432')}"
        ) as conn:
            # Create a cursor object
            with conn.cursor() as cur:
                # Execute the SQL query
                cur.execute(sql_query)

                # Fetch all rows
                rows = cur.fetchall()
                if not rows or cur.description is None:
                    print('No data returned or query does not produce a result set!')
                    return None

                # Get column names
                col_names = [desc.name for desc in cur.description]

                if raw:
                    return rows, col_names

                # Convert to a DataFrame
                df = pd.DataFrame(rows, columns=col_names)
                return df
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return None
