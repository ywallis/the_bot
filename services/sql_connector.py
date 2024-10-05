import psycopg
import pandas as pd


def send_sql_query(pg_config :dict, sql_query :str, raw :bool=False):


    # Establish a connection to the database
    with psycopg.connect(
        f"dbname={pg_config['POSTGRES_DB']} user={pg_config['POSTGRES_USER']} password={pg_config['POSTGRES_PASSWORD']} host=localhost port=5432"
    ) as conn:

        # Create a cursor object
        with conn.cursor() as cur:
            # Execute a SQL query
            cur.execute(
                sql_query
            )

            # Fetch all the data
            rows = cur.fetchall()
            if len(rows) == 0:
                print('No data returned from query!')
                return


            # Get column names
            col_names = [desc.name for desc in cur.description]

            if raw:
                return rows, col_names

            # Print the fetched data
            # for row in rows:
            #     print(row[0], row[3])

            df = pd.DataFrame(rows)
            # Add col names to df
            df.columns = col_names

            return df

