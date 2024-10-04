import psycopg
from dotenv import dotenv_values
import pandas as pd
from sql_queries.queries import orphans, daily_inventory
pg_config = dotenv_values('../docker/database/.env')


# Establish a connection to the database
with psycopg.connect(
    f"dbname={pg_config['POSTGRES_DB']} user={pg_config['POSTGRES_USER']} password={pg_config['POSTGRES_PASSWORD']} host=localhost port=5432"
) as conn:

    # Create a cursor object
    with conn.cursor() as cur:
        # Execute a SQL query
        cur.execute(
            daily_inventory
        )

        # Fetch all the data
        rows = cur.fetchall()

        # Get column names
        col_names = [desc.name for desc in cur.description]

        # Print the fetched data
        # for row in rows:
        #     print(row[0], row[3])

        df = pd.DataFrame(rows)
        # Add col names to df
        df.columns = col_names

        print(df.tail(10))

# Connection is automatically closed when the 'with' block exits