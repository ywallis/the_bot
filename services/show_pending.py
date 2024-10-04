import psycopg
from dotenv import dotenv_values
import pandas as pd
from sql_queries.queries import orphans
pg_config = dotenv_values('../docker/database/.env')


# Establish a connection to the database
with psycopg.connect(
    f"dbname={pg_config['POSTGRES_DB']} user={pg_config['POSTGRES_USER']} password={pg_config['POSTGRES_PASSWORD']} host=localhost port=5432"
) as conn:

    # Create a cursor object
    with conn.cursor() as cur:
        # Execute a SQL query
        cur.execute(
            orphans
        )

        # Fetch all the data
        rows = cur.fetchall()

        # Print the fetched data
        for row in rows:
            print(row[0], row[3])

        df = pd.DataFrame(rows)

        print(df)

# Connection is automatically closed when the 'with' block exits