import duckdb
import datetime
import argparse
import concurrent.futures

# Parse arguments
parser = argparse.ArgumentParser(description='Dump data from a DuckDB database.')
parser.add_argument('--db', type=str, required=True, help='Name of the DuckDB database file')
parser.add_argument('--days', type=int, default=7, help='Number of days to look back from today')
args = parser.parse_args()

# Define date range for the "last x days"
x_days_ago = (datetime.datetime.now() - datetime.timedelta(args.days)).strftime('%Y-%m-%d %H:%M:%S')


# Query to find tables and columns with TIMESTAMP data type
conn = duckdb.connect(database=args.db)
tables_columns = conn.execute("""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE data_type IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE')
""").fetchall()
conn.close()

# Function to dump data for a single table and column
def dump_data(table, column):
    conn = duckdb.connect(database=args.db)
    query = f"SELECT * FROM {table} WHERE '{column}' >= '{x_days_ago}'"
    result = conn.execute(query).fetchall()

    # Dump data to a file or process as needed
    with open(f"{table}_dump{args.days}days.csv", "w") as file:
        # Write header
        file.write(",".join([desc[0] for desc in conn.description]) + "\n")
        for row in result:
            file.write(",".join(map(str, row)) + "\n")

    conn.close()

    print(query)

# Iterate through tables and columns to dump data using multithreading
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(dump_data, table, column) for table, column in tables_columns]
    for future in concurrent.futures.as_completed(futures):
        future.result()  # Wait for all threads to complete
