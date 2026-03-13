# ingest.py: loads sample readings into a database

import duckdb
import os

# Create warehouse directory if it doesn't exist
os.makedirs("warehouse", exist_ok=True)

# Create a connection to a db file, duckdb creates it if doesnt exist
con = duckdb.connect("warehouse.db")

# Create table and load data into it
con.sql("CREATE TABLE raw_telemetry AS SELECT * FROM read_json_auto('raw_data/readings.ndjson')")