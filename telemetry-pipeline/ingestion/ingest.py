# ingest.py: loads sample readings into a database

import duckdb
import os

# Create warehouse directory if it doesn't exist
os.makedirs("warehouse", exist_ok=True)

# Create a connection to a db file, duckdb creates it if doesnt exist
con = duckdb.connect("warehouse/telemetry.duckdb")

# Create table and load data into it
# We add _ingested_at so we know when data was loaded (for traceability)
con.sql("DROP TABLE IF EXISTS raw_telemetry")
con.sql("""
    CREATE TABLE raw_telemetry AS
    SELECT *, current_timestamp AS _ingested_at
    FROM read_json_auto('raw_data/readings.ndjson')
""")

count = con.sql("SELECT COUNT(*) FROM raw_telemetry").fetchone()[0]
print(f"Loaded {count:,} readings into raw_telemetry")

# Load reference tables — these are the lookup/dimension data
con.sql("DROP TABLE IF EXISTS raw_machines")
con.sql("""
    CREATE TABLE raw_machines AS
    SELECT *, current_timestamp AS _ingested_at
    FROM read_json_auto('raw_data/reference/machines.json')
""")
print(f"Loaded {con.sql('SELECT COUNT(*) FROM raw_machines').fetchone()[0]} machines")

con.sql("DROP TABLE IF EXISTS raw_error_codes")
con.sql("""
    CREATE TABLE raw_error_codes AS
    SELECT *, current_timestamp AS _ingested_at
    FROM read_json_auto('raw_data/reference/error_codes.json')
""")
print(f"Loaded {con.sql('SELECT COUNT(*) FROM raw_error_codes').fetchone()[0]} error codes")

con.sql("DROP TABLE IF EXISTS raw_sites")
con.sql("""
    CREATE TABLE raw_sites AS
    SELECT *, current_timestamp AS _ingested_at
    FROM read_json_auto('raw_data/reference/sites.json')
""")
print(f"Loaded {con.sql('SELECT COUNT(*) FROM raw_sites').fetchone()[0]} sites")

# show a sample reading to confirm it looks right
print("\nSample reading:")
sample = con.sql("SELECT * FROM raw_telemetry LIMIT 1").fetchone()
columns = [desc[0] for desc in con.sql("SELECT * FROM raw_telemetry LIMIT 1").description]
for col, val in zip(columns, sample):
    print(f"  {col}: {val}")

con.close()
print("\nDone! Database at warehouse/telemetry.duckdb")
