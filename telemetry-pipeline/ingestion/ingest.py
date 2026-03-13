# ingest.py - downloads from s3 and loads into duckdb

import duckdb
import os
import boto3

# make dirs if they dont exist
os.makedirs("warehouse", exist_ok=True)
os.makedirs("raw_data/reference", exist_ok=True)

# s3 config - same bucket as generate.py
S3_BUCKET = "telemetry-pipeline-raw"
s3 = boto3.client("s3")

# try to grab the data from s3 first
print(f"Downloading from s3 bucket: {S3_BUCKET}...")
try:
    s3.download_file(S3_BUCKET, "equipment_telemetry/readings.ndjson", "raw_data/readings.ndjson")
    print("  got readings.ndjson")
    for filename in ["machines.json", "error_codes.json", "sites.json"]:
        s3.download_file(S3_BUCKET, f"reference/{filename}", f"raw_data/reference/{filename}")
    print("  got reference data")
except Exception as e:
    print(f"  s3 download failed: {e}")
    print("  falling back to local files in raw_data/")

# connect to duckdb: creates the file if it doesnt exist
con = duckdb.connect("warehouse/telemetry.duckdb")

# load readings into raw_telemetry table
# _ingested_at is so we know when this data was loaded
con.sql("DROP TABLE IF EXISTS raw_telemetry")
con.sql("""
    CREATE TABLE raw_telemetry AS
    SELECT *, current_timestamp AS _ingested_at
    FROM read_json_auto('raw_data/readings.ndjson')
""")

count = con.sql("SELECT COUNT(*) FROM raw_telemetry").fetchone()[0]
print(f"\nLoaded {count:,} readings into raw_telemetry")

# load reference tables: these are the lookup/dimension data
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

# Quick sanity check
print("\nSample reading:")
sample = con.sql("SELECT * FROM raw_telemetry LIMIT 1").fetchone()
columns = [desc[0] for desc in con.sql("SELECT * FROM raw_telemetry LIMIT 1").description]
for col, val in zip(columns, sample):
    print(f"  {col}: {val}")

con.close()
print("\ndone! database at warehouse/telemetry.duckdb")
