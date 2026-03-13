# telemetry-pipeline

data pipeline that simulates IoT sensor data from industrial ore analyzers, uploads to AWS S3, ingests into DuckDB, transforms with dbt, and displays on a streamlit dashboard.

## stack
python, AWS S3, DuckDB, dbt, streamlit, plotly

## what it does
simulates 5 ore analysers at mine sites across aus, PNG and canada. each machine sends temp, vibration, power and throughput readings every 30 secs with anomalies injected (overheating, faults, connectivity drops etc).

raw data gets uploaded to s3, pulled into duckdb, transformed thru dbt layers (staging > intermediate > marts), then displayed on a dashboard with health cards, trend charts, anomaly alerts and per-machine deep dives.

## setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
aws configure 

# INFO: need an s3 bucket called telemetry-pipeline-raw, can also make ur own bucket name using command: python3 -m awscli s3 mb s3://telemetry-pipeline-raw --region ap-southeast-2 < configured to match my own preferences this can always be changed  just ensure you have your own aws account

make_bucket: telemetry-pipeline-raw
```

## run it
```bash
python3 simulator/generate.py
python3 ingestion/ingest.py
cd dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir . && cd ..
streamlit run dashboard/app.py
```

works without aws too, just falls back to local files.

## structure
```
simulator/generate.py     - generates ~28k sensor readings, uploads to s3
ingestion/ingest.py       - downloads from s3, loads into duckdb
dbt_project/models/
  staging/                - cleans nulls, casts types
  intermediate/           - hourly aggregations, anomaly detection
  marts/                  - equipment health, dashboard data, anomaly feed
dashboard/app.py          - streamlit dashboard
```

## design stuff
- duckdb instead of snowflake so it runs lcally with no cloud setup. sql is basically the same, swapping to snowflake is a one line config change
- ndjson format bc its appendable and streamable, standard for telemetry data
- anomaly detection uses rolling window averages per machine instead of static thresholds. machine in PNG runs hotter than one in canada so comparing against its own history makes more sense
- dbt layers are separated so changing anomaly logic doesnt break ingestion

## todo
- hive style date partitioning on s3!!
- lambda trigger to auto ingest when new files land
- more machines + longer time ranges maybe more variety too 
- fault injection from the dashboard
