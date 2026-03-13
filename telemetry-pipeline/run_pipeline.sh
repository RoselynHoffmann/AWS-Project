#!/bin/bash
set -e
echo "================================================"
echo "  Industrial Equipment Telemetry Pipeline"
echo "================================================"
echo ""
echo "Step 1/4: Generating data & uploading to S3..."
python simulator/generate.py
echo ""
echo "Step 2/4: Downloading from S3 & ingesting into DuckDB..."
python ingestion/ingest.py
echo ""
echo "Step 3/4: Running dbt transformations..."
cd dbt_project
dbt run --profiles-dir .
echo ""
echo "Running dbt tests..."
dbt test --profiles-dir .
cd ..
echo ""
echo "Step 4/4: Launching dashboard..."
echo "Open http://localhost:8501 in your browser"
streamlit run dashboard/app.py
