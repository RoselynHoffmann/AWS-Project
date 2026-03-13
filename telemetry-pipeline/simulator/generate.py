# generate.py: Generates all the sample readings for the simulator

import json
import random
import os
from datetime import datetime, timedelta, timezone

# Creates a folder for the raw data if it does not exist already
os.makedirs("raw_data", exist_ok=True)

# Define Machines in list as it does not have behaviour
machines = [
    {
    "machine_id": "Delta-001",
    "site": "Kalgoorlie, WA",
    "model": "OreSpec-X1",
    "baseline_temp": 68.0,
    "baseline_vibration": 2.1,
    "baseline_power": 12.5,
    "baseline_throughput": 30,
    },

    {
        "machine_id": "Delta-002",
        "site": "Adelaide, SA",
        "model": "GeoScan-X1",
        "baseline_temp": 50.0,
        "baseline_vibration": 2.0,
        "baseline_power": 12,
        "baseline_throughput": 45,
    },

    {
        "machine_id": "Delta-003",
        "site": "Melbourne, VIC",
        "model": "MineSense-X1",
        "baseline_temp": 76.0,
        "baseline_vibration": 2.5,
        "baseline_power": 10,
        "baseline_throughput": 60,
    }
]

# takes time in increments
duration_hours = 48
start_time = datetime(2026, 3, 7, 0, 0, 0)
total_seconds = duration_hours * 3600 #convert it to seconds
interval = 30  # seconds between readings

# loop through machines to generate new readings
with open("raw_data/readings.ndjson", "w") as f:
    for machine in machines:
        for i in range(0, total_seconds, interval):
            current_time = start_time + timedelta(seconds=i)
            reading = {
                "time": current_time.isoformat(),
                "machine_id": machine["machine_id"],
                "site": machine["site"],
                "model": machine["model"],
                "temp": random.gauss(machine["baseline_temp"], 1.5),
                "vibration": random.gauss(machine["baseline_vibration"], 0.3),
                "power": random.gauss(machine["baseline_power"], 0.5),
                "throughput": random.gauss(machine["baseline_throughput"], 3),
            }
            # Writes each json object into an ndjson file at file location
            f.write(json.dumps(reading) + "\n")
