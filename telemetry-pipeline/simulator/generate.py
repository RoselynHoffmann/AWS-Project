# generate.py - generates all the sample readings for the simulator
# writes locally and uploads to s3

import json
import random
import os
import boto3
from datetime import datetime, timedelta, timezone

# make the raw data folder if it doesnt exist
os.makedirs("raw_data", exist_ok=True)

# s3 setup - change bucket name to ur own
S3_BUCKET = "telemetry-pipeline-raw"
S3_PREFIX = "equipment_telemetry"
s3 = boto3.client("s3")

# machines stored as dicts in a list since they dont need behaviour
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
    },
    {
        "machine_id": "Delta-004",
        "site": "Lihir, PNG",
        "model": "OreSpec-X2",
        "baseline_temp": 72.0,
        "baseline_vibration": 2.8,
        "baseline_power": 13.0,
        "baseline_throughput": 42,
    },
    {
        "machine_id": "Delta-005",
        "site": "Timmins, Canada",
        "model": "GeoScan-X2",
        "baseline_temp": 55.0,
        "baseline_vibration": 1.9,
        "baseline_power": 11.5,
        "baseline_throughput": 50,
    },
]

# anomaly scenarios - these are the problems we inject into the data
# so the pipeline actually has smth interesting to detect
anomalies = [
    {
        "machine_id": "Delta-004",
        "type": "gradual_overheat",
        "start_hour": 14,
        "duration_hours": 8,
        "temp_increase": 25.0,
    },
    {
        "machine_id": "Delta-003",
        "type": "sudden_fault",
        "start_hour": 20,
        "duration_hours": 2,
        "error_code": "E101",
    },
    {
        "machine_id": "Delta-001",
        "type": "connectivity_loss",
        "start_hour": 8,
        "duration_hours": 1,
    },
    {
        "machine_id": "Delta-005",
        "type": "high_throughput_stress",
        "start_hour": 6,
        "duration_hours": 10,
        "throughput_multiplier": 1.4,
    },
]

# error code lookup table - reference data for the warehouse
error_codes = {
    "E101": "Motor bearing failure",
    "E102": "X-ray source degradation",
    "E201": "Calibration drift detected",
    "E301": "Sample feeder jam",
    "E401": "Communication timeout",
}


def get_active_anomalies(machine_id, hour):
    """check which anomalies are active for a machine at a given hour"""
    active = []
    for a in anomalies:
        if a["machine_id"] != machine_id:
            continue
        start = a["start_hour"]
        end = start + a["duration_hours"]
        if start <= hour < end:
            active.append(a)
    return active


# time config
duration_hours = 48
start_time = datetime(2026, 3, 7, 0, 0, 0)
total_seconds = duration_hours * 3600  # convert to secs
interval = 30  # reading every 30 secs

total_readings = 0
lost_readings = 0

# loop thru machines and generate readings at each time step
with open("raw_data/readings.ndjson", "w") as f:
    for machine in machines:
        for i in range(0, total_seconds, interval):
            current_time = start_time + timedelta(seconds=i)
            hour_offset = i / 3600

            # check if any anomalies are active rn
            active = get_active_anomalies(machine["machine_id"], hour_offset)

            # if connectivity loss then no reading gets sent (creates gaps in data)
            skip = False
            for a in active:
                if a["type"] == "connectivity_loss":
                    skip = True
                    lost_readings += 1
            if skip:
                continue

            # baseline + random noise for normal fluctuation
            temp = random.gauss(machine["baseline_temp"], 1.5)
            vibration = random.gauss(machine["baseline_vibration"], 0.3)
            power = random.gauss(machine["baseline_power"], 0.5)
            throughput = random.gauss(machine["baseline_throughput"], 3)
            status = "running"
            error_code = None

            # apply whatever anomaly effects are active
            for a in active:
                if a["type"] == "gradual_overheat":
                    # temp goes up on a curve over the duration
                    progress = (hour_offset - a["start_hour"]) / a["duration_hours"]
                    temp += a["temp_increase"] * (progress ** 1.5)
                    power += 2.0 * progress  # cooling system works harder
                    if progress > 0.8:
                        status = "warning"

                if a["type"] == "sudden_fault":
                    temp += random.uniform(5, 15)
                    vibration += random.uniform(2, 8)
                    throughput = 0
                    status = "error"
                    error_code = a.get("error_code")

                if a["type"] == "high_throughput_stress":
                    throughput *= a["throughput_multiplier"]
                    power *= a["throughput_multiplier"]
                    vibration += 0.5

            # calibration happens every 6 hrs for like 10 min
            hours_since_midnight = current_time.hour + current_time.minute / 60
            if hours_since_midnight % 6 < 0.17 and status == "running":
                status = "calibrating"
                throughput = 0

            # sometimes sensors just glitch out (~0.5% of readings)
            if random.random() < 0.005:
                temp = -9999.0

            reading = {
                "timestamp": current_time.isoformat() + "Z",
                "machine_id": machine["machine_id"],
                "site": machine["site"],
                "model": machine["model"],
                "temperature_c": round(temp, 2),
                "vibration_mm_s": round(max(0, vibration), 3),
                "power_draw_kw": round(max(0, power), 2),
                "throughput_samples_hr": round(max(0, throughput)),
                "status": status,
                "error_code": error_code,
            }

            # Write each reading as a json line
            f.write(json.dumps(reading) + "\n")
            total_readings += 1

print(f"Generated {total_readings:,} readings ({lost_readings} lost to connectivity gaps)")

# upload to s3
print(f"\nUploading to s3 bucket: {S3_BUCKET}...")
try:
    s3.upload_file("raw_data/readings.ndjson", S3_BUCKET, f"{S3_PREFIX}/readings.ndjson")
    print(f"  uploaded to s3://{S3_BUCKET}/{S3_PREFIX}/readings.ndjson")
except Exception as e:
    print(f"  s3 upload failed: {e}")
    print("  (data still available locally in raw_data/)")

# reference data - lookup tables that give context to the telemetry
ref_dir = "raw_data/reference"
os.makedirs(ref_dir, exist_ok=True)

machines_ref = []
for m in machines:
    machines_ref.append({
        "machine_id": m["machine_id"],
        "site": m["site"],
        "model": m["model"],
        "install_date": "2024-06-15" if "X1" in m["model"] else "2025-01-20",
        "last_maintenance": "2026-01-10",
    })
with open(f"{ref_dir}/machines.json", "w") as f:
    json.dump(machines_ref, f, indent=2)

error_ref = [{"code": k, "description": v} for k, v in error_codes.items()]
with open(f"{ref_dir}/error_codes.json", "w") as f:
    json.dump(error_ref, f, indent=2)

sites_ref = [
    {"site": "Kalgoorlie, WA", "country": "Australia", "timezone": "Australia/Perth"},
    {"site": "Adelaide, SA", "country": "Australia", "timezone": "Australia/Adelaide"},
    {"site": "Melbourne, VIC", "country": "Australia", "timezone": "Australia/Melbourne"},
    {"site": "Lihir, PNG", "country": "Papua New Guinea", "timezone": "Pacific/Port_Moresby"},
    {"site": "Timmins, Canada", "country": "Canada", "timezone": "America/Toronto"},
]
with open(f"{ref_dir}/sites.json", "w") as f:
    json.dump(sites_ref, f, indent=2)

# upload ref data to s3 too
try:
    for filename in ["machines.json", "error_codes.json", "sites.json"]:
        s3.upload_file(f"{ref_dir}/{filename}", S3_BUCKET, f"reference/{filename}")
    print(f"  uploaded reference data to s3://{S3_BUCKET}/reference/")
except Exception as e:
    print(f"  s3 ref upload failed: {e}")

print("done!")
