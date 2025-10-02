from influxdb_client import InfluxDBClient
import csv
import os
from concurrent.futures import ThreadPoolExecutor
from helper.configurations import *
from helper.influxdb_helper import *

# ****************** MAKE YOUR SELECTION ************************ #
radar_name = "C004"
time_range_start = "2025-08-11T07:30:00Z"  # UTC TIME !!!
time_range_stop = "2025-08-11T12:00:00Z"  # UTC TIME !!!
# *************************************************************** #

config = retrieve_yaml_file()

# Configuration
bucket = config["influxdb"]["bucket"]
org = config["influxdb"]["org"]
token = config["influxdb"]["read-token"]
url = config["influxdb"]["url"]

radar_measurement_name = "radar_measurement"

# Get timestamps
timestamps = get_timestamps(config, radar_name, radar_measurement_name, time_range_start, time_range_stop)

# Make connection
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

def download_and_save(ts):
    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {time_range_start}, stop: {time_range_stop})
    |> filter(fn: (r) => r._measurement == "{radar_measurement_name}")
    |> filter(fn: (r) => r["radar"] == "{radar_name}")
    |> filter(fn: (r) => r._time == time(v: "{ts}"))
    |> pivot(rowKey:["_time", "frequency", "pol"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: ["frequency", "pol", "real", "imag"])
    '''
    result = query_api.query(org=org, query=query)
    records = []
    for table in result:
        for row in table.records:
            records.append({
                "frequency": row["frequency"],
                "pol": row["pol"],
                "real": row["real"],
                "imag": row["imag"]
            })

    output_dir = radar_name
    os.makedirs(output_dir, exist_ok=True)
    for pol in set(r["pol"] for r in records):
        timestamp = datetime.fromisoformat(ts).strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(output_dir, f"{timestamp}_{pol}.csv")
        with open(output_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["frequency", "real", "imag"])
            for r in records:
                if r["pol"] == pol:
                    writer.writerow([r["frequency"], r["real"], r["imag"]])
        print(f"Data saved in {output_file}")

# Run in parallel (e.g. max 5 workers)
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(download_and_save, timestamps)
