from influxdb_client import InfluxDBClient
import csv
import os
from datetime import datetime
from helper.configurations import *
from helper.influxdb_helper import *

# ****************** MAKE YOUR SELECTION ************************ #
radar_name = "C003"
time_range_start = "2025-07-29T01:30:00Z"  # UTC TIME !!!
time_range_stop = "2025-07-29T20:00:00Z"  # UTC TIME !!!
# *************************************************************** #

config = retrieve_yaml_file()

# Configuration
bucket = config["influxdb"]["bucket"]
org = config["influxdb"]["org"]
token = config["influxdb"]["read-token"]
url = config["influxdb"]["url"]

radar_measurement_name = "radar_configuration"

# Get timestamps
timestamps = get_timestamps(config, radar_name, radar_measurement_name, time_range_start, time_range_stop)

print(len(timestamps))

# Make connection
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

def save_radar_configuration_csv(ts):
    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {time_range_start}, stop: {time_range_stop})
    |> filter(fn: (r) => r._measurement == "{radar_measurement_name}")
    |> filter(fn: (r) => r["radar"] == "{radar_name}")
    |> filter(fn: (r) => r._time == time(v: "{ts}"))
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    result = query_api.query(org=org, query=query)

    if not result:
        print(f"No data found for timestamp {ts}")
        return

    # Alleen eerste record nemen (er zou maar één moeten zijn per ts)
    record = result[0].records[0]
    output_dir = radar_name
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "radar_configurations.csv")
    write_header = not os.path.exists(output_file)

    with open(output_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow([
                "timestamp", "center", "span", "start", "stop",
                "power", "sweeps", "points", "ifbw", "parameter"
            ])
        writer.writerow([
            ts,
            record["center"],
            record["span"],
            record["start"],
            record["stop"],
            record["power"],
            record["sweeps"],
            record["points"],
            record["ifbw"],
            record["parameter"]
        ])
    print(f"Radar configuration for {ts} saved to {output_file}")


for ts in timestamps:
    save_radar_configuration_csv(ts)