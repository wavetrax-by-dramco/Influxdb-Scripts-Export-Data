from influxdb_client import InfluxDBClient
from helper.configurations import *
from datetime import datetime

config = retrieve_yaml_file()

# Configuratie
bucket = config["influxdb"]["bucket"]
org = config["influxdb"]["org"]
token = config["influxdb"]["read-token"]
url = config["influxdb"]["url"]

radar_name = "C002"
time_range_start = "2025-05-07T10:00:00Z" # UTC TIME !!!
time_range_stop = "2025-05-28T12:00:00Z" # UTC TIME !!!

# Connectie
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

# Flux-query om alleen timestamps op te halen
query = f'''
from(bucket: "{bucket}")
  |> range(start: {time_range_start}, stop: {time_range_stop})
  |> filter(fn: (r) => r._measurement == "radar_measurement")
  |> filter(fn: (r) => r["radar"] == "{radar_name}")
  |> keep(columns: ["_time"])
'''

result = query_api.query(org=org, query=query)

timestamps = [row.get_time().isoformat() for table in result for row in table.records]
# print(timestamps)

# Uniek maken en sorteren
unique_sorted = sorted(set(timestamps), key=lambda x: datetime.fromisoformat(x))

for ts in unique_sorted:
    print(ts)

print(len(unique_sorted))

exit()

# Uitvoeren en timestamps verzamelen
result = query_api.query(org=org, query=query)

for table in result:
    for row in table.records:
        print(row.values)  # ← debug

timestamps = [row.values["_time"] for table in result for row in table.records]

# Weergeven
print("Timestamps van metingen binnen de tijdsrange:")
for ts in timestamps:
    print(ts)