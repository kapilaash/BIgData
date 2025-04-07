from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import csv

# Configuration for your local InfluxDB
url = "http://192.168.2.15:8086"
username = "admin"
password = "supersecurepassword"
org = "home"  # Default organization in new installations
bucket_name = "AMOD"
measurement_name = "training"

# Create client
client = InfluxDBClient(
    url=url,
    username=username,
    password=password,
    org=org,
    timeout=30_000
)

try:
    # Verify bucket exists
    buckets_api = client.buckets_api()
    if not buckets_api.find_bucket_by_name(bucket_name):
        raise Exception(f"Bucket '{bucket_name}' does not exist")

    # Prepare write client
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Process CSV with sales data
    with open("train.csv") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            point = {
                "measurement": measurement_name,
                "tags": {
                    "store": row["store"],
                    "item": row["item"]
                },
                "fields": {
                    "sales": int(row["sales"])  # Convert sales to integer
                },
                "time": f"{row['date']}T00:00:00Z"  # Add time component
            }
            write_api.write(bucket=bucket_name, record=point)
    
    print(f"Successfully imported {csv_reader.line_num - 1} sales records!")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    client.close()