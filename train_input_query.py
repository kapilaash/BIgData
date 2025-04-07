from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import csv

# Configuration for your local InfluxDB
url = "http://192.168.2.15:8086"
username = "admin"
password = "supersecurepassword"
org = "home"
bucket_name = "AMOD"

# Create client
client = InfluxDBClient(
    url=url,
    username=username,
    password=password,
    org=org,
    timeout=30_000
)

try:
    # Query API instance
    query_api = client.query_api()
    
    # Flux query to fetch all data from the 'train' measurement
    query = f'''
    from(bucket: "{bucket_name}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "train")
      |> filter(fn: (r) => r._field == "sales")
    '''
    
    # Execute the query
    result = query_api.query(query, org=org)
    
    # Write results to CSV
    with open("exported_train.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(["store", "item", "sales", "date"])
        
        # Iterate over results and write rows
        count = 0
        for table in result:
            for record in table.records:
                # Extract data from each record
                store = record.values.get("store")
                item = record.values.get("item")
                sales = int(record.values.get("_value"))  # Ensure integer sales
                date = record.get_time().strftime("%Y-%m-%d")  # Format date
                writer.writerow([store, item, sales, date])
                count += 1
        
        print(f"Exported {count} records to exported_train.csv")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    client.close()