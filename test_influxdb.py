from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone

# InfluxDB Cloud setup
INFLUXDB_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com/"
INFLUXDB_TOKEN = "EGlGbNeY78sXhQIuGwFYsvo3KMQ9zP659p27SLLPYFeqN37_ln7Vi8G_IK92irox-xEQpc6Tr1r8gHHqQ8JJeg=="
INFLUXDB_ORG = "orgs"
INFLUXDB_BUCKET = "Iot_data"

# Create InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

# Test writing a data point
write_api = client.write_api(write_options=SYNCHRONOUS)

try:
    # Create a data point
    point = (
        Point("test_measurement")
        .tag("location", "office")
        .field("temperature", 23.5)
        .time(datetime.now(timezone.utc))  # Use timezone-aware UTC time
    )

    # Write data to InfluxDB bucket
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    print("Data written successfully to InfluxDB.")

    # Test querying data
    query_api = client.query_api()
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "test_measurement")
    '''
    tables = query_api.query(query, org=INFLUXDB_ORG)

    print("Querying data from InfluxDB:")
    for table in tables:
        for record in table.records:
            print(f"Time: {record.get_time()}, Measurement: {record.get_measurement()}, Value: {record.get_value()}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    client.close()
