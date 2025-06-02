
import io
import json
import re

from contextlib import asynccontextmanager

from pydantic import BaseModel
from mqtt_bridge_server_backup import mqtt_data_uploader_t

import rclpy
import threading
    
from fastapi.responses import Response
from fastapi import FastAPI
import csv
from typing import Any, Dict, List, Tuple

from influxdb_client import InfluxDBClient
from influxdb_client import QueryApi 

# Set your InfluxDB parameters
url = "http://localhost:8086"
token = "e_astaAaEQv7mtvrviX6dLAFjWPYUlUs14GiENyOqyk9uwjfHLHAbfk0E_SUJKDQy5Gg3WE5YpmgmYbR4ceCFg=="
org = "CDEI-UPC"
bucket = "ROS2DATA"

# Create a client
client: InfluxDBClient = None
query_api: QueryApi = None

node: mqtt_data_uploader_t = None  # Global node reference

limitation = 10

@asynccontextmanager
async def lifespan(app: FastAPI):
    
    global node
    global client
    global query_api
    
    # ROS 2 init
    rclpy.init()
    
    node = mqtt_data_uploader_t("192.168.13.26")
    client =  InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()
    # Optionally spin ROS node in background if needed
    spin_thread = threading.Thread(target=rclpy.spin, args=(node,), daemon=True)
    spin_thread.start()

    yield  # FastAPI starts serving after this line

    # On shutdown
    node.mqtt_client_robot.disconnect()
    node.get_logger().info("Disconnected from MQTT broker.")
    node.destroy_node()
    rclpy.shutdown()

# Attach lifespan to the FastAPI app
app = FastAPI(lifespan=lifespan)

# Precompile regex patterns once
_ndvi_pattern = re.compile(r"^ndvi_data_(\d+)$")
_temp_pattern = re.compile(r"^temperature_data_(\d+)$")

def extract_sorted_values_by_prefix(data: Dict[str, Any], pattern: re.Pattern) -> List[Any]:
    
    matched_items: List[Tuple[int, str, Any]] = [
        (int(m.group(1)), key, data[key])
        for key in data
        if (m := pattern.match(key)) and data.get(key) is not None
    ]
    matched_items.sort(key=lambda x: x[0])
    return [value for _, _, value in matched_items]

class DataEntry(BaseModel):
    timestamp: float
    latitude: float
    longitude: float
    altitude: float
    ndvi_data: List[float]
    temperature_data: List[float]

# Ultra sanitized data because Influx sucks
def get_influx_data():
    flux_query = f'''
    from(bucket: "{bucket}")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "mqtt_consumer") 
    |> pivot(
        rowKey: ["_time"],
        columnKey: ["_field"],
        valueColumn: "_value")
    |> limit(n: {limitation})'''
    
    query_result = query_api.query(flux_query)
    
    data = []
    
    for table in query_result:
        for record in table.records:
            row = record.values
            
            entry = {
                "timestamp": row.get("timestamp"),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "altitude": row.get("altitude"),
                "ndvi_data": extract_sorted_values_by_prefix(row, _ndvi_pattern),
                "temperature_data": extract_sorted_values_by_prefix(row, _temp_pattern)
            }
            
            data.append(entry)
    
    return data

@app.get("/robot_data_01/json", response_model=List[DataEntry])
async def robot_data_01_json() -> Dict[str, Any]:
    return get_influx_data()
    
@app.get("/robot_data_01/csv")
async def robot_data_01_csv():
    data = get_influx_data()
    
    if not data:
        return Response(content="No data available", media_type="text/plain", status_code=204)
    
    headers = ['timestamp', 'latitude', 'longitude', 'altitude', 'ndvi_data', 'temperature_data']
    
    output = io.StringIO()
    
    writer = csv.DictWriter(output, fieldnames=headers)
    
    writer.writeheader()
    
    for entry in data:
        # Serialize the list fields as JSON strings so they fit nicely in CSV cells
        row = {
            'timestamp': entry['timestamp'],
            'latitude': entry['latitude'],
            'longitude': entry['longitude'],
            'altitude': entry['altitude'],
            'ndvi_data': json.dumps(entry['ndvi_data']),
            'temperature_data': json.dumps(entry['temperature_data'])
        }
        writer.writerow(row)

    return Response(content=output.getvalue(), media_type="text/csv")
