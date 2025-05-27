
from datetime import datetime, timezone
import io
from fastapi import FastAPI
from contextlib import asynccontextmanager
from mqtt_bridge_server import mqtt_data_uploader_t
import rclpy
import threading
    
from fastapi.responses import Response
import csv
import io
import bson.json_util as utils

node: mqtt_data_uploader_t = None  # Global node reference

@asynccontextmanager
async def lifespan(app: FastAPI):
    
    global node
    
    # ROS 2 init
    rclpy.init()
    
    node = mqtt_data_uploader_t("192.168.13.26")

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

@app.get("/robot_data_01/json")
async def robot_data_01_json():
    return Response(utils.dumps(node.collection.find().limit(400).to_list()), media_type="application/json")

@app.get("/robot_data_01/csv")
async def robot_data_01_csv():
    
    # Flattening function
    def flatten_document(doc):
        robot_data = doc["robot_data"]
        gps = robot_data["gps"]
        
        flat_doc = {
            "_id": str(doc["_id"]),
            "timestamp": robot_data["timestamp"],
            "latitude": gps["latitude"],
            "longitude": gps["longitude"],
            "altitude": gps["altitude"],
            "temperature": robot_data["temperature"]
        }
        
        for i, sample in enumerate(robot_data["ndvi"]):
            flat_doc[f"ndvi_data_{i}"] = sample
            
        return flat_doc

    # Get documents (await!)
    docs = node.collection.find().to_list()  # or any suitable length
    flat_docs = [flatten_document(doc) for doc in docs]

    if not flat_docs:
        return Response(content="No data", media_type="text/plain")

    # Write CSV to memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=flat_docs[0].keys())
    writer.writeheader()
    writer.writerows(flat_docs)

    # Return as CSV response
    return Response(content=output.getvalue(), media_type="text/csv")
