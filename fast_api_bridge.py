
from fastapi import FastAPI
from contextlib import asynccontextmanager
from mqtt_bridge_server import mqtt_data_uploader_t
import rclpy
import threading

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

@app .get("/robot_data_01")
async def robot_data_01():
    return list(node.collection.find())
