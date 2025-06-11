import json
import math
import paho.mqtt.client as mqtt
from  paho.mqtt.client import MQTTMessage
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from ros_data import ros_data_t
import rclpy
from rclpy.node import Node

from dataclasses import dataclass
from enum import Enum
import threading
import time

# MQTT Specs
MQTT_DEFAULT_HOST = "localhost"
MQTT_DEFAULT_PORT = 1883
MQTT_DEFAULT_TIMEOUT = 120

# Configuration -- START

# MQTT topics where the data is being sent to
JSON_GLOBAL_TOPIC = "global/json"

# MQTT Topics this node listens to

MQTT_GLOBAL_TOPIC = "mqtt/global"

# Configuration -- END

# ros_data_t msg types
class msg_type(Enum):
    GPS = 0
    TEMPERATURE = 1
    NDVI = 2

# ---------------------- WHOLE SOURCE STARTS HERE ----------------------

# SUMMARY -- START

"""

SERVERSIDE - THIS SCRIPT MUST BE EXECUTED ON THE SERVER!!!

This code subscribes to specified MQTT topics and publishes the received data. 
It includes predefined callback functions that intercept the incoming messages.

Upon interception, the messages are processed and transferred to a 
global data class, making the data available in real-time.

This approach allows the generation of custom JSON data containing values 
from the subscribed topics, providing flexibility in selecting
the specific information to be made available.

"""

# SUMMARY -- END

# A helper class to store and update data from ROS messages.
# Main class for the ROS 2 node that publishes received MQTT messages to an MQTT broker.
# After that, we just listen with Telegraf and InfluxDB processes everything
class mqtt_data_uploader_t(Node):
    
    # Initialize the ROS 2 node, instantiate shared data container,
    # subscribe to topics, and connect to MQTT broker.
    def __init__(self, host=MQTT_DEFAULT_HOST, port=MQTT_DEFAULT_PORT):
        super().__init__('ros2_mqtt_publisher')

        # Instantiate shared data container
        self.ros_data = ros_data_t()

        # Initialize and connect the MQTT client
        self.mqtt_client_robot = mqtt.Client()
        self.mqtt_client_server = mqtt.Client()
        
        try:
            
            # Hard coded for now, don't blame 
            self.client = MongoClient("mongodb://admin:cdei2025@192.168.13.106:27017/", server_api=ServerApi('1'))
            self.db = self.client["ROS2"]
            self.collection = self.db["General"]
            
            # Connect to Mongo DB Client
            self.get_logger().info(f"Connected to MongoDB..")
            
            # We use our client to publish the data and we use the robot client to collect it.
            self.mqtt_client_server.connect(MQTT_DEFAULT_HOST, MQTT_DEFAULT_PORT, MQTT_DEFAULT_TIMEOUT)
            self.get_logger().info(f"Connected to our MQTT client")
            
            self.mqtt_client_robot.connect(host, port, MQTT_DEFAULT_TIMEOUT)
            self.get_logger().info(f"Connected to Foreign MQTT broker at {host}:{port}")
            
            # Subscribe to required MQTT topics
            self.subscribe_to_topics()
        
            thread = threading.Thread(target=self.robot_message_01)
            thread.start()
            
        except Exception as e:
            self.get_logger().error(f"Failed to connect to MQTT broker: {e}")
            
        # Start MQTT client loop
        self.mqtt_client_robot.loop_start()
        self.mqtt_client_server.loop_start()

    # Sets up ROS topic subscriptions with appropriate message types and callbacks.
    def subscribe_to_topics(self) -> None:
        self.mqtt_client_robot.subscribe(MQTT_GLOBAL_TOPIC, qos=0)                       
        
        self.get_logger().info(f"Subscribed to the robot topics")
        
        # Let the robot client get the callback so we can get the values from the topics
        # we got subscribed to
        self.mqtt_client_robot.on_message = self.on_mqtt_message
    
    # This is the main callback to get all the data from the subscribed topics
    def on_mqtt_message(self, client, userdata, msg: MQTTMessage):
    
        self.ros_data.update(json.loads(msg.payload.decode("utf-8")))
        
    # Publishes the given data to the specified MQTT topic.
    # Plus it sends it to MongoDB Insert..
    def publish(self, topic: str, data: dict) -> None:
        if data is not None:
            payload = json.dumps(data, indent=4)
            self.mqtt_client_server.publish(topic, payload)
            self.collection.insert_one(payload)
            self.get_logger().info(f"Published to MQTT Topic ({topic}) and MongoDB the data : {payload}")
            
    def robot_message_01(self):
        rd_handler = self.ros_data
        sampling_duration_sec = 1  # seconds per batch

        def manage_data(sample, samples: list):
            
            if sample is None:
                return
            
            if isinstance(sample, float) and math.isnan(sample):
                return
            
            if sample not in samples:
                samples.append(sample)

        while True:
            canopy_temperature_samples = []
            ndvi_samples = []
            ndvi_3d_samples = []
            ndvi_ir_samples = []
            ndvi_visible_samples = []

            start_time = time.time()

            while (time.time() - start_time) <= sampling_duration_sec:
                manage_data(rd_handler.t_canopy_temperature, canopy_temperature_samples)
                manage_data(rd_handler.n_ndvi, ndvi_samples)
                manage_data(rd_handler.n_ndvi_3d, ndvi_3d_samples)
                manage_data(rd_handler.n_ir, ndvi_ir_samples)
                manage_data(rd_handler.n_visible, ndvi_visible_samples)

            json_data = {
                **rd_handler.g_timestamp.json,
                **rd_handler.g_latitude.json,
                **rd_handler.g_longitude.json,
                **rd_handler.g_altitude.json,

                "canopy_temperature_data": canopy_temperature_samples,
                "ndvi_data": ndvi_samples,
                "ndvi_3d_data": ndvi_3d_samples,
                "ndvi_ir_data": ndvi_ir_samples,
                "ndvi_visible_data": ndvi_visible_samples,

                "environment_temperature": 0,
                "environment_humidity": 0,
                "sensor_orientation": 0,
                "robot_status": 0
            }

            # Replace the print with your actual publish function
        
            # Publish to both MongoDB a InfluxDB!!
            #self.publish(JSON_GLOBAL_TOPIC, json_data)
            print(json_data)
            input()  # Pause, remove or replace in production

            # Clearing lists is unnecessary here because you recreate them each loop
            # But if you want to clear before reuse, just do it once per outer loop iteration

def main(args=None):
    # Main entry point for running the ROS 2 node.
    rclpy.init(args=args)

    node = mqtt_data_uploader_t("192.168.13.26")

    try:
        rclpy.spin(node)  # Start processing callbacks
    except KeyboardInterrupt:
        pass
    finally:
        node.mqtt_client_robot.disconnect()
        node.mqtt_client_server.disconnect()
        node.get_logger().info("Disconnected from MQTT broker.")
        node.destroy_node()
        rclpy.shutdown()

# Ensure script can be run standalone
if __name__ == '__main__':
    main()

# ---------------------- WHOLE SOURCE ENDS HERE ----------------------
