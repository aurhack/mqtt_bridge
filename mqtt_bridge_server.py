
import paho.mqtt.client as mqtt
from  paho.mqtt.client import MQTTMessage

import rclpy
from rclpy.node import Node

from collections import OrderedDict

import threading
import time
import json

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from ros_data import msg_type, ros_data_t

# Configuration -- START

# MQTT Specs
MQTT_DEFAULT_HOST = "localhost"
MQTT_DEFAULT_PORT = 1883
MQTT_DEFAULT_TIMEOUT = 120

# MQTT topics where the data is being sent to
JSON_GPS_TOPIC = "gps/json"
JSON_TEMPERATURE_TOPIC = "temperature/json"
JSON_NDVI_TOPIC = "ndvi/json"

# MQTT Topics this node listens to
MQTT_TOPIC_GPS = "mqtt/gps"
MQTT_TOPIC_TEMPERATURE = "mqtt/temperature"
MQTT_TOPIC_NDVI = "mqtt/ndvi"

# Configuration -- END

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
        
        try:
            
            # Hard coded for now, don't blame 
            self.client = MongoClient("mongodb://localhost:27017/", server_api=ServerApi('1'))
            self.db = self.client["ROS2"]
            self.collection = self.db["General"]

            self.mqtt_client_robot.connect(host, port, MQTT_DEFAULT_TIMEOUT)
            self.get_logger().info(f"Connected to Foreign MQTT broker at {host}:{port}")
            
            # Subscribe to required MQTT topics of the robot
            self.subscribe_to_topics()
            
            thread = threading.Thread(target=self.robot_message_01)
            thread.start()
            
        except Exception as e:
            self.get_logger().error(f"Failed to connect to MQTT broker: {e}")

        # Start MQTT client loop
        self.mqtt_client_robot.loop_start()

    # Sets up ROS topic subscriptions with appropriate message types and callbacks.
    def subscribe_to_topics(self) -> None:
        qos = 0
        self.mqtt_client_robot.subscribe(MQTT_TOPIC_GPS, qos)
        self.mqtt_client_robot.subscribe(MQTT_TOPIC_TEMPERATURE, qos)  
        self.mqtt_client_robot.subscribe(MQTT_TOPIC_NDVI, qos)                       
        
        # Let the robot client get the callback so we can get the values from the topics
        # we got subscribed to
        self.mqtt_client_robot.on_message = self.on_mqtt_message
    
    # This is the main callback to get all the data from the subscribed topics
    def on_mqtt_message(self, client, userdata, msg: MQTTMessage):
        self.ros_data.update({"msg_type": self.get_msg_type_by_topic(msg.topic), **json.loads(msg.payload.decode("utf-8"))})
    
    def get_msg_type_by_topic(self, topic_name) -> msg_type:
        return {
            MQTT_TOPIC_GPS: msg_type.GPS,
            MQTT_TOPIC_TEMPERATURE: msg_type.TEMPERATURE,
            MQTT_TOPIC_NDVI: msg_type.NDVI}.get(topic_name)
        
    # Continuous Robot Message Thread Loop
    def robot_message_01(self):
        
        rd_handler = self.ros_data
        samples_needed = 20
        sampling_duration_sec = 1  # seconds per batch

        def get_last_id():
            last_doc = self.collection.find_one(sort=[("_id", -1)])
            return last_doc["_id"] + 1 if last_doc else 1
    
        def manage_data(sample: dict, data_to_get, samples: list):
            if sample:
                
                sample_to_add = sample.get(data_to_get)
                
                if sample_to_add is not None and sample_to_add not in samples:
                    samples.append(sample_to_add)
                            

        while True:
            
            ndvi_samples = []
            temperature_samples = []
            start_time = time.time()
            
            while (time.time() - start_time) <= sampling_duration_sec:
                
                manage_data(rd_handler.get(msg_type.NDVI, False, True), "ndvi", ndvi_samples)
                manage_data(rd_handler.get(msg_type.TEMPERATURE, False, True), "temperature", temperature_samples)
                    
            # Build the JSON for MongoDB
            json_data = {
                "_id": get_last_id(),
                "robot_data": {
                    **rd_handler.g_timestamp.json,
                    **rd_handler.get(msg_type.GPS),
                    "temperature_data": temperature_samples,
                    "ndvi_data": ndvi_samples
                }
            }

            # # Insert into MongoDB
            # self.collection.delete_many({})
            # self.collection.insert_one(json_data)
            # print("Inserted new robot data : ", json_data["_id"])
            
            ndvi_samples.clear()
            temperature_samples.clear()
            
### For individual Testing ###
       
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
        node.get_logger().info("Disconnected from MQTT broker.")
        node.destroy_node()
        rclpy.shutdown()

## Ensure script can be run standalone
if __name__ == '__main__':
    main()

# ---------------------- WHOLE SOURCE ENDS HERE ----------------------
