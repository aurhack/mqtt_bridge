
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
        
    # Publishes the given data to the specified MQTT topic.
    # def publish(self, topic: str, data: dict) -> None:
    #     if data is not None:
    #         payload = json.dumps(data, indent=4)
    #         self.mqtt_client_server.publish(topic, payload)
    #         self.get_logger().info(f"Published to MQTT Topic -> {topic} the data : {payload}")
    
    # Robot Message
    def robot_message_01(self) -> str:
        
        def organized_and_unique(sequence): 
            return list(OrderedDict.fromkeys(sequence))

        rd_handler = self.ros_data
        accumulated_ndvi_data = {"ndvi": {}}
        
        while True:
            
            data_samples = []
            start_time = time.time()
            finish_line = 1
                
            while (int(time.time() - start_time)) <= finish_line:
                
                new_ndvi_data = rd_handler.get(msg_type.NDVI, False, True)
                
                if new_ndvi_data is not None:
                    data_samples.append(new_ndvi_data["ndvi"])
            
            # Get all the data of the second on a organized and unique way
            # After doing the last step, add it to the dict for the json
            accumulated_ndvi_data["ndvi"] = [data_sample for data_sample in organized_and_unique(data_samples) if data_sample is not None]
                    
            # Prepare for next json
            data_samples.clear()

            def get_last_id():
                last_doc = self.collection.find_one(sort=[("_id", -1)])
                return last_doc["_id"] + 1 if last_doc else 1

            custom_json = {
                
                "_id": get_last_id(),
                
                "robot_data": {
                    **rd_handler.g_timestamp.json,
                    **rd_handler.get(msg_type.GPS),
                    **rd_handler.t_temperature.json,
                    **accumulated_ndvi_data
                    }
                }
            
            #print("inserted a record!\n")
            #self.collection.insert_one(custom_json)
            
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

# Ensure script can be run standalone
# if __name__ == '__main__':
#     main()

# ---------------------- WHOLE SOURCE ENDS HERE ----------------------
