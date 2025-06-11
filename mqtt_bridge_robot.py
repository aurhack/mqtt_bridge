import re
import json
import paho.mqtt.client as mqtt

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile

# If you want a new type choose it from here, with a comma,
# e.g 'from std_msgs.msg import String, Float32, Bool' 
from sensor_msgs.msg import NavSatFix
from std_msgs.msg import String, Float32

from enum import Enum

# Configuration -- START

# MQTT Specs
MQTT_DEFAULT_HOST = "localhost"
MQTT_DEFAULT_PORT = 1883
MQTT_DEFAULT_TIMEOUT = 120

# MQTT output topics where we can get the data easily formatted
ROS2MQTT_GPS_TOPIC = "mqtt/gps"
ROS2MQTT_TEMPERATURE_TOPIC = "mqtt/temperature"
ROS2MQTT_NDVI_TOPIC = "mqtt/ndvi"

# ROS2 Topics this node listens to
ROS2_TOPIC_GPS = "gps/fix"
ROS2_TOPIC_TEMPERATURE = "/Temperature_and_CSWI/text"
ROS2_TOPIC_NDVI = "/NDVI"

# Regex
GET_FLOAT_NUMBER = r'\d+\.\d+'
GET_TEMPERATURE_DATA = rf'([^":]+):|({GET_FLOAT_NUMBER})'

# ROS2 Message Types
class msg_type(Enum):
    GPS = 0
    TEMPERATURE = 1
    NDVI = 2

# Configuration -- END

# ---------------------- WHOLE SOURCE STARTS HERE ----------------------

# SUMMARY -- START

"""
CLIENTSIDE - THIS SCRIPT MUST BE EXECUTED ON THE ROBOT!!!

This code subscribes to specified ROS2 topics and publishes the received data. 
It includes predefined callback functions that intercept the incoming messages.
Upon interception, the messages are processed and transferred to the MQTT broker
"""

# SUMMARY -- END

# Main class for the ROS 2 node that publishes received ROS2 messages to an MQTT broker.
class ros2_mqtt_publisher_t(Node):
    
    # Initialize the ROS 2 node, instantiate shared data container,
    # subscribe to topics, and connect to MQTT broker.
    def __init__(self, host=MQTT_DEFAULT_HOST, port=MQTT_DEFAULT_PORT):
        super().__init__('ros2_mqtt_publisher')

        # Subscribe to required ROS topics
        self.subscribe_to_topics()

        # Initialize and connect the MQTT client
        self.mqtt_client = mqtt.Client()
        
        try:
            self.mqtt_client.connect(host, port, MQTT_DEFAULT_TIMEOUT)
            self.get_logger().info(f"Connected to MQTT broker at {host}:{port}")
            
        except Exception as e:
            self.get_logger().error(f"Failed to connect to MQTT broker: {e}")

        # Start MQTT client loop
        self.mqtt_client.loop_start()

    # Sets up ROS topic subscriptions with appropriate message types and callbacks.
    def subscribe_to_topics(self) -> None:
        qos = QoSProfile(depth=10)
        self.create_subscription(NavSatFix, ROS2_TOPIC_GPS, self.gps_callback, qos)
        self.create_subscription(String, ROS2_TOPIC_TEMPERATURE, self.temperature_callback, qos)
        self.create_subscription(String, ROS2_TOPIC_NDVI, self.ndvi_callback, qos)
   
    # Publishes the given data to the specified MQTT topic.
    def publish(self, topic: str, data: dict) -> None:
        if data is not None:
            payload = json.dumps(data)
            self.mqtt_client.publish(topic, payload)
            self.get_logger().info(f"Published to MQTT: {payload}")
        
    def warn_malformed_data(self, data_type): 
        return self.get_logger().warn(f"Malformed {data_type} data received")

    # Callback for GPS data; parses NavSatFix and sends it to MQTT.
    def gps_callback(self, msg: NavSatFix):
        
        # Make a dictionary with the data we want
        sanitized_data = {
            # We get the timestamp specifically from GPS for real time position precision
            "timestamp": msg.header.stamp.sec,
            # ~!!!
            
            "latitude": msg.latitude,
            "longitude": msg.longitude,
            "altitude": msg.altitude
        }
        
        self.publish(ROS2MQTT_GPS_TOPIC, sanitized_data)

    # Callback for temperature and CWSI data, expected in a formatted string.
    def temperature_callback(self, msg: String):
        
        # Gets the specified data with a regular expression
        found_data_serialized = [x for tup in re.findall(GET_TEMPERATURE_DATA, msg.data) for x in tup if x]
        
        # Checks if the serialized data is healthy, otherwise we do not publish 
        if len(found_data_serialized) >= 2:
            try:
                sanitized_data = {
                    "entity_count": found_data_serialized[0],
                    "canopy_temperature": float(found_data_serialized[1]),
                    "cwsi": float(found_data_serialized[2])
                }
                
                self.publish(ROS2MQTT_TEMPERATURE_TOPIC, sanitized_data)
            
            except ValueError as e: self.get_logger().warn(f"Invalid data format: {e}")
        else: self.warn_malformed_data("temperature")

    # Callback for NDVI data, on float32
    def ndvi_callback(self, msg: Float32):
        
        # We get the float values with regex
        found_data = re.findall(GET_FLOAT_NUMBER, msg.data)
        
        # Check if it's healthy
        if len(found_data) <= 4:
            try:
                sanitized_data = {
                    "ndvi": float(found_data[0]),
                    "ndvi_3d": float(found_data[1]),
                    "ir": float(found_data[2]),
                    "visible": float(found_data[3])
                }
                
                self.publish(ROS2MQTT_NDVI_TOPIC, sanitized_data)
                
            except ValueError as e: self.get_logger().warn(f"Invalid data format: {e}")
        else: self.warn_malformed_data("NDVI")

def main(args=None):
    # Main entry point for running the ROS 2 node.
    rclpy.init(args=args)

    node = ros2_mqtt_publisher_t()

    try:
        rclpy.spin(node)  # Start processing callbacks
    except KeyboardInterrupt:
        pass
    finally:
        node.mqtt_client.disconnect()
        node.get_logger().info("Disconnected from MQTT broker.")
        node.destroy_node()
        rclpy.shutdown()

# Ensure script can be run standalone
if __name__ == '__main__':
    main()

# ---------------------- WHOLE SOURCE ENDS HERE ----------------------