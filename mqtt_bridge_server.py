import json
import paho.mqtt.client as mqtt
from  paho.mqtt.client import MQTTMessage
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from mqtt_bridge_utils import json_wrapper_t
import rclpy
from rclpy.node import Node

from dataclasses import dataclass
from enum import Enum
import threading
import time

from dataclasses import dataclass, field
from typing import Any, Dict

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
@dataclass
class ros_data_t:
    
    # Raw fields (auto-wrapped via __getattribute__)
    
    g_timestamp: int = None 
    g_latitude: float = None
    g_longitude: float = None
    g_altitude: float = None

    t_entity_count: str = None
    t_canopy_temperature: float = None
    t_cswi: float = None

    n_ndvi: float = None
    n_ndvi_3d: float = None
    n_ir: float = None
    n_visible: float = None

    # Internal change flags
    _changed_flags: Dict[str, bool] = field(default_factory=lambda: {
        "gps": False,
        "temperature": False,
        "ndvi": False
    }, init=False)

    def update(self, data: dict):
        msg = data["msg_type"]
        if msg == msg_type.GPS:
            self.g_timestamp = data["timestamp"]
            self.g_latitude = data["latitude"]
            self.g_longitude = data["longitude"]
            self.g_altitude = data["altitude"]
            self._changed_flags["gps"] = True

        elif msg == msg_type.TEMPERATURE:
            self.t_entity_count = data["entity_count"]
            self.t_canopy_temperature = data["canopy_temperature"]
            self.t_cswi = data["cwsi"]
            self._changed_flags["temperature"] = True

        elif msg == msg_type.NDVI:
            self.n_ndvi = data["ndvi"]
            self.n_ndvi_3d = data["ndvi_3d"]
            self.n_ir = data["ir"]
            self.n_visible = data["visible"]
            self._changed_flags["ndvi"] = True

        else:
            raise ValueError(f"Unsupported msg_type: {msg}")

    def __getattribute__(self, name: str) -> Any:
        value = super().__getattribute__(name)
        if name.startswith('_') or callable(value):
            return value
        return json_wrapper_t(name[2:], value)

    def get(self, type, with_key=True, only_if_changed=False):
        config = {
            msg_type.GPS: ("gps", [self.g_latitude.json, self.g_longitude.json, self.g_altitude.json]),
            msg_type.TEMPERATURE: ("temperature", [self.t_entity_count.json, self.t_canopy_temperature.json, self.t_cswi.json]),
            msg_type.NDVI: ("ndvi", [self.n_ndvi.json, self.n_ndvi_3d.json, self.n_ir.json, self.n_visible.json])
        }

        if type not in config:
            raise ValueError("Unsupported message type")

        key, json_parts = config[type]

        # Return nothing if not changed (and only_if_changed=True)
        if only_if_changed and not self._changed_flags[key]:
            return None

        # Reset the changed flag after fetching
        self._changed_flags[key] = False

        merged_data = {}
        
        for part in json_parts:
            merged_data.update(part)

        return {key: merged_data} if with_key else merged_data

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
            #soon threads here 
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
            
    # Continuous Robot Message Thread Loop
    def robot_message_01(self):
        
        rd_handler = self.ros_data
        sampling_duration_sec = 1  # seconds per batch
    
        def manage_data(sample: dict, data_to_get, samples: list):
            if sample:
                
                sample_to_add = sample.get(data_to_get)
                
                if sample_to_add is not None and sample_to_add not in samples:
                    samples.append(sample_to_add)
                            
        while True:
            
            canopy_temperature_samples = []
            ndvi_samples = []
            ndvi_3d_samples = []
            
            with_key = False
            only_if_changed = True
            
            start_time = time.time()
            
            while (time.time() - start_time) <= sampling_duration_sec:
                
                manage_data(rd_handler.get(msg_type.TEMPERATURE, with_key, only_if_changed), "canopy_temperature", canopy_temperature_samples)
                manage_data(rd_handler.get(msg_type.NDVI, with_key, only_if_changed), "ndvi", ndvi_samples)
                manage_data(rd_handler.get(msg_type.NDVI, with_key, only_if_changed), "ndvi_3d", ndvi_3d_samples)
                    
            # Build the JSON for Mosquitto
            json_data = {
                **rd_handler.g_timestamp.json,
                **rd_handler.get(msg_type.GPS, with_key),
                "canopy_temperature_data": canopy_temperature_samples,
                "ndvi_data": ndvi_samples,
                "ndvi_3d_data": ndvi_3d_samples,
                
                #non-categorized (temporal)
                "environment_temperature": 0,
                "environment_humidity": 0,
                "sensor_orientation": 0,
                "robot_status": 0
                }
            
            # Publish to both MongoDB a InfluxDB!!
            self.publish(JSON_GLOBAL_TOPIC, json_data)
            
            ndvi_samples.clear()
            canopy_temperature_samples.clear()
            ndvi_3d_samples.clear()

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
