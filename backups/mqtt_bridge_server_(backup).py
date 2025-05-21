import json
import paho.mqtt.client as mqtt
from  paho.mqtt.client import MQTTMessage

import rclpy
from rclpy.node import Node

from dataclasses import dataclass
from collections import OrderedDict, defaultdict
from enum import Enum
import threading
import time

# Configuration -- START

# MQTT Specs
MQTT_DEFAULT_HOST = "localhost"
MQTT_DEFAULT_PORT = 1883
MQTT_DEFAULT_TIMEOUT = 120

# MQTT topics where the data is being sent to
JSON_GPS_TOPIC = "gps/json"
JSON_TEMPERATURE_TOPIC = "temperature/json"
JSON_NDVI_TOPIC = "ndvi/json"

JSON_ROBOT_DATA_01 = "robot/json"

# MQTT Topics this node listens to
MQTT_TOPIC_GPS = "mqtt/gps"
MQTT_TOPIC_TEMPERATURE = "mqtt/temperature"
MQTT_TOPIC_NDVI = "mqtt/ndvi"

# ros_data_t msg types
class msg_type(Enum):
    GPS = 0
    TEMPERATURE = 1
    NDVI = 2

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

# This class helps on the access fast deliver members in a json-compatible format  
class json_wrapper_t:
    
    def __init__(self, name, value):
        self._name = name
        self._value = value
    
    # To be able to remove 'outer' braces, so we can do :
    # class.member
    # class.member.json
    
    # Without back and forth braces
    @property
    def json(self):
            return dict({self._name : self._value})

    def __getattr__(self, attr): 
        return getattr(self._value, attr)

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return repr(self._value)

    def __int__(self):
        return int(self._value)

    def __float__(self):
        return float(self._value)

    def __eq__(self, other):
        return self._value == other

    def __add__(self, other):
        return self._value + other

    @property
    def value(self):
        return self._value

# Class made for hashing purposes, to get unique and ordered data inside
# it's not necessary to create a class for each topic, we could too, but implementation
# could be harder and maybe even complex, we need to keep the code clean and faster

# IMPORTANT : This is just a helper class, is not meant to work on a native case 
# it doesn't support a normal constructor based on the normal functionality of the whole source
# It's made for 'robot_message_01'
class ndvi_data_hashable_t:

    def __init__(self, json_data:json):
        self.ndvi = json_data["ndvi"]
        self.ndvi_3d = json_data["ndvi_3d"]
        self.ir = json_data["ir"]
        self.visible = json_data["visible"]
        
    def __eq__(self, other):
        if not isinstance(other, ndvi_data_hashable_t):
            return False
        return (self.ndvi == other.ndvi and
                self.ndvi_3d == other.ndvi_3d and
                self.ir == other.ir and
                self.visible == other.visible)
        
    def to_json(self):
        return {"ndvi":self.ndvi,
                           "ndvi_3d":self.ndvi_3d,
                           "ir":self.ir,
                           "visible":self.visible}

    def __hash__(self):
        return hash((self.ndvi, self.ndvi_3d, self.ir, self.visible))
    
    def __iter__(self):
       yield self.ndvi
       yield self.ndvi_3d
       yield self.ir
       yield self.visible


# A helper class to store and update data from ROS messages.
from dataclasses import dataclass, field
from typing import Any, Dict
from collections import OrderedDict

@dataclass
class ros_data_t:
    # Raw fields (auto-wrapped via __getattribute__)
    g_timestamp: int = None 
    g_latitude: float = None
    g_longitude: float = None
    g_altitude: float = None

    t_entity_count: str = None
    t_temperature: float = None
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
            self.t_temperature = data["temperature"]
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
            msg_type.TEMPERATURE: ("temperature", [self.t_entity_count.json, self.t_temperature.json, self.t_cswi.json]),
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
    def publish(self, topic: str, data: dict) -> None:
        if data is not None:
            payload = json.dumps(data, indent=4)
            self.mqtt_client_server.publish(topic, payload)
            self.get_logger().info(f"Published to MQTT Topic -> {topic} the data : {payload}")
    
    # Robot Message
    def robot_message_01(self) -> str:
        
        def count_repeats(sequence):
            counts = OrderedDict()
            for item in sequence:
                counts[item] = counts.get(item, 0) + 1
            return counts

        rd_handler = self.ros_data
        accumulated_ndvi_data = {"analyzed_ndvi_data": {}}
        
        # Before analysis snapshot
        accumulated_ndvi_data["analyzed_ndvi_data"]["before_analysis"] = rd_handler.get(msg_type.NDVI, False)
        
        second_count = 1
        last_logged_second = 0
        data_samples = []
        start_time = time.time()
        finish_line = 5
        data_per_second = 10

        while (time_passed := int(time.time() - start_time)) <= finish_line:
            
            new_ndvi_data = rd_handler.get(msg_type.NDVI, False, True)
            
            if new_ndvi_data is not None and len(data_samples) < data_per_second:
                data_samples.append(ndvi_data_hashable_t(new_ndvi_data))
            
            if time_passed > last_logged_second:
                
                # Process collected data samples for the past second
                counted_samples = count_repeats(data_samples)
                sec_key = f"second_{second_count}"
                final_result_dict = {}

                for idx, (data_sample, repeats) in enumerate(counted_samples.items(), start=1):
                    final_sample_data_result = {"data" : data_sample.to_json()}
                    
                    if repeats > 1:
                        final_sample_data_result["repeated"] = repeats
                        
                    final_result_dict[f"sample_n_{idx}"] = final_sample_data_result

                accumulated_ndvi_data["analyzed_ndvi_data"][sec_key] = final_result_dict
                
                # Prepare for next second
                data_samples.clear()
                second_count += 1
                last_logged_second = time_passed

        # After analysis snapshot
        accumulated_ndvi_data["analyzed_ndvi_data"]["after_analysis"] = rd_handler.get(msg_type.NDVI, False)
        
        custom_json = {
            "robot_data": {
                **rd_handler.g_timestamp.json,
                **rd_handler.get(msg_type.GPS),
                **rd_handler.t_temperature.json,
                **accumulated_ndvi_data
            }
        }

        #print(json.dumps(custom_json, indent=4))

        self.publish(JSON_ROBOT_DATA_01, custom_json)

    # The looper function will be the main stream.
    # Where all the json data is being published
    # def data_looper(self) -> None:
        
    #     while True:
            
    #         self.publish(JSON_ROBOT_DATA_01, self.robot_message_01())

    #         if DEFAULT_PUBLISH:
    #             self.publish(JSON_GPS_TOPIC, self.data_bulk[msg_type.GPS.value])
    #             self.publish(JSON_TEMPERATURE_TOPIC, self.data_bulk[msg_type.TEMPERATURE.value])
    #             self.publish(JSON_NDVI_TOPIC, self.data_bulk[msg_type.NDVI.value])

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
