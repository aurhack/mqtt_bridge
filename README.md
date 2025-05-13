# ROS 2 to MQTT Integration Explanation

## Overview

This Python script is designed to run on a robot (client-side) and a server-side system. It facilitates communication between ROS 2 and an MQTT broker by subscribing to various ROS 2 topics, extracting specific data, and publishing that data to MQTT topics. The goal is to provide real-time data exchange between the robot (which generates the data) and the server (which processes and stores it).

## ROS 2 Node (Client-Side)

The client-side script subscribes to ROS 2 topics (GPS, Temperature, NDVI), processes incoming data, and publishes it to an MQTT broker.

### Key Components

- **ROS 2 Node (`ros2_mqtt_publisher_t`)**: This class represents the ROS 2 node that subscribes to specified ROS topics (GPS, Temperature, NDVI). It also manages MQTT client connections and data publishing.
  
- **Subscriptions**: The node subscribes to three ROS 2 topics:
  - `gps/fix`: GPS data
  - `/Temperature_and_CSWI/text`: Temperature data (with CWSI)
  - `/NDVI`: NDVI data
  
- **Data Parsing**: 
  - GPS data is extracted directly from the `NavSatFix` message.
  - Temperature and CWSI data are extracted using regular expressions from the string message.
  - NDVI data is parsed as a `Float32` message.

- **Data Publishing**: After parsing, the data is published to MQTT topics, such as:
  - `mqtt/gps`
  - `mqtt/temperature`
  - `mqtt/ndvi`

### Key Methods
- `__init__(self, host, port)`: Initializes the ROS 2 node, subscribes to topics, and connects to the MQTT broker.
- `subscribe_to_topics(self)`: Subscribes to the required ROS topics.
- `publish(self, topic, data)`: Publishes data to the specified MQTT topic.
- `gps_callback(self, msg)`, `temperature_callback(self, msg)`, `ndvi_callback(self, msg)`: Callback methods for processing and parsing the data from the respective ROS topics.

### Data Looper
The `data_looper` method runs in an infinite thread, repeatedly publishing the parsed data to the corresponding MQTT topics.

---

## MQTT Client (Server-Side)

The server-side script subscribes to MQTT topics and processes the received data to make it available in a structured JSON format.

### Key Components

- **JSON Wrapper (`json_wrapper_t`)**: This class is used to wrap individual pieces of data to make them JSON-compatible, with additional features like property access and formatting.
  
- **Data Storage (`ros_data_t`)**: A `dataclass` that holds various sensor data (GPS, Temperature, NDVI) and provides methods for updating and retrieving the data.

- **MQTT Client (`mqtt_data_uploader_t`)**: This class handles MQTT connections, subscribes to topics, processes incoming data, and publishes custom JSON messages.
  
### Key Methods
- `__init__(self, host, port)`: Initializes the MQTT clients and subscribes to the necessary topics.
- `subscribe_to_topics(self)`: Subscribes to the MQTT topics for GPS, Temperature, and NDVI data.
- `on_mqtt_message(self, client, userdata, msg)`: Callback method for processing incoming MQTT messages.
- `global_data_update(self, msg_type, data)`: Updates the global data container with new data.
- `robot_message_01(self)`: Generates and formats a custom JSON message containing robot data, including GPS and Temperature information.
- `data_looper(self)`: Continuously publishes the formatted data to MQTT topics.

### Data Publishing
The `mqtt_data_uploader_t` class can publish data to topics such as:
- `gps/json`
- `temperature/json`
- `ndvi/json`

Custom data can also be published under the `robot/json` topic.

---

## How the Code Works

1. **Client-Side (Robot)**: 
   - The client-side script subscribes to ROS 2 topics (GPS, Temperature, NDVI).
   - It parses the data and sends it to an MQTT broker under specific topics.
  
2. **Server-Side (Server)**: 
   - The server subscribes to the MQTT topics and processes the incoming data.
   - It uses a helper class (`json_wrapper_t`) to format the data in JSON and publishes it under new topics.
  
3. **Data Flow**:
   - The client collects data from ROS 2 topics and sends it to the MQTT broker.
   - The server collects the data from MQTT topics, processes it, and generates custom JSON data for further use or storage.

---

## Configuration

### MQTT Parameters
- **Host**: The MQTT broker's address (default is `localhost`).
- **Port**: The MQTT broker's port (default is `1883`).
- **Timeout**: The timeout for MQTT connections (default is `120` seconds).

### Topics
- **Client-Side Topics**:
  - `mqtt/gps`
  - `mqtt/temperature`
  - `mqtt/ndvi`

- **Server-Side Topics**:
  - `gps/json`
  - `temperature/json`
  - `ndvi/json`
  - `robot/json`

### Regular Expressions
- `GET_FLOAT_NUMBER`: Matches float numbers (e.g., `12.34`).
- `GET_TEMPERATURE_DATA`: Extracts temperature and CWSI values from the temperature string.

---

## Conclusion

This script enables seamless communication between a ROS 2 system and an MQTT broker, allowing for real-time data exchange and processing. The client-side script captures sensor data from ROS 2, while the server-side script processes and formats this data into a structured JSON format for storage or further analysis.
