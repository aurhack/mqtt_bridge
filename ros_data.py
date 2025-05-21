
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict

from mqtt_bridge_utils import json_wrapper_t

# ros_data_t msg types
class msg_type(Enum):
    GPS = 0
    TEMPERATURE = 1
    NDVI = 2
    
# A helper class to store and update data from ROS messages.

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


