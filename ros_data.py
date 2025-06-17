from dataclasses import dataclass
    
# A helper class to store and update data from ROS messages.

@dataclass
class ros_data_t:
    
    # Raw fields (auto-wrapped via __getattribute__)
    
    g_timestamp: int = None 
    g_latitude: float = None
    g_longitude: float = None
    g_altitude: float = None
    g_status: int = None
    g_service: int = None

    t_entity_count: str = None
    t_canopy_temperature: float = None
    t_cswi: float = None

    n_ndvi: float = None
    n_ndvi_3d: float = None
    n_ir: float = None
    n_visible: float = None

    def update(self, data: dict):
        msg: str = data.get("msg_type")
        
        if msg.find("gps") != -1:
        
            self.g_timestamp = data.get("timestamp")
            self.g_latitude = data.get("latitude")
            self.g_longitude = data.get("longitude")
            self.g_altitude = data.get("altitude")
            self.g_status = data.get("status")
            self.g_service = data.get("service")

        elif msg.find("temperature") != -1:
            
            self.t_entity_count = data.get("entity_count")
            self.t_canopy_temperature = data.get("canopy_temperature")
            self.t_cswi = data.get("cwsi")
            
        elif msg.find("ndvi") != -1:
            
            self.n_ndvi = data.get("ndvi")
            self.n_ndvi_3d = data.get("ndvi_3d")
            self.n_ir = data.get("ir")
            self.n_visible = data.get("visible")

        else:
            raise ValueError(f"Unsupported msg_type: {msg}")