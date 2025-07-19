#!/bin/bash

# Get Automatic IP from the Server (Should be 147.83.52.40 anyways)
PUBLIC_IP=$(curl -s https://api.ipify.org || echo "unknown_ip")

# Export the INFLUX_TOKEN
export INFLUX_TOKEN=

# Kill existing Telegraf and Python processes to avoid duplicates
pkill -f "telegraf --config http://$PUBLIC_IP" 2>/dev/null
pkill -f "python3 mqtt_bridge_server.py" 2>/dev/null

# Launch everything in GNOME Terminal tabs
gnome-terminal --tab --title="Telegraf" -- bash -c \
  "echo 'Running Telegraf with IP: $PUBLIC_IP'; \
   telegraf --config http://$PUBLIC_IP:8086/api/v2/telegrafs/; \
   exec bash"

gnome-terminal --tab --title="MQTT Server" -- bash -c \
  "export LAUNCHED_VIA_SETUP=1;python3 mqtt_bridge_server.py; exec bash"

# Optional: Close all tabs when scripts exit
# gnome-terminal --wait --tab ... (would wait for completion)
