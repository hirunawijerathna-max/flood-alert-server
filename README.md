# Real-Time IoT Flood Monitoring & Automated SMS Alert Server

A Node.js backend server designed to monitor real-time water level data from IoT sensors via MQTT, log metrics to InfluxDB, evaluate risk zones using Supabase, and trigger automated emergency SMS alerts via Notify.lk.

## 🚀 Tech Stack
* **Runtime:** Node.js
* **Broker:** HiveMQ Cloud (MQTT Protocol)
* **Database (Time-Series):** InfluxDB Cloud
* **Database (Relational):** Supabase (PostgreSQL)
* **SMS Gateway:** Notify.lk API

## 📋 Features
* Real-time MQTT message listening for water level sensors.
* Automatic time-series logging to InfluxDB.
* Risk-level evaluation and automated SMS broadcasting to affected zones.

## ⚙️ Environment Variables (.env)
Create a `.env` file in the root directory and add the following configurations:

```env
MQTT_HOST=your_mqtt_host
MQTT_PORT=8883
MQTT_USER=your_mqtt_user
MQTT_PASS=your_mqtt_pass
MQTT_TOPIC=flood/sensor1/water_level

INFLUX_URL=your_influx_url
INFLUX_TOKEN=your_influx_token
INFLUX_ORG=your_influx_org
INFLUX_BUCKET=your_influx_bucket

SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key

NOTIFY_USER_ID=your_notify_user_id
NOTIFY_API_KEY=your_notify_api_key
NOTIFY_SENDER_ID=your_notify_sender_id