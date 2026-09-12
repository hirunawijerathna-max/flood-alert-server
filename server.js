require('dotenv').config();
const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

// --- 1. SUPABASE SETUP ---
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// --- 2. INFLUXDB SETUP ---
const influxDB = new InfluxDB({ 
    url: process.env.INFLUX_URL, 
    token: process.env.INFLUX_TOKEN 
});
const writeApi = influxDB.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET);

// යැවූ Alerts නැවත නැවත යැවීම වැළැක්වීමට (State Tracking)
let activeAlerts = {}; 

// --- 3. MQTT SETUP ---
const mqttOptions = {
    port: parseInt(process.env.MQTT_PORT),
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS,
    protocol: 'mqtts',
    rejectUnauthorized: false
};

const client = mqtt.connect(process.env.MQTT_HOST, mqttOptions);

client.on('connect', () => {
    console.log('✅ Connected to HiveMQ Cloud Broker successfully!');
    client.subscribe(process.env.MQTT_TOPIC, (err) => {
        if (!err) {
            console.log(`📡 Subscribed to topic: ${process.env.MQTT_TOPIC}`);
        }
    });
});

// --- 4. DATA PROCESSING & BIDIRECTIONAL ALERT LOGIC ---
client.on('message', async (topic, message) => {
    try {
        const data = JSON.parse(message.toString());
        const waterLevel = data.water_level; // සෙන්සර් දුර (distance)
        const sensorId = data.sensor_id || 'sensor1';
        const battery = data.battery || 100;

        console.log(`\n========================================`);
        console.log(`[Data Received] Sensor: ${sensorId} | Water Level: ${waterLevel} cm`);

        // 2. InfluxDB එකට Sensor Data Save කිරීම
        try {
            const point = new Point('water_level_sensor')
                .tag('sensor_id', sensorId)
                .floatField('water_level_cm', waterLevel)
                .floatField('battery_pct', battery);

            writeApi.writePoint(point);
            await writeApi.flush();
            console.log('📊 InfluxDB: Data saved successfully.');
        } catch (error) {
            console.error('❌ InfluxDB Error:', error.message);
        }

        // --- 3. Threshold-based Bidirectional Escalation & Recovery Alert Logic ---
        try {
            const { data: areas, error: areaError } = await supabase
                .from('areas')
                .select('*')
                .order('min_threshold', { ascending: true });

            if (areaError) throw areaError;

            if (areas && areas.length > 0) {
                for (const area of areas) {
                    const thresholdLimit = area.min_threshold; 
                    let lastState = activeAlerts[area.area_name] || 'NORMAL'; 

                    // තත්ත්වය 1: දුර අඩුවී threshold අගයට වඩා අඩු හෝ සමාන වීම (වතුර ඉහළ නැගීම / අවදානම වැඩි වීම)
                    if (waterLevel <= thresholdLimit && lastState !== 'HIGH_UP') {
                        console.log(`🚨 Alert (Danger): Water level reached threshold ${thresholdLimit}cm for ${area.area_name} (Current: ${waterLevel}cm)`);

                        const { data: users, error: userError } = await supabase
                            .from('users')
                            .select('phone_number')
                            .eq('area_id', area.id);

                        if (userError) throw userError;

                        const phoneNumbers = users.map(u => u.phone_number);
                        if (phoneNumbers.length > 0) {
                            sendCustomSMS(area.area_name, waterLevel, phoneNumbers, 'RISEN');
                        }

                        activeAlerts[area.area_name] = 'HIGH_UP';
                    }
                    // තත්ත්වය 2: දුර වැඩි වී threshold අගයට වඩා ඉහළ යාම (වතුර බැසීම / තත්ත්වය සාමාන්‍ය වීම)
                    else if (waterLevel > thresholdLimit && lastState === 'HIGH_UP') {
                        console.log(`📉 Alert (Safe): Water level receded above ${thresholdLimit}cm for ${area.area_name} (Current: ${waterLevel}cm)`);

                        const { data: users, error: userError } = await supabase
                            .from('users')
                            .select('phone_number')
                            .eq('area_id', area.id);

                        if (userError) throw userError;

                        const phoneNumbers = users.map(u => u.phone_number);
                        if (phoneNumbers.length > 0) {
                            sendCustomSMS(area.area_name, waterLevel, phoneNumbers, 'DROPPED');
                        }

                        activeAlerts[area.area_name] = 'NORMAL';
                    }
                }
            }
        } catch (error) {
            console.error('❌ Supabase Query Error:', error.message);
        }

    } catch (error) {
        console.error('❌ Invalid data format (JSON Data Parse Error):', error.message);
    }
});

// --- 5. UPDATED NOTIFY.LK SMS FUNCTION (WITH PROPER PHONE CLEANING) ---
function sendCustomSMS(areaName, level, phoneNumbers, direction) {
    phoneNumbers.forEach(phoneNumber => {
        // අංකයේ ඇති අනවශ්‍ය අක්ෂර ඉවත් කර අංක පමණක් ලබා ගැනීම
        let cleanedPhone = phoneNumber.toString().trim().replace(/[^0-9]/g, '');
        
        if (cleanedPhone.startsWith('0')) {
            cleanedPhone = '94' + cleanedPhone.substring(1);
        } else if (cleanedPhone.length === 9) {
            cleanedPhone = '94' + cleanedPhone;
        }

        let messageText = "";
        if (direction === 'RISEN') {
            messageText = `FLOOD ALERT: ${areaName} - Water level is rising dangerously! Please stay alert.`;
        } else {
            messageText = `WATER LEVEL UPDATE: ${areaName} - Water level is receding. Situation is normalizing.`;
        }

        const notifyUrl = `https://app.notify.lk/api/v1/send` +
            `?user_id=${process.env.NOTIFY_USER_ID}` +
            `&api_key=${process.env.NOTIFY_API_KEY}` +
            `&sender_id=${process.env.NOTIFY_SENDER_ID}` +
            `&to=${cleanedPhone}` +
            `&message=${encodeURIComponent(messageText)}`;

        axios.get(notifyUrl)
            .then(res => {
                console.log(`📱 Notify.lk Response for ${cleanedPhone}:`, res.data);
            })
            .catch(err => {
                if (err.response) {
                    console.error(`❌ SMS API Error (${cleanedPhone}) [Status ${err.response.status}]:`, err.response.data);
                } else {
                    console.error(`❌ SMS Request Error (${cleanedPhone}):`, err.message);
                }
            });
    });
}