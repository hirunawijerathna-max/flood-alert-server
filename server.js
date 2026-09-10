
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

// --- 4. DATA PROCESSING & CASCADING ALERT LOGIC ---
client.on('message', async (topic, message) => {
    try {
        // 1. කලින් (උඩ) ආකාරයටම JSON ලෙස දත්ත ලබාගැනීම
        const data = JSON.parse(message.toString());
        const waterLevel = data.water_level;
        const sensorId = data.sensor_id || 'sensor1';
        const battery = data.battery || 100;

        console.log(`\n========================================`);
        console.log(`[Data Received] Sensor: ${sensorId} | Water Level: ${waterLevel} cm`);

        // 2. InfluxDB එකට Sensor Data Save කිරීම (කලින් තිබූ ආකෘතියටම)
        try {
            const point = new Point('water_level_sensor')
                .tag('sensor_id', sensorId) // දැන් මෙතනට එන්නේ JSON එකෙන් එන නමයි (S1 කියා hardcode නොවේ)
                .floatField('water_level_cm', waterLevel)
                .floatField('battery_pct', battery); // බැටරි ප්‍රතිශතයත් එකතු කර ඇත

            writeApi.writePoint(point);
            await writeApi.flush();
            console.log('📊 InfluxDB: Data saved successfully.');
        } catch (error) {
            console.error('❌ InfluxDB Error:', error.message);
        }

        // 3. Cascading Alert Logic: min_threshold එක ජල මට්ටමට වඩා අඩු හෝ සමාන සියලුම Areas තෝරාගැනීම
        try {
            const { data: areas, error: areaError } = await supabase
                .from('areas')
                .select('*')
                .lte('min_threshold', waterLevel);

            if (areaError) throw areaError;

            if (areas && areas.length > 0) {
                for (const area of areas) {
                    // මෙම Area එකට තවම Alert යවා නැත්නම් පමණක් SMS trigger කිරීම
                    if (!activeAlerts[area.area_name]) {
                        console.log(`🚨 Triggering Alert for: ${area.area_name}`);

                        // අදාළ Area එකට අයිති Usersලාගේ Phone Numbers Supabase එකෙන් ගැනීම
                        const { data: users, error: userError } = await supabase
                            .from('users')
                            .select('phone_number')
                            .eq('area_id', area.id);

                        if (userError) throw userError;

                        const phoneNumbers = users.map(u => u.phone_number);
                        if (phoneNumbers.length > 0) {
                            sendSMSAlerts(area.area_name, waterLevel, phoneNumbers);
                        } else {
                            console.log(`⚠️ No users found for ${area.area_name}`);
                        }
                        
                        activeAlerts[area.area_name] = true;
                    }
                }
            } else {
                // ජල මට්ටම සාමාන්‍ය තත්ත්වයට ආ විට Active Alerts Reset කිරීම
                if (Object.keys(activeAlerts).length > 0) {
                    console.log('🟢 Water level normalized. Resetting active alert state.');
                    activeAlerts = {};
                }
            }
        } catch (error) {
            console.error('❌ Supabase Query Error:', error.message);
        }

    } catch (error) {
        // අහම්බෙන් හෝ JSON නොවන දත්තයක් ආවොත් පද්ධතිය බිඳ වැටීම වළක්වයි
        console.error('❌ Invalid data format (JSON Data Parse Error):', error.message);
    }
});

// --- 5. NOTIFY.LK SMS FUNCTION ---
function sendSMSAlerts(areaName, level, phoneNumbers) {
    phoneNumbers.forEach(phoneNumber => {
        let formattedPhone = phoneNumber.toString().trim();
        
        if (formattedPhone.startsWith('0')) {
            formattedPhone = '94' + formattedPhone.substring(1);
        } else if (formattedPhone.startsWith('+94')) {
            formattedPhone = formattedPhone.substring(1);
        }

        const messageText = `FLOOD ALERT: ${areaName} - Water level reached ${level}cm. Please move to a safe location!`;

        const notifyUrl = `https://app.notify.lk/api/v1/send` +
            `?user_id=${process.env.NOTIFY_USER_ID}` +
            `&api_key=${process.env.NOTIFY_API_KEY}` +
            `&sender_id=${process.env.NOTIFY_SENDER_ID}` +
            `&to=${formattedPhone}` +
            `&message=${encodeURIComponent(messageText)}`;

        axios.get(notifyUrl)
            .then(res => {
                console.log(`📱 Notify.lk Response for ${formattedPhone}:`, res.data);
            })
            .catch(err => {
                if (err.response) {
                    // API එකෙන් එවපු නිශ්චිත Error එක පෙන්වීම
                    console.error(`❌ Notify.lk API Error (${formattedPhone}) [Status ${err.response.status}]:`, err.response.data);
                } else {
                    console.error(`❌ SMS Request Error (${formattedPhone}):`, err.message);
                }
            });
    });
}