
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

        // --- 3. Threshold-based Bidirectional Escalation & Recovery Alert Logic ---
try {
    // Supabase වලින් සියලුම areas වල threshold අගයන් ලබා ගැනීම (අනුපිළිවෙලට සකසා ගැනීම වැදගත් වේ)
    const { data: areas, error: areaError } = await supabase
        .from('areas')
        .select('*')
        .order('min_threshold', { ascending: true });

    if (areaError) throw areaError;

    if (areas && areas.length > 0) {
        for (const area of areas) {
            const thresholdLimit = area.min_threshold; // උදා: 50 හෝ 70
            let lastState = activeAlerts[area.area_name] || 'NORMAL'; // පෙර තත්ත්වය ('NORMAL', 'HIGH_UP', 'HIGH_DOWN')

            // තත්ත්වය 1: ජල මට්ටම Threshold එකට වඩා වැඩි වීම (ඉහළ යෑම)
            if (waterLevel >= thresholdLimit && lastState !== 'HIGH_UP') {
                console.log(`🚨 Alert (Rising): Water level reached/crossed ${thresholdLimit}cm for ${area.area_name} (Current: ${waterLevel}cm)`);

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
            // තත්ත්වය 2: ජල මට්ටම ඉහළ මට්ටමේ සිට අදාළ Threshold එකට වඩා පහළට බැසීම
            else if (waterLevel < thresholdLimit && lastState === 'HIGH_UP') {
                console.log(`📉 Alert (Dropping): Water level dropped below ${thresholdLimit}cm for ${area.area_name} (Current: ${waterLevel}cm)`);

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

// --- 5. UPDATED NOTIFY.LK SMS FUNCTION ---
function sendCustomSMS(areaName, level, phoneNumbers, direction) {
    phoneNumbers.forEach(phoneNumber => {
        let formattedPhone = phoneNumber.toString().trim();
        
        if (formattedPhone.startsWith('0')) {
            formattedPhone = '94' + formattedPhone.substring(1);
        } else if (formattedPhone.startsWith('+94')) {
            formattedPhone = 'formattedPhone.substring(1);' // හෝ substring(1)
        }

        let messageText = "";
        if (direction === 'RISEN') {
            messageText = `FLOOD ALERT: ${areaName} - Water level rose to ${level}cm. Please stay alert!`;
        } else {
            messageText = `WATER LEVEL UPDATE: ${areaName} - Water level dropped to ${level}cm. Situation normalizing.`;
        }

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
                console.error(`❌ SMS Request Error (${formattedPhone}):`, err.message);
            });
    });
}