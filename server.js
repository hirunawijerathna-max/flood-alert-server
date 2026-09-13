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

// යැවූ Alerts නැවත නැවත යැවීම වැළැක්වීමට සහ Level Update කිරීම සඳහා (State Tracking)
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

// --- 4. DATA PROCESSING & ADVANCED CUMULATIVE UPDATE LOGIC ---
client.on('message', async (topic, message) => {
    try {
        const data = JSON.parse(message.toString());
        const waterLevel = data.water_level; // මීටර් වලින් එන අගය
        const sensorId = data.sensor_id || 'sensor1';
        const battery = data.battery || 100;

        console.log(`\n========================================`);
        console.log(`[Data Received] Sensor: ${sensorId} | Water Level: ${waterLevel} m`);

        // InfluxDB එකට Sensor Data Save කිරීම
        try {
            const point = new Point('water_level_sensor')
                .tag('sensor_id', sensorId)
                .floatField('water_level_m', waterLevel)
                .floatField('battery_pct', battery);

            writeApi.writePoint(point);
            await writeApi.flush();
            console.log('📊 InfluxDB: Data saved successfully.');
        } catch (error) {
            console.error('❌ InfluxDB Error:', error.message);
        }

        // --- Threshold-based Escalation, Updates & Recovery Logic ---
        try {
            const { data: areas, error: areaError } = await supabase
                .from('areas')
                .select('*')
                .order('min_threshold', { ascending: true }); // 28, 30, 32

            if (areaError) throw areaError;

            if (areas && areas.length > 0) {
                // 1. දැනට වතුර මට්ටම පහුකරපු 'උපරිම' සීමාව (Max Threshold) සොයාගැනීම
                let maxCrossedThreshold = 0;
                for (const area of areas) {
                    if (waterLevel >= area.min_threshold) {
                        maxCrossedThreshold = Math.max(maxCrossedThreshold, area.min_threshold);
                    }
                }

                // 2. එක් එක් කලාපයට අදාළව ඇට් යැවීම සහ යාවත්කාලීන කිරීම
                for (const area of areas) {
                    const thresholdLimit = area.min_threshold; 
                    let lastState = activeAlerts[area.area_name] || 'NORMAL'; 

                    // තත්ත්වය A: ජල මට්ටම අදාළ කලාපයේ අවදානම් සීමාවට වඩා වැඩි නම්
                    if (waterLevel >= thresholdLimit) {
                        
                        // අලුත්ම අවදානම් සීමාවක් පැනලා නම් (පහළ කලාප වලට Update එකක් හෝ අලුත් කලාපයට මුල් SMS එක)
                        if (lastState === 'NORMAL' || maxCrossedThreshold > lastState) {
                            
                            // අලුත් Alert එකක් ද නැත්නම් Update එකක් ද යන්න තීරණය කිරීම
                            let alertType = (lastState === 'NORMAL') ? 'RISEN' : 'UPDATE_RISEN';
                            
                            console.log(`🚨 Alert (${alertType}): Limit reached ${maxCrossedThreshold}m! Sending to ${area.area_name} (Current: ${waterLevel}m)`);

                            const { data: users, error: userError } = await supabase
                                .from('users')
                                .select('phone_number')
                                .eq('area_id', area.id);

                            if (userError) throw userError;

                            const phoneNumbers = users.map(u => u.phone_number);
                            if (phoneNumbers.length > 0) {
                                sendCustomSMS(area.area_name, waterLevel, phoneNumbers, alertType);
                            }

                            // අදාළ කලාපයේ අලුත්ම තත්ත්වය උපරිම Threshold එක ලෙස සටහන් කිරීම
                            activeAlerts[area.area_name] = maxCrossedThreshold;
                        } 
                        // ජල මට්ටම ඉහළම සීමාවෙන් (උදා: 32න්) පහළට බැස්සත්, තවමත් මේ කලාපයේ (උදා: 28) සීමාවට වඩා වැඩිනම්,
                        // SMS නොයවා State එක පමණක් නිහඬව Update කිරීම (නැවත ඉහළ ගියොත් Update SMS යැවීමට පහසු වීමට)
                        else if (maxCrossedThreshold < lastState) {
                            activeAlerts[area.area_name] = maxCrossedThreshold;
                        }

                    } 
                    // තත්ත්වය B: ජල මට්ටම අදාළ කලාපයේ Threshold එකට වඩා සම්පූර්ණයෙන්ම පහළට බැසීම (Dropping)
                    else if (waterLevel < thresholdLimit && lastState !== 'NORMAL') {
                        console.log(`📉 Alert (Dropping): Water level dropped below ${thresholdLimit}m for ${area.area_name} (Current: ${waterLevel}m)`);

                        const { data: users, error: userError } = await supabase
                            .from('users')
                            .select('phone_number')
                            .eq('area_id', area.id);

                        if (userError) throw userError;

                        const phoneNumbers = users.map(u => u.phone_number);
                        if (phoneNumbers.length > 0) {
                            sendCustomSMS(area.area_name, waterLevel, phoneNumbers, 'DROPPED');
                        }

                        // කලාපයේ තත්ත්වය නැවත සාමාන්‍ය (NORMAL) කිරීම
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

// --- 5. UPDATED NOTIFY.LK SMS FUNCTION (WITH 'UPDATE' MESSAGE) ---
function sendCustomSMS(areaName, level, phoneNumbers, direction) {
    phoneNumbers.forEach(phoneNumber => {
        let cleanedPhone = phoneNumber.toString().trim().replace(/[^0-9]/g, '');
        
        if (cleanedPhone.startsWith('0')) {
            cleanedPhone = '94' + cleanedPhone.substring(1);
        } else if (cleanedPhone.length === 9) {
            cleanedPhone = '94' + cleanedPhone;
        }

        let messageText = "";
        if (direction === 'RISEN') {
            // පළමු වතාවට අවදානම් සීමාව පනිද්දී යන පණිවිඩය
            messageText = `FLOOD ALERT: ${areaName} - Water level rose to ${level}m. Please stay alert!`;
        } else if (direction === 'UPDATE_RISEN') {
            // තවත් අවදානම් සීමාවක් ඉක්මවා වතුර වැඩි වෙද්දී පහළ කලාප වලට යන 'Update' පණිවිඩය
            messageText = `FLOOD UPDATE: ${areaName} - Water level increased further to ${level}m. Danger is escalating!`;
        } else {
            // ජල මට්ටම බැස යද්දී අදාළ කලාපයට යන පණිවිඩය
            messageText = `WATER LEVEL UPDATE: ${areaName} - Water level dropped to ${level}m. Situation normalizing.`;
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