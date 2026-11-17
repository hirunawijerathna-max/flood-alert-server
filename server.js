
require('dotenv').config();

const http = require('http');
const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

// ==================================================
// 1. ENVIRONMENT CONFIGURATION
// ==================================================

const PORT = process.env.PORT || 3000;

const requiredVariables = [
    'MQTT_HOST',
    'MQTT_PORT',
    'MQTT_USER',
    'MQTT_PASS',
    'MQTT_TOPIC',
    'INFLUX_URL',
    'INFLUX_TOKEN',
    'INFLUX_ORG',
    'INFLUX_BUCKET',
    'SUPABASE_URL',
    'SUPABASE_KEY',
    'NOTIFY_USER_ID',
    'NOTIFY_API_KEY',
    'NOTIFY_SENDER_ID'
];

const missingVariables = requiredVariables.filter(
    key => !process.env[key]
);

if (missingVariables.length > 0) {
    console.error('Missing environment variables:', missingVariables.join(', '));
    process.exit(1);
}

// ==================================================
// 2. SUPABASE SETUP
// ==================================================

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY
);

// ==================================================
// 3. INFLUXDB SETUP
// ==================================================

const influxDB = new InfluxDB({
    url: process.env.INFLUX_URL,
    token: process.env.INFLUX_TOKEN
});

const writeApi = influxDB.getWriteApi(
    process.env.INFLUX_ORG,
    process.env.INFLUX_BUCKET
);

// ==================================================
// 4. MQTT SETUP
// ==================================================

const mqttOptions = {
    port: Number(process.env.MQTT_PORT),
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS,
    protocol: 'mqtts',
    rejectUnauthorized: true,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clientId: `HydroSentinel_${Math.random().toString(16).slice(2, 10)}`
};

console.log('Connecting to HiveMQ Cloud...');

const client = mqtt.connect(
    process.env.MQTT_HOST,
    mqttOptions
);

client.on('connect', () => {
    console.log('HiveMQ Connected Successfully!');

    client.subscribe(
        process.env.MQTT_TOPIC,
        { qos: 1 },
        (err) => {
            if (err) {
                console.error('MQTT Subscribe Error:', err.message);
            } else {
                console.log('Subscribed to:', process.env.MQTT_TOPIC);
            }
        }
    );
});

client.on('error', (err) => {
    console.error('MQTT Connection Error:', err.message);
});

client.on('reconnect', () => {
    console.log('MQTT Reconnecting...');
});

client.on('offline', () => {
    console.log('MQTT Client Offline');
});

client.on('close', () => {
    console.log('MQTT Connection Closed');
});

// ==================================================
// 5. ALERT STATE TRACKING
// ==================================================

let activeAlerts = {};

// Prevent overlapping message processing
let processingQueue = Promise.resolve();

// ==================================================
// 6. MQTT MESSAGE RECEIVING
// ==================================================

client.on('message', (topic, message) => {

    processingQueue = processingQueue
        .then(() => processSensorData(topic, message))
        .catch(err => {
            console.error('Message Processing Error:', err.message);
        });

});

// ==================================================
// 7. SENSOR DATA PROCESSING
// ==================================================

async function processSensorData(topic, message) {

    let data;

    try {
        data = JSON.parse(message.toString());
    } catch (err) {
        console.error('Invalid JSON:', err.message);
        return;
    }

    const waterLevel = Number(data.water_level);
    const sensorId = data.sensor_id || 'sensor1';
    const battery = Number(data.battery ?? 100);

    if (!Number.isFinite(waterLevel) || !Number.isFinite(battery)) {
        console.error('Invalid sensor values');
        return;
    }

    console.log('\n====================================');
    console.log('SENSOR DATA RECEIVED');
    console.log('Sensor ID:', sensorId);
    console.log('Water Level:', waterLevel, 'm');
    console.log('Battery:', battery, '%');
    console.log('====================================');

    // ----------------------------------------------
    // INFLUXDB DATA STORAGE
    // ----------------------------------------------

    try {
        const point = new Point('water_level_sensor')
            .tag('sensor_id', String(sensorId))
            .floatField('water_level_m', waterLevel)
            .floatField('battery_pct', battery);

        writeApi.writePoint(point);
        await writeApi.flush();

        console.log('InfluxDB: Data Saved Successfully');

    } catch (error) {
        console.error('InfluxDB Error:', error.message);
    }

    // ----------------------------------------------
    // SUPABASE THRESHOLD PROCESSING
    // ----------------------------------------------

    try {

        const { data: areas, error: areaError } = await supabase
            .from('areas')
            .select('*')
            .order('min_threshold', { ascending: true });

        if (areaError) throw areaError;

        if (!areas || areas.length === 0) {
            console.log('No areas found in Supabase');
            return;
        }

        let maxCrossedThreshold = 0;

        for (const area of areas) {

            const threshold = Number(area.min_threshold);

            if (waterLevel >= threshold) {
                maxCrossedThreshold = Math.max(
                    maxCrossedThreshold,
                    threshold
                );
            }
        }

        for (const area of areas) {

            const thresholdLimit = Number(area.min_threshold);

            const lastState =
                activeAlerts[area.area_name] ?? 'NORMAL';

            // WATER LEVEL RISEN
            if (waterLevel >= thresholdLimit) {

                if (
                    lastState === 'NORMAL' ||
                    maxCrossedThreshold > lastState
                ) {

                    const alertType =
                        lastState === 'NORMAL'
                            ? 'RISEN'
                            : 'UPDATE_RISEN';

                    console.log(
                        `FLOOD ALERT: ${area.area_name} - ${waterLevel}m`
                    );

                    const { data: users, error: userError } =
                        await supabase
                            .from('users')
                            .select('phone_number')
                            .eq('area_id', area.id);

                    if (userError) throw userError;

                    const phoneNumbers = (users || [])
                        .map(user => user.phone_number)
                        .filter(Boolean);

                    if (phoneNumbers.length > 0) {
                        await sendCustomSMS(
                            area.area_name,
                            waterLevel,
                            phoneNumbers,
                            alertType
                        );
                    }

                    activeAlerts[area.area_name] =
                        maxCrossedThreshold;
                }

                else if (maxCrossedThreshold < lastState) {
                    activeAlerts[area.area_name] =
                        maxCrossedThreshold;
                }

            }

            // WATER LEVEL DROPPED
            else if (
                waterLevel < thresholdLimit &&
                lastState !== 'NORMAL'
            ) {

                console.log(
                    `WATER LEVEL DROPPED: ${area.area_name}`
                );

                const { data: users, error: userError } =
                    await supabase
                        .from('users')
                        .select('phone_number')
                        .eq('area_id', area.id);

                if (userError) throw userError;

                const phoneNumbers = (users || [])
                    .map(user => user.phone_number)
                    .filter(Boolean);

                if (phoneNumbers.length > 0) {
                    await sendCustomSMS(
                        area.area_name,
                        waterLevel,
                        phoneNumbers,
                        'DROPPED'
                    );
                }

                activeAlerts[area.area_name] = 'NORMAL';
            }
        }

    } catch (error) {
        console.error('Supabase Error:', error.message);
    }
}

// ==================================================
// 8. NOTIFY.LK SMS FUNCTION
// ==================================================

async function sendCustomSMS(
    areaName,
    level,
    phoneNumbers,
    direction
) {

    const statusName = String(areaName).toUpperCase();

    let messageText = '';

    if (
        String(areaName).toLowerCase() === 'critical' &&
        (direction === 'RISEN' || direction === 'UPDATE_RISEN')
    ) {

        messageText =
            `EXTREME DANGER: CRITICAL flood level reached (${level}m)! Follow official emergency instructions.`;

    } else if (direction === 'RISEN') {

        messageText =
            `FLOOD WARNING: Status is now ${statusName}. Water level: ${level}m. Please stay alert!`;

    } else if (direction === 'UPDATE_RISEN') {

        messageText =
            `FLOOD ESCALATION: Status worsened to ${statusName}. Water level: ${level}m. Stay safe!`;

    } else {

        messageText =
            `LEVEL DROP: Status downgraded to ${statusName}. Water level: ${level}m. Follow official updates.`;
    }

    for (const phoneNumber of phoneNumbers) {

        let cleanedPhone = String(phoneNumber)
            .trim()
            .replace(/[^0-9]/g, '');

        if (cleanedPhone.startsWith('0')) {
            cleanedPhone = '94' + cleanedPhone.substring(1);
        } else if (cleanedPhone.length === 9) {
            cleanedPhone = '94' + cleanedPhone;
        }

        try {

            const response = await axios.get(
                'https://app.notify.lk/api/v1/send',
                {
                    params: {
                        user_id: process.env.NOTIFY_USER_ID,
                        api_key: process.env.NOTIFY_API_KEY,
                        sender_id: process.env.NOTIFY_SENDER_ID,
                        to: cleanedPhone,
                        message: messageText
                    },
                    timeout: 15000
                }
            );

            console.log(
                `Notify.lk Response for ${cleanedPhone}:`,
                response.data
            );

        } catch (error) {

            console.error(
                `Notify.lk SMS Error for ${cleanedPhone}:`,
                error.response?.data || error.message
            );
        }
    }
}

// ==================================================
// 9. RAILWAY HTTP HEALTH SERVER
// ==================================================

const healthServer = http.createServer((req, res) => {

    if (req.url === '/health') {

        res.writeHead(200, {
            'Content-Type': 'application/json'
        });

        res.end(JSON.stringify({
            status: 'online',
            service: 'HydroSentinel Flood Alert Server',
            mqtt: client.connected ? 'connected' : 'disconnected',
            timestamp: new Date().toISOString()
        }));

    } else {

        res.writeHead(200, {
            'Content-Type': 'text/plain'
        });

        res.end('HydroSentinel Flood Alert Server is running!');
    }
});

healthServer.listen(PORT, '0.0.0.0', () => {
    console.log(`HTTP Server Running on Port ${PORT}`);
    console.log(`Health Check: /health`);
});

// ==================================================
// 10. GRACEFUL SHUTDOWN
// ==================================================

async function shutdown() {

    console.log('Shutting down server...');

    try {
        await new Promise(resolve => {
            client.end(false, {}, resolve);
        });

        await writeApi.close();

        healthServer.close(() => {
            console.log('Server shutdown completed');
            process.exit(0);
        });

    } catch (error) {
        console.error('Shutdown Error:', error.message);
        process.exit(1);
    }
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
