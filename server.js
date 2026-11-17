
require('dotenv').config();

const http = require('http');
const mqtt = require('mqtt');
const axios = require('axios');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { createClient } = require('@supabase/supabase-js');

// ==========================================
// 1. SERVER START
// ==========================================

console.log('======================================');
console.log('HYDROSENTINEL SERVER STARTING...');
console.log('======================================');

const PORT = process.env.PORT || 3000;

// ==========================================
// 2. ENVIRONMENT VARIABLES CHECK
// ==========================================

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
    console.error(
        'MISSING ENVIRONMENT VARIABLES:',
        missingVariables.join(', ')
    );
    process.exit(1);
}

console.log('Environment variables check PASSED');

// ==========================================
// 3. SUPABASE SETUP
// ==========================================

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY
);

console.log('Supabase initialized');

// ==========================================
// 4. INFLUXDB SETUP
// ==========================================

const influxDB = new InfluxDB({
    url: process.env.INFLUX_URL,
    token: process.env.INFLUX_TOKEN
});

const writeApi = influxDB.getWriteApi(
    process.env.INFLUX_ORG,
    process.env.INFLUX_BUCKET
);

console.log('InfluxDB initialized');

// ==========================================
// 5. SERVER HEALTH CHECK
// ==========================================

let mqttConnected = false;
let receivedMessages = 0;
let lastReceivedAt = null;

const healthServer = http.createServer((req, res) => {

    if (req.url === '/health') {

        res.writeHead(200, {
            'Content-Type': 'application/json'
        });

        res.end(JSON.stringify({
            status: 'online',
            service: 'HydroSentinel Flood Alert',
            mqtt: mqttConnected ? 'connected' : 'disconnected',
            receivedMessages,
            lastReceivedAt
        }));

    } else {

        res.writeHead(200, {
            'Content-Type': 'text/plain'
        });

        res.end('HydroSentinel Server Running!');
    }
});

healthServer.listen(PORT, '0.0.0.0', () => {
    console.log(`HTTP Server Running on Port ${PORT}`);
});

// ==========================================
// 6. MQTT CONFIGURATION
// ==========================================

const mqttOptions = {
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS,
    protocol: 'mqtts',
    port: Number(process.env.MQTT_PORT),
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clean: true,
    clientId: 'HydroSentinel_' +
        Math.random().toString(16).substring(2, 10)
};

console.log('======================================');
console.log('MQTT CONFIG CHECK');
console.log('Host configured:', !!process.env.MQTT_HOST);
console.log('Port:', process.env.MQTT_PORT);
console.log('Username configured:', !!process.env.MQTT_USER);
console.log('Password configured:', !!process.env.MQTT_PASS);
console.log('Topic:', process.env.MQTT_TOPIC);
console.log('======================================');

// ==========================================
// 7. CONNECT TO HIVEMQ
// ==========================================

console.log('Connecting to HiveMQ Cloud...');

const client = mqtt.connect(
    process.env.MQTT_HOST,
    mqttOptions
);

client.on('connect', () => {

    mqttConnected = true;

    console.log('======================================');
    console.log('HIVEMQ CONNECTED SUCCESSFULLY!');
    console.log('======================================');

    client.subscribe(
        process.env.MQTT_TOPIC,
        { qos: 1 },
        (err) => {

            if (err) {
                console.error('SUBSCRIBE ERROR:', err);
            } else {
                console.log(
                    'SUBSCRIBED:',
                    process.env.MQTT_TOPIC
                );
            }
        }
    );
});

client.on('error', (err) => {
    console.error('MQTT ERROR DETAILS:', err);
    console.error('MQTT ERROR CODE:', err.code);
});

client.on('reconnect', () => {
    console.log('MQTT RECONNECTING...');
});

client.on('offline', () => {
    mqttConnected = false;
    console.log('MQTT OFFLINE');
});

client.on('close', () => {
    mqttConnected = false;
    console.log('MQTT CONNECTION CLOSED');
});

// ==========================================
// 8. ALERT STATE
// ==========================================

let activeAlerts = {};

// ==========================================
// 9. RECEIVE MQTT MESSAGES
// ==========================================

let processingQueue = Promise.resolve();

client.on('message', (topic, message) => {

    receivedMessages++;
    lastReceivedAt = new Date().toISOString();

    console.log('======================================');
    console.log('MQTT DATA RECEIVED!');
    console.log('Topic:', topic);
    console.log('Message:', message.toString());
    console.log('======================================');

    processingQueue = processingQueue
        .then(() => processSensorData(topic, message))
        .catch(error => {
            console.error(
                'MESSAGE PROCESSING ERROR:',
                error.message
            );
        });
});

// ==========================================
// 10. PROCESS SENSOR DATA
// ==========================================

async function processSensorData(topic, message) {

    let data;

    try {
        data = JSON.parse(message.toString());
    } catch (error) {
        console.error('INVALID JSON:', error.message);
        return;
    }

    const waterLevel = Number(data.water_level);
    const sensorId = data.sensor_id || 'sensor1';
    const battery = Number(data.battery ?? 100);

    if (
        !Number.isFinite(waterLevel) ||
        !Number.isFinite(battery)
    ) {
        console.error('INVALID SENSOR DATA');
        return;
    }

    console.log('SENSOR ID:', sensorId);
    console.log('WATER LEVEL:', waterLevel, 'm');
    console.log('BATTERY:', battery, '%');

    // ======================================
    // 10A. SAVE TO INFLUXDB
    // ======================================

    try {

        const point = new Point('water_level_sensor')
            .tag('sensor_id', String(sensorId))
            .floatField('water_level_m', waterLevel)
            .floatField('battery_pct', battery);

        writeApi.writePoint(point);

        await writeApi.flush();

        console.log('INFLUXDB DATA SAVED SUCCESSFULLY');

    } catch (error) {
        console.error('INFLUXDB ERROR:', error.message);
    }

    // ======================================
    // 10B. SUPABASE THRESHOLD CHECK
    // ======================================

    try {

        const { data: areas, error } = await supabase
            .from('areas')
            .select('*')
            .order('min_threshold', { ascending: true });

        if (error) throw error;

        if (!areas || areas.length === 0) {
            console.log('NO AREAS FOUND IN SUPABASE');
            return;
        }

        let maxCrossedThreshold = 0;

        for (const area of areas) {

            const threshold = Number(area.min_threshold);

            if (
                Number.isFinite(threshold) &&
                waterLevel >= threshold
            ) {
                maxCrossedThreshold = Math.max(
                    maxCrossedThreshold,
                    threshold
                );
            }
        }

        for (const area of areas) {

            const areaName = area.area_name;
            const threshold = Number(area.min_threshold);

            if (!Number.isFinite(threshold)) {
                continue;
            }

            const lastState =
                activeAlerts[area.id] ?? 'NORMAL';

            if (waterLevel >= threshold) {

                if (
                    lastState === 'NORMAL' ||
                    (
                        typeof lastState === 'number' &&
                        maxCrossedThreshold > lastState
                    )
                ) {

                    const direction =
                        lastState === 'NORMAL'
                            ? 'RISEN'
                            : 'UPDATE_RISEN';

                    console.log(
                        'FLOOD ALERT:',
                        areaName,
                        waterLevel
                    );

                    await notifyAreaUsers(
                        area,
                        waterLevel,
                        direction
                    );
                }

                activeAlerts[area.id] = maxCrossedThreshold;

            } else if (lastState !== 'NORMAL') {

                console.log(
                    'WATER LEVEL DROPPED:',
                    areaName
                );

                await notifyAreaUsers(
                    area,
                    waterLevel,
                    'DROPPED'
                );

                activeAlerts[area.id] = 'NORMAL';
            }
        }

    } catch (error) {
        console.error('SUPABASE ERROR:', error.message);
    }
}

// ==========================================
// 11. GET USERS FROM SUPABASE
// ==========================================

async function notifyAreaUsers(
    area,
    waterLevel,
    direction
) {

    try {

        const { data: users, error } = await supabase
            .from('users')
            .select('phone_number')
            .eq('area_id', area.id);

        if (error) throw error;

        const phoneNumbers = (users || [])
            .map(user => user.phone_number)
            .filter(Boolean);

        if (phoneNumbers.length === 0) {
            console.log('NO USERS FOUND:', area.area_name);
            return;
        }

        await sendCustomSMS(
            area.area_name,
            waterLevel,
            phoneNumbers,
            direction
        );

    } catch (error) {
        console.error('USER FETCH ERROR:', error.message);
    }
}

// ==========================================
// 12. NOTIFY.LK SMS
// ==========================================

async function sendCustomSMS(
    areaName,
    level,
    phoneNumbers,
    direction
) {

    let messageText;

    if (direction === 'RISEN') {

        messageText =
            `FLOOD WARNING: ${areaName}. Water level: ${level}m. Please stay alert and follow official instructions.`;

    } else if (direction === 'UPDATE_RISEN') {

        messageText =
            `FLOOD ESCALATION: ${areaName}. Water level: ${level}m. Follow official emergency instructions.`;

    } else {

        messageText =
            `WATER LEVEL DROP: ${areaName}. Current level: ${level}m. Follow official updates.`;
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

            console.log('SMS RESPONSE:', response.data);

        } catch (error) {

            console.error(
                'SMS ERROR:',
                error.response?.data || error.message
            );
        }
    }
}

// ==========================================
// 13. GRACEFUL SHUTDOWN
// ==========================================

let shuttingDown = false;

async function shutdown() {

    if (shuttingDown) return;
    shuttingDown = true;

    console.log('SERVER SHUTTING DOWN...');

    client.end(true);

    try {
        await writeApi.close();
    } catch (error) {
        console.error('INFLUX CLOSE ERROR:', error.message);
    }

    healthServer.close(() => {
        console.log('HTTP SERVER CLOSED');
        process.exit(0);
    });

    setTimeout(() => {
        console.error('FORCED SHUTDOWN');
        process.exit(1);
    }, 10000).unref();
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// ==========================================
// 14. GLOBAL ERROR HANDLERS
// ==========================================

process.on('uncaughtException', error => {
    console.error('UNCAUGHT EXCEPTION:', error);
});

process.on('unhandledRejection', error => {
    console.error('UNHANDLED REJECTION:', error);
});

console.log('HYDROSENTINEL INITIALIZATION COMPLETED');
