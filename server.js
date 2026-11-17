
require('dotenv').config();

const http = require('http');
const mqtt = require('mqtt');
const axios = require('axios');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { createClient } = require('@supabase/supabase-js');

console.log('======================================');
console.log('HYDROSENTINEL SERVER STARTING...');
console.log('======================================');

// ==========================================
// 1. ENVIRONMENT VARIABLES
// ==========================================

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
    console.error('MISSING VARIABLES:', missingVariables.join(', '));
    process.exit(1);
}

console.log('Environment variables check: PASSED');

// ==========================================
// 2. SUPABASE
// ==========================================

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY
);

console.log('Supabase client initialized');

// ==========================================
// 3. INFLUXDB
// ==========================================

const influxDB = new InfluxDB({
    url: process.env.INFLUX_URL,
    token: process.env.INFLUX_TOKEN
});

const writeApi = influxDB.getWriteApi(
    process.env.INFLUX_ORG,
    process.env.INFLUX_BUCKET
);

console.log('InfluxDB client initialized');

// ==========================================
// 4. HTTP HEALTH SERVER
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

        res.end('HydroSentinel Flood Alert Server is Running!');
    }
});

healthServer.listen(PORT, '0.0.0.0', () => {
    console.log(`HTTP SERVER RUNNING ON PORT ${PORT}`);
});

// ==========================================
// 5. MQTT CONNECTION
// ==========================================

console.log('Connecting to HiveMQ Cloud...');

const mqttOptions = {
    port: Number(process.env.MQTT_PORT),
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS,
    protocol: 'mqtts',
    rejectUnauthorized: true,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clientId: 'HydroSentinel_' +
        Math.random().toString(16).substring(2, 10)
};

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
        (error) => {

            if (error) {
                console.error('MQTT SUBSCRIBE ERROR:', error.message);
            } else {
                console.log(
                    'SUBSCRIBED TO:',
                    process.env.MQTT_TOPIC
                );
            }
        }
    );
});

client.on('error', (error) => {
    console.error('MQTT ERROR:', error.message);
});

client.on('reconnect', () => {
    console.log('MQTT RECONNECTING...');
});

client.on('offline', () => {
    mqttConnected = false;
    console.log('MQTT CLIENT OFFLINE');
});

client.on('close', () => {
    mqttConnected = false;
    console.log('MQTT CONNECTION CLOSED');
});

// ==========================================
// 6. ALERT STATE
// ==========================================

let activeAlerts = {};

// ==========================================
// 7. MQTT MESSAGE RECEIVING
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
            console.error('PROCESSING ERROR:', error.message);
        });
});

// ==========================================
// 8. PROCESS SENSOR DATA
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
        console.error('INVALID SENSOR VALUES');
        return;
    }

    console.log('SENSOR ID:', sensorId);
    console.log('WATER LEVEL:', waterLevel);
    console.log('BATTERY:', battery);

    // ======================================
    // 8A. SAVE DATA TO INFLUXDB
    // ======================================

    try {

        const point = new Point('water_level_sensor')
            .tag('sensor_id', String(sensorId))
            .floatField('water_level_m', waterLevel)
            .floatField('battery_pct', battery);

        writeApi.writePoint(point);
        await writeApi.flush();

        console.log('INFLUXDB: DATA SAVED');

    } catch (error) {
        console.error('INFLUXDB ERROR:', error.message);
    }

    // ======================================
    // 8B. GET THRESHOLDS FROM SUPABASE
    // ======================================

    try {

        const { data: areas, error } = await supabase
            .from('areas')
            .select('*')
            .order('min_threshold', { ascending: true });

        if (error) throw error;

        if (!areas || areas.length === 0) {
            console.log('NO AREAS FOUND');
            return;
        }

        // Highest threshold crossed by current water level
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

        // Process each configured area
        for (const area of areas) {

            const areaName = area.area_name;
            const threshold = Number(area.min_threshold);

            const lastState =
                activeAlerts[area.id] ?? 'NORMAL';

            if (!Number.isFinite(threshold)) {
                continue;
            }

            // WATER LEVEL ABOVE THRESHOLD
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
// 9. GET USERS AND SEND SMS
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
            console.log('NO USERS FOR AREA:', area.area_name);
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
// 10. NOTIFY.LK SMS
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
            `WATER LEVEL DROP: ${areaName}. Current level: ${level}m. Please follow official updates.`;
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
                'SMS RESPONSE:',
                response.data
            );

        } catch (error) {

            console.error(
                'SMS ERROR:',
                error.response?.data || error.message
            );
        }
    }
}

// ==========================================
// 11. SHUTDOWN
// ==========================================

async function shutdown() {

    console.log('SERVER SHUTTING DOWN...');

    try {

        client.end(true);

        await writeApi.close();

        healthServer.close(() => {
            console.log('HTTP SERVER CLOSED');
            process.exit(0);
        });

    } catch (error) {

        console.error('SHUTDOWN ERROR:', error.message);
        process.exit(1);
    }
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

process.on('uncaughtException', error => {
    console.error('UNCAUGHT EXCEPTION:', error);
});

process.on('unhandledRejection', error => {
    console.error('UNHANDLED REJECTION:', error);
});

console.log('HYDROSENTINEL INITIALIZATION COMPLETED');
