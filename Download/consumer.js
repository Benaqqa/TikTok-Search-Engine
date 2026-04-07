const { Kafka } = require('kafkajs');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'tiktok-video-links';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'yt-dlp-consumer';
const OUTPUT_PATH = process.env.OUTPUT_PATH || '/output';

// Initialize Kafka client
const kafka = new Kafka({
  clientId: 'yt-dlp-consumer',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_PATH)) {
  fs.mkdirSync(OUTPUT_PATH, { recursive: true });
}

async function downloadVideo(url, videoId) {
  try {
    console.log(`\n[${new Date().toISOString()}] Downloading: ${url}`);
    
    const outputTemplate = path.join(OUTPUT_PATH, `%(title)s-%(id)s.%(ext)s`);
    
    const command = `yt-dlp -f best -o "${outputTemplate}" "${url}"`;
    
    const output = execSync(command, { 
      encoding: 'utf-8',
      stdio: 'pipe'
    });
    
    console.log(`✓ Successfully downloaded: ${url}`);
    return { success: true, videoId, url };
  } catch (error) {
    console.error(`✗ Failed to download ${url}:`, error.message);
    return { success: false, videoId, url, error: error.message };
  }
}

async function start() {
  try {
    console.log(`Connecting to Kafka brokers: ${KAFKA_BROKERS.join(', ')}`);
    console.log(`Subscribing to topic: ${KAFKA_TOPIC}`);
    console.log(`Consumer group: ${KAFKA_GROUP_ID}`);
    console.log(`Output path: ${OUTPUT_PATH}`);
    
    await consumer.connect();
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    
    console.log('✓ Connected to Kafka and subscribed to topic');
    console.log('Waiting for messages...\n');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const messageContent = JSON.parse(message.value.toString());
          const { url, timestamp, source } = messageContent;
          const videoId = message.key?.toString() || 'unknown';
          
          console.log(`\nReceived message on partition ${partition}`);
          console.log(`  Video ID: ${videoId}`);
          console.log(`  URL: ${url}`);
          console.log(`  Source: ${source}`);
          console.log(`  Timestamp: ${timestamp}`);
          
          // Download the video
          const result = await downloadVideo(url, videoId);
          
          if (result.success) {
            console.log(`Video saved to: ${OUTPUT_PATH}`);
          }
        } catch (error) {
          console.error('Error processing message:', error.message);
        }
      },
    });
  } catch (error) {
    console.error('Fatal error:', error);
    await consumer.disconnect();
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down gracefully...');
  await consumer.disconnect();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nShutting down gracefully...');
  await consumer.disconnect();
  process.exit(0);
});

start().catch(error => {
  console.error('Error starting consumer:', error);
  process.exit(1);
});
