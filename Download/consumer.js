const { Kafka } = require('kafkajs');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const KAFKA_BROKERS  = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_TOPIC    = process.env.KAFKA_TOPIC    || 'tiktok-video-links';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'yt-dlp-consumer';
const OUTPUT_PATH    = process.env.OUTPUT_PATH    || '/output';

const kafka    = new Kafka({ clientId: 'yt-dlp-consumer', brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

if (!fs.existsSync(OUTPUT_PATH)) {
  fs.mkdirSync(OUTPUT_PATH, { recursive: true });
}

// ── Extract username from TikTok URL ──
// "https://www.tiktok.com/@idabou_01/video/7604144177886137622" → "idabou_01"
function extractUsername(url) {
  const match = url.match(/tiktok\.com\/@([^/]+)\/video/);
  return match ? match[1] : 'unknown';
}

// ── Filename encodes both username and videoId so the link is fully reconstructable ──
// Format: {username}__{videoId}.mp3
// Whisper reads: "idabou_01__7604144177886137622.mp3"
//           → "https://www.tiktok.com/@idabou_01/video/7604144177886137622"
async function downloadAudio(url, videoId) {
  try {
    console.log(`\n[${new Date().toISOString()}] Downloading audio: ${url}`);

    const username     = extractUsername(url);
    const audioName    = `${username}__${videoId}`;           // e.g. idabou_01__7604144177886137622
    const outputTemplate = path.join(OUTPUT_PATH, `${audioName}.%(ext)s`);

    const command = [
      'yt-dlp',
      '-f bestaudio/best',
      '--extract-audio',
      '--audio-format mp3',
      '--audio-quality 0',
      '--no-playlist',
      `-o "${outputTemplate}"`,
      `"${url}"`
    ].join(' ');

    execSync(command, { encoding: 'utf-8', stdio: 'pipe' });

    const audioFile = path.join(OUTPUT_PATH, `${audioName}.mp3`);
    console.log(`  ✓ Audio saved: ${audioFile}`);
    return { success: true, videoId, url, audioFile };
  } catch (error) {
    console.error(`  ✗ Failed to download audio for ${url}:`, error.message);
    return { success: false, videoId, url, error: error.message };
  }
}

async function start() {
  try {
    console.log(`Connecting to Kafka brokers: ${KAFKA_BROKERS.join(', ')}`);
    console.log(`Subscribing to topic: ${KAFKA_TOPIC}`);
    console.log(`Consumer group: ${KAFKA_GROUP_ID}`);
    console.log(`Audio output path: ${OUTPUT_PATH}`);

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
          console.log(`  Video ID : ${videoId}`);
          console.log(`  URL      : ${url}`);
          console.log(`  Source   : ${source}`);
          console.log(`  Timestamp: ${timestamp}`);

          const result = await downloadAudio(url, videoId);

          if (result.success) {
            console.log(`  → Audio ready for Whisper at: ${result.audioFile}`);
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