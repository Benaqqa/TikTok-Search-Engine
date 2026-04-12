# TikTok Video Search Engine

A real-time, event-driven pipeline that scrapes TikTok videos, downloads their audio, transcribes speech to text, and indexes the results for full-text search.

## Architecture

```
TikTok Feed
    │
    ▼
[tiktok-scraper]          Puppeteer + Stealth plugin
Scrolls home feed         Streams each URL instantly
    │
    ▼ Kafka topic: tiktok-video-links
    │
    ▼
[yt-dlp-consumer]         Downloads audio only (mp3)
Node.js + yt-dlp          Filename: username__videoId.mp3
    │
    ▼ Shared volume: audio-data
    │
    ▼
[whisper-transcriber]     OpenAI Whisper (local, free)
Python                    Polls volume, transcribes, deletes mp3
    │
    ├──▶ Kafka topic: transcriptions
    ├──▶ Elasticsearch index: tiktok-transcriptions
    └──▶ JSON file: videoId.json
```

## Services

| Service | Stack | Role |
|---|---|---|
| `tiktok-scraper` | Node.js, Puppeteer, KafkaJS | Scrapes TikTok home feed, streams URLs to Kafka |
| `yt-dlp-consumer` | Node.js, yt-dlp | Consumes URLs, downloads audio only |
| `whisper-transcriber` | Python, Whisper, kafka-python, Elasticsearch | Transcribes audio, publishes results |
| `redpanda` | Redpanda (Kafka-compatible) | Message broker between services |
| `redpanda-console` | Redpanda Console | Web UI for topic/message monitoring |
| `elasticsearch` | Elasticsearch 8.x | Full-text search index for transcriptions |

## Key Design Decisions

- **Audio-only download** — ~10x smaller than video; Whisper only needs the audio track
- **Filename encoding** — `username__videoId.mp3` encodes the full TikTok URL in the filename; no sidecar files or database lookups needed to reconstruct the source link
- **Local Whisper model** — runs entirely offline, no API key, no cost
- **Claim-check pattern** — Kafka carries lightweight URL messages; heavy files stay on a shared Docker volume
- **Idempotent ES indexing** — `video_id` is used as the Elasticsearch document ID, so reprocessing a video never creates duplicates

## Transcript Output

Each processed video produces a JSON document:

```json
{
  "video_id": "7604144177886137622",
  "username": "someuser",
  "url": "https://www.tiktok.com/@someuser/video/7604144177886137622",
  "language": "en",
  "transcript": "Full transcribed speech text...",
  "segments": [
    { "start": 0.0, "end": 2.5, "text": "First sentence." }
  ],
  "processed_at": "2026-04-09T18:05:00Z"
}
```

## Running Locally

```bash
# 1. Export TikTok cookies to scraper/cookies.json

# 2. Start all services
docker-compose up --build

# 3. Monitor topics
open http://localhost:8080        # Redpanda Console

# 4. Search transcriptions
curl http://localhost:9200/tiktok-transcriptions/_search?pretty \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"transcript": "your search term"}}}'
```

## Environment Variables

| Variable | Service | Default | Description |
|---|---|---|---|
| `KAFKA_BROKERS` | all | `redpanda:29092` | Kafka broker address |
| `KAFKA_TOPIC` | scraper, yt-dlp | `tiktok-video-links` | Input topic |
| `MAX_VIDEOS` | scraper | `100` | Max videos per run |
| `COOKIES_PATH` | scraper | `/app/cookies.json` | TikTok session cookies |
| `OUTPUT_PATH` | yt-dlp | `/output` | Audio file destination |
| `WHISPER_MODEL` | whisper | `medium` | Model size (tiny/small/medium/large) |
| `AUDIO_PATH` | whisper | `/audio` | Shared volume mount |
| `ES_HOST` | whisper | `http://elasticsearch:9200` | Elasticsearch URL |
| `ES_INDEX` | whisper | `tiktok-transcriptions` | Index name |
| `POLL_INTERVAL` | whisper | `5` | Seconds between volume scans |

## Tech Stack

- **Containerization** — Docker, Docker Compose
- **Message Broker** — Redpanda (Kafka-compatible)
- **Scraping** — Puppeteer, puppeteer-extra-plugin-stealth
- **Audio Download** — yt-dlp with curl-cffi (TikTok impersonation)
- **Transcription** — OpenAI Whisper (local)
- **Search** — Elasticsearch 8.x
- **Languages** — Node.js (scraper, downloader), Python (transcriber)
