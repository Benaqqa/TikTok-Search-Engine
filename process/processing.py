import os
import time
import whisper
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


from elasticsearch import Elasticsearch

AUDIO_PATH      = os.environ.get('AUDIO_PATH', '/audio')
OUTPUT_PATH     = os.environ.get('OUTPUT_PATH', '/audio')
WHISPER_MODEL   = os.environ.get('WHISPER_MODEL', 'medium')
POLL_INTERVAL   = int(os.environ.get('POLL_INTERVAL', '5'))
KAFKA_BROKERS   = os.environ.get('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_OUT_TOPIC = os.environ.get('KAFKA_OUTPUT_TOPIC', 'transcriptions')
ES_HOST         = os.environ.get('ES_HOST', 'http://elasticsearch:9200')
ES_INDEX        = os.environ.get('ES_INDEX', 'tiktok-transcriptions')


def parse_filename(filename: str) -> tuple[str, str, str]:
    """
    "idabou_01__7604144177886137622.mp3"
     → username = "idabou_01"
     → video_id = "7604144177886137622"
     → url      = "https://www.tiktok.com/@idabou_01/video/7604144177886137622"
    """
    stem = filename.replace('.mp3', '')
    parts = stem.split('__', 1)
    if len(parts) == 2:
        username, video_id = parts
    else:
        username, video_id = 'unknown', stem
    url = f"https://www.tiktok.com/@{username}/video/{video_id}"
    return username, video_id, url


def transcribe(audio_file: str, model) -> dict:
    result = model.transcribe(audio_file, fp16=False)
    return {
        "text":     result["text"].strip(),
        "language": result.get("language", "unknown"),
        "segments": [
            {
                "start": round(s["start"], 2),
                "end":   round(s["end"], 2),
                "text":  s["text"].strip()
            }
            for s in result.get("segments", [])
        ]
    }


def connect_kafka() -> KafkaProducer:
    
    print(f"Connecting to Kafka brokers: {KAFKA_BROKERS}...")
    
    while True:
        
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
            try:
                admin.create_topics([
                    NewTopic(
                        name=KAFKA_OUT_TOPIC,
                        num_partitions=1,
                        replication_factor=1
                    )
                ])
                print(f"  ✓ Created Kafka topic: '{KAFKA_OUT_TOPIC}'")
            except TopicAlreadyExistsError:
                print(f"  ✓ Kafka topic already exists: '{KAFKA_OUT_TOPIC}'")
            finally:
                admin.close()

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
            print(f"Connected to Kafka — publishing to '{KAFKA_OUT_TOPIC}'")
            return producer
        except Exception as e:
            print(f"  Kafka not ready: {e} — retrying in 5s...")
            time.sleep(5)


def connect_elasticsearch() -> Elasticsearch:
    print(f"Connecting to Elasticsearch at {ES_HOST}...")
    while True:
        try:
            es = Elasticsearch(ES_HOST)
            if es.ping():
                print(f"Connected to Elasticsearch — index: '{ES_INDEX}'")
                # Create index with mapping if it doesn't exist
                if not es.indices.exists(index=ES_INDEX):
                    es.indices.create(index=ES_INDEX, body={
                        "mappings": {
                            "properties": {
                                "video_id":     { "type": "keyword" },
                                "username":     { "type": "keyword" },
                                "url":          { "type": "keyword" },
                                "language":     { "type": "keyword" },
                                "transcript":   { "type": "text"    },
                                "processed_at": { "type": "date"    },
                                "segments": {
                                    "type": "nested",
                                    "properties": {
                                        "start": { "type": "float" },
                                        "end":   { "type": "float" },
                                        "text":  { "type": "text"  }
                                    }
                                }
                            }
                        }
                    })
                    print(f"  Created index '{ES_INDEX}'")
                return es
        except Exception as e:
            print(f"  Elasticsearch not ready: {e} — retrying in 5s...")
            time.sleep(5)


def main():
    print(f"Loading Whisper model: '{WHISPER_MODEL}'...")
    model = whisper.load_model(WHISPER_MODEL)
    print(f"Whisper '{WHISPER_MODEL}' model loaded")
    print(f"Watching for audio files in: {AUDIO_PATH}")
    print(f"Saving transcripts to: {OUTPUT_PATH}\n")

    producer = connect_kafka()
    es       = connect_elasticsearch()

    processed = set()

    # Skip files already transcribed in a previous run
    for f in os.listdir(AUDIO_PATH):
        if f.endswith('.mp3'):
            _, video_id, _ = parse_filename(f)
            transcript_file = os.path.join(OUTPUT_PATH, f"{video_id}.json")
            if os.path.exists(transcript_file):
                processed.add(f)

    while True:
        mp3_files = [f for f in os.listdir(AUDIO_PATH) if f.endswith('.mp3')]
        pending   = [f for f in mp3_files if f not in processed]

        if not pending:
            time.sleep(POLL_INTERVAL)
            continue

        for filename in pending:
            audio_file = os.path.join(AUDIO_PATH, filename)

            username, video_id, url = parse_filename(filename)
            transcript_file = os.path.join(OUTPUT_PATH, f"{video_id}.json")

            # Wait until yt-dlp has finished writing (file size must be stable)
            prev_size = -1
            while True:
                curr_size = os.path.getsize(audio_file)
                if curr_size == prev_size and curr_size > 0:
                    break
                prev_size = curr_size
                time.sleep(1)

            print(f"\n[{time.strftime('%Y-%m-%dT%H:%M:%SZ')}] Processing: {filename}")
            print(f"  Username : @{username}")
            print(f"  Video ID : {video_id}")
            print(f"  URL      : {url}")
            print(f"  Size     : {os.path.getsize(audio_file) // 1024} KB")

            try:
                result = transcribe(audio_file, model)

                payload = {
                    "video_id":     video_id,
                    "username":     username,
                    "url":          url,
                    "language":     result["language"],
                    "transcript":   result["text"],
                    "segments":     result["segments"],
                    "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }

                # ── 1. Save JSON to disk ──
                with open(transcript_file, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                print(f"  Transcript saved : {transcript_file}")

                # ── 2. Stream to Kafka topic ──
                producer.send(KAFKA_OUT_TOPIC, key=video_id, value=payload)
                producer.flush()
                print(f"  Streamed to Kafka : '{KAFKA_OUT_TOPIC}'")

                # ── 3. Index into Elasticsearch ──
                es.index(index=ES_INDEX, id=video_id, document=payload)
                print(f"  Indexed in ES     : '{ES_INDEX}/{video_id}'")

                print(f"  Language  : {result['language']}")
                print(f"  Preview   : {result['text'][:100]}...")

                os.remove(audio_file)
                print(f"   Deleted audio  : {audio_file}")

                processed.add(filename)

            except Exception as e:
                print(f"  ✗ Failed to process {filename}: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()