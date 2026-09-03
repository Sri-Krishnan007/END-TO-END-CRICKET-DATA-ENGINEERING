import os
import json
import time
import zipfile
import pandas as pd
from pathlib import Path
from config.config import BRONZE_PATH, STAGING_PATH, LOGS_PATH, clean_team_name

ZIP_PATH = BRONZE_PATH / "source" / "ipl_json.zip"
QUEUE_FILE = LOGS_PATH / "mock_kafka_queue.json"

# Attempt to import confluent_kafka, fallback to Mock if unavailable
try:
    from confluent_kafka import Producer, Consumer, KafkaError
    print("[KAFKA] Using real confluent_kafka library.")
    USE_MOCK = False
except ImportError:
    print("[KAFKA] confluent_kafka not installed. Using mock simulated Kafka broker.")
    USE_MOCK = True


# ==========================================
# 1. MOCK KAFKA IMPLEMENTATION
# ==========================================

class MockProducer:
    def __init__(self, configs=None):
        self.configs = configs or {}
        print("[MOCK KAFKA PRODUCER] Initialized simulated Kafka Producer.")
        # Ensure queue file is reset / exists
        if not QUEUE_FILE.parent.exists():
            QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)

    def produce(self, topic, key=None, value=None, callback=None):
        try:
            if QUEUE_FILE.exists():
                with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                    queue = json.load(f)
            else:
                queue = []
        except Exception:
            queue = []

        message = {
            "topic": topic,
            "key": key,
            "value": value if isinstance(value, str) else json.dumps(value),
            "timestamp": time.time(),
            "offset": len(queue)
        }
        queue.append(message)

        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(queue, f, indent=2)

        if callback:
            class MockRecordMetadata:
                def topic(self): return topic
                def offset(self): return message["offset"]
            callback(None, MockRecordMetadata())

    def flush(self, timeout=None):
        print("[MOCK KAFKA PRODUCER] Flushed simulated messages.")
        return 0


class MockConsumer:
    def __init__(self, configs=None):
        self.configs = configs or {}
        self.subscribed_topics = []
        self.current_index = 0
        print("[MOCK KAFKA CONSUMER] Initialized simulated Kafka Consumer.")

    def subscribe(self, topics):
        self.subscribed_topics = topics
        self.current_index = 0
        print(f"[MOCK KAFKA CONSUMER] Subscribed to simulated topics: {topics}")

    def poll(self, timeout=1.0):
        if not QUEUE_FILE.exists():
            return None
        
        try:
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                queue = json.load(f)
        except Exception:
            return None

        valid_messages = [msg for msg in queue if msg["topic"] in self.subscribed_topics]
        
        if self.current_index < len(valid_messages):
            msg_data = valid_messages[self.current_index]
            self.current_index += 1
            
            class MockMessage:
                def __init__(self, data):
                    self._topic = data["topic"]
                    self._key = data["key"]
                    self._value = data["value"].encode("utf-8")
                    self._error = None
                    self._offset = data["offset"]

                def topic(self): return self._topic
                def key(self): return self._key
                def value(self): return self._value
                def error(self): return self._error
                def offset(self): return self._offset
                    
            return MockMessage(msg_data)
        
        time.sleep(0.01)
        return None

    def commit(self, asynchronous=True):
        pass

    def close(self):
        print("[MOCK KAFKA CONSUMER] Simulated consumer closed.")


# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def get_match_season(match_data):
    """Extracts the season year from the match JSON."""
    info = match_data.get("info", {})
    season = info.get("season")
    if not season:
        dates = info.get("dates", [])
        if dates:
            season = dates[0].split("-")[0]
            
    if season:
        season_str = str(season).strip()
        if "/" in season_str:
            season_str = season_str.split("/")[0]
        return season_str
    return None


def delivery_report(err, msg):
    """Callback triggered on successful message delivery."""
    if err is not None:
        print(f"[KAFKA PRODUCER ERROR] Delivery failed: {err}")
    else:
        # Avoid printing for every single message to keep output clean, but verify in logs
        pass


# ==========================================
# 3. PRODUCER RUNNER
# ==========================================

def run_producer(target_seasons, target_team=None, target_player=None):
    """
    Reads IPL JSON match files from the source ZIP file and publishes
    filtered matches to the Kafka topic 'ipl_matches'.
    """
    target_seasons_str = [str(s) for s in target_seasons]
    
    # Initialize Kafka configurations
    conf = {
        'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        'client.id': 'ipl-producer',
        'queue.buffering.max.messages': 100000
    }

    # Decide whether to use real Kafka or mock
    producer = None
    if not USE_MOCK:
        try:
            producer = Producer(conf)
            # Try to fetch metadata to verify if broker is actually reachable
            producer.list_topics(timeout=1.0)
        except Exception as e:
            print(f"[KAFKA WARNING] Real broker connection failed: {e}. Falling back to Mock.")
            producer = MockProducer(conf)
    else:
        producer = MockProducer(conf)

    if not ZIP_PATH.exists():
        raise FileNotFoundError(f"Source ZIP file not found at {ZIP_PATH}. Please run ingestion stage first.")

    sent_count = 0
    print(f"[KAFKA PRODUCER] Processing ZIP files for seasons {target_seasons_str}...")

    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        for zip_info in z.infolist():
            if zip_info.filename.endswith('.json') and not zip_info.is_dir():
                with z.open(zip_info) as f:
                    try:
                        content = f.read().decode('utf-8')
                        match_data = json.loads(content)
                    except Exception as e:
                        print(f"[KAFKA PRODUCER ERROR] Failed to parse {zip_info.filename}: {e}")
                        continue

                season = get_match_season(match_data)
                if season in target_seasons_str:
                    info = match_data.get("info", {})
                    
                    # Apply team filters
                    if target_team:
                        teams = [clean_team_name(t) for t in info.get("teams", [])]
                        if clean_team_name(target_team) not in teams:
                            continue
                            
                    # Apply player filters
                    if target_player:
                        players_dict = info.get("players", {})
                        found_player = False
                        for team_name, squad in players_dict.items():
                            if target_player in squad:
                                found_player = True
                                break
                        if not found_player:
                            continue

                    match_id = Path(zip_info.filename).stem
                    payload = {
                        "match_id": match_id,
                        "season": season,
                        "raw_data": match_data
                    }

                    # Publish to Kafka
                    producer.produce(
                        topic='ipl_matches',
                        key=match_id,
                        value=json.dumps(payload),
                        callback=delivery_report
                    )
                    sent_count += 1

    producer.flush()
    print(f"[KAFKA PRODUCER] Published {sent_count} matches to topic 'ipl_matches'.")
    return sent_count


# ==========================================
# 4. CONSUMER RUNNER
# ==========================================

def run_consumer(target_seasons):
    """
    Consumes matches from the 'ipl_matches' topic and writes them
    to their respective season staging folders in JSON and Parquet formats.
    """
    target_seasons_str = [str(s) for s in target_seasons]
    
    conf = {
        'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        'group.id': 'ipl-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = None
    is_mock = False
    if not USE_MOCK:
        try:
            consumer = Consumer(conf)
            consumer.list_topics(timeout=1.0)
        except Exception as e:
            print(f"[KAFKA WARNING] Real broker connection failed: {e}. Falling back to Mock.")
            consumer = MockConsumer(conf)
            is_mock = True
    else:
        consumer = MockConsumer(conf)
        is_mock = True

    consumer.subscribe(['ipl_matches'])
    consumed_count = 0
    empty_polls = 0
    max_empty_polls = 10 if not is_mock else 3

    print(f"[KAFKA CONSUMER] Consuming matches for seasons {target_seasons_str}...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    # If we poll nothing for multiple seconds, we assume we have finished the batch
                    break
                continue
            
            empty_polls = 0

            # Error handling for confluent_kafka
            if not is_mock and msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[KAFKA CONSUMER ERROR] Message error: {msg.error()}")
                    break

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                match_id = payload.get("match_id")
                season = payload.get("season")
                raw_data = payload.get("raw_data")

                if not match_id or not season or not raw_data:
                    print("[KAFKA CONSUMER WARNING] Received incomplete payload. Skipping.")
                    continue

                if season in target_seasons_str:
                    # Write to JSON Staging
                    dest_json_dir = STAGING_PATH / season / "json"
                    dest_json_dir.mkdir(parents=True, exist_ok=True)
                    dest_json_path = dest_json_dir / f"{match_id}.json"
                    
                    with open(dest_json_path, 'w', encoding='utf-8') as df:
                        json.dump(raw_data, df, indent=2)
                    
                    # Write to Parquet Staging
                    dest_parquet_dir = STAGING_PATH / season / "parquet"
                    dest_parquet_dir.mkdir(parents=True, exist_ok=True)
                    dest_parquet_path = dest_parquet_dir / f"{match_id}.parquet"
                    
                    df_raw = pd.DataFrame([{
                        "match_id": match_id,
                        "season": season,
                        "raw_data": json.dumps(raw_data)
                    }])
                    df_raw.to_parquet(dest_parquet_path, index=False)
                    
                    consumed_count += 1
                
                if not is_mock:
                    consumer.commit(message=msg, asynchronous=False)
                    
            except Exception as e:
                print(f"[KAFKA CONSUMER ERROR] Failed to process message: {e}")

    except KeyboardInterrupt:
        print("[KAFKA CONSUMER] Consumer stopped by user.")
    finally:
        consumer.close()

    print(f"[KAFKA CONSUMER] Successfully staged {consumed_count} matches.")
    return consumed_count
