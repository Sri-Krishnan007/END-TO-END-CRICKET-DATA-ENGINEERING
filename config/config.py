import os
import json
from pathlib import Path

# Paths config
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_PATH = PROJECT_ROOT / "data"
BRONZE_PATH = DATA_PATH / "bronze"
STAGING_PATH = DATA_PATH / "staging"
SILVER_PATH = DATA_PATH / "silver"
GOLD_PATH = DATA_PATH / "gold"
QUARANTINE_PATH = DATA_PATH / "quarantine"
LOGS_PATH = PROJECT_ROOT / "logs"

# Ensure dirs exist
for path in [BRONZE_PATH / "source", STAGING_PATH, SILVER_PATH, GOLD_PATH, QUARANTINE_PATH, LOGS_PATH]:
    path.mkdir(parents=True, exist_ok=True)

CONFIG_STATE_FILE = PROJECT_ROOT / "config" / "db_config.json"

# Database Configuration
LOCAL_DB_URL = os.environ.get(
    "LOCAL_DATABASE_URL", 
    "postgresql://postgres:krish%40123@localhost:5432/postgres"
)
ONLINE_DB_URL = os.environ.get(
    "ONLINE_DATABASE_URL", 
    "postgresql://postgres.dfgjodtxiuepowmphotf:yWCKLOQ8znyxRCDD@aws-0-ap-southeast-2.pooler.supabase.com:5432/postgres"
)

def get_active_db_mode():
    """Returns 'online' (Supabase Cloud) or 'offline' (Local PostgreSQL)."""
    if CONFIG_STATE_FILE.exists():
        try:
            with open(CONFIG_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                mode = data.get("db_mode", "").lower()
                if mode in ["online", "offline", "local"]:
                    return "offline" if mode == "local" else mode
        except Exception:
            pass
    return os.environ.get("DB_MODE", "online").lower()

def set_active_db_mode(mode):
    """Sets active database mode to 'online' or 'offline'."""
    mode = "offline" if mode in ["local", "offline"] else "online"
    try:
        with open(CONFIG_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"db_mode": mode}, f, indent=2)
    except Exception as e:
        print(f"Failed to persist DB mode: {e}")
    os.environ["DB_MODE"] = mode
    return mode

def get_active_db_url():
    """Returns the connection URL based on currently active mode."""
    mode = get_active_db_mode()
    if mode == "offline":
        return LOCAL_DB_URL
    return ONLINE_DB_URL

# Default fallback
DATABASE_URL = get_active_db_url()
PGPASSWORD = os.environ.get("PGPASSWORD", "yWCKLOQ8znyxRCDD")

# Lakehouse Configuration (Week 2: Apache Iceberg + Nessie + MinIO)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_WAREHOUSE_BUCKET = os.environ.get("MINIO_WAREHOUSE_BUCKET", "warehouse")

NESSIE_URI = os.environ.get("NESSIE_URI", "http://localhost:19120/api/v2")
ICEBERG_CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "ipl")
ICEBERG_WAREHOUSE_PATH = os.environ.get("ICEBERG_WAREHOUSE_PATH", "s3a://warehouse/")
ICEBERG_CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "iceberg_streaming"
ICEBERG_TABLE_NAME = "ipl.ipl_matches"

# Ensure checkpoint directory exists
ICEBERG_CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)

def clean_team_name(name):
    """Maps historical/variant team names to their canonical representations."""
    if not name:
        return name
    name_clean = str(name).strip()
    mapping = {
        "Delhi Daredevils": "Delhi Capitals",
        "Kings XI Punjab": "Punjab Kings",
        "Rising Pune Supergiant": "Rising Pune Supergiants",
        "Royal Challengers Bangalore": "Royal Challengers Bengaluru"
    }
    return mapping.get(name_clean, name_clean)


