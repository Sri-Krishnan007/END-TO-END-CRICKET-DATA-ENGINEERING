import os
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

# Database Configuration
DATABASE_URL = os.environ.get(
    "DATABASE_URL", 
    "postgresql://postgres:krish%40123@localhost:5432/postgres"
)
PGPASSWORD = os.environ.get("PGPASSWORD", "krish@123")

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

