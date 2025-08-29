import os
import requests
import duckdb
import pandas as pd
import json
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from typing import Optional, List, Dict
from pathlib import Path

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

POSTHOG_API_KEY = os.getenv("POSTHOG_API_KEY", "")
POSTHOG_PROJECT_ID = os.getenv("POSTHOG_PROJECT_ID", "")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
RAW_DIR = Path("data/raw/posthog")  # Bronze layer root


class PostHogConnector:
    """Connector for PostHog Query API with parquet file creation"""

    def __init__(self, api_key: str, project_id: str):
        self.api_key = api_key
        self.project_id = project_id
        self.base_url = f"https://us.posthog.com/api/projects/{self.project_id}/query/"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def fetch_events_batch(
        self, since: datetime, until: datetime,
        batch_size: int = BATCH_SIZE, offset: int = 0
    ) -> List[Dict]:
        since_str = since.strftime('%Y-%m-%d %H:%M:%S')
        until_str = until.strftime('%Y-%m-%d %H:%M:%S')

        query_body = {
            "query": {
                "kind": "HogQLQuery",
                "query": f"""
                    SELECT *
                    FROM events
                    WHERE timestamp >= '{since_str}'
                      AND timestamp < '{until_str}'
                    ORDER BY timestamp
                    LIMIT {batch_size} OFFSET {offset}
                """
            }
        }

        try:
            response = requests.post(self.base_url, headers=self.headers, json=query_body)
            response.raise_for_status()
            data = response.json()
            columns = data.get("columns", [])
            results = data.get("results", [])
            return [dict(zip(columns, row)) for row in results]
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return []

    def save_events_to_parquet(self, target_date: datetime) -> int:
        """Fetch events for a date and save as parquet files"""
        since = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        until = since + timedelta(days=1)

        # Output directory like ./data/raw/posthog/2025/08/25/
        out_dir = RAW_DIR / f"{since.year}/{since.month:02d}/{since.day:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        batch_idx = 0
        total_events = 0
        
        while True:
            events = self.fetch_events_batch(since, until, BATCH_SIZE, batch_idx * BATCH_SIZE)
            if not events:
                break
                
            df = pd.DataFrame(events)
            if 'properties' in df.columns:
                df['properties'] = df['properties'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            parquet_path = out_dir / f"batch_{batch_idx:04d}.parquet"
            df.to_parquet(parquet_path, index=False)
            print(f"Saved {len(df)} events → {parquet_path}")
            total_events += len(df)
            batch_idx += 1

        return total_events


class DuckDBHandler:
    """DuckDB warehouse operations with schema management"""

    def __init__(self, db_path: str = DUCKDB_PATH):
        self.db_path = db_path
        Path(db_path).parent.mkdir(exist_ok=True)
        self._initialize_schemas()

    def _initialize_schemas(self):
        with duckdb.connect(self.db_path) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    def create_external_view(self, target_date: datetime):
        """Register Parquet files in DuckDB by date"""
        path = RAW_DIR / f"{target_date.year}/{target_date.month:02d}/{target_date.day:02d}/*.parquet"
        with duckdb.connect(self.db_path) as conn:
            conn.execute(f"""
                CREATE OR REPLACE VIEW raw.posthog_events AS
                SELECT * FROM read_parquet('{path}')
            """)
        print(f"Created view raw.posthog_events over {path}")


class BatchProcessor:
    """Main batch processing orchestrator"""

    def __init__(self):
        self.posthog = PostHogConnector(POSTHOG_API_KEY, POSTHOG_PROJECT_ID)
        self.duckdb = DuckDBHandler(DUCKDB_PATH)

    def run_daily_sync(self, target_date: Optional[datetime] = None):
        if target_date is None:
            target_date = datetime.now(UTC)

        since = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        print(f"Processing events for {since.date()}")

        # Save events to parquet files
        total_events = self.posthog.save_events_to_parquet(target_date)

        # Register files into DuckDB as external view
        # if total_events > 0:
        #     self.duckdb.create_external_view(since)

        print(f"Total events saved for {since.date()}: {total_events}")
        return total_events


if __name__ == "__main__":
    processor = BatchProcessor()
    n_events = processor.run_daily_sync()
    print(f"Total events processed: {n_events}")