import os
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from typing import Optional, List, Dict
from pathlib import Path

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

POSTHOG_API_KEY = os.getenv("POSTHOG_API_KEY", "")
POSTHOG_PROJECT_ID = os.getenv("POSTHOG_PROJECT_ID", "")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


class PostHogConnector:
    """Connector for PostHog Query API"""

    def __init__(self, api_key: str, project_id: str):
        self.api_key = api_key
        self.project_id = project_id
        self.base_url = f"https://us.posthog.com/api/projects/{self.project_id}/query/"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def fetch_events_batch(self, since: datetime, until: datetime, batch_size: int = BATCH_SIZE, offset: int = 0) -> List[Dict]:
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

    def insert_events(self, events: List[Dict], table_name: str = "raw.posthog_events"):
        if not events:
            print("No events to insert.")
            return 0

        df = pd.DataFrame(events)

        with duckdb.connect(self.db_path) as conn:
            # Create table if not exists
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM df LIMIT 1
            """)
            # Insert rows
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            return len(events)

    def delete_events(self, condition: Optional[str] = None, table_name: str = "raw.posthog_events"):
        with duckdb.connect(self.db_path) as conn:
            schema, table = table_name.split(".")

            try:
                # Use DuckDB's system catalog to check for table existence
                result = conn.execute(f"""
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table}'
                    LIMIT 1;
                """).fetchone()

                if not result:
                    print(f"Table {table_name} does not exist. Skipping delete.")
                    return

                # Proceed with deletion
                if condition:
                    query = f"DELETE FROM {table_name} WHERE {condition};"
                else:
                    query = f"DELETE FROM {table_name};"
                conn.execute(query)
                print(f"Deleted events from {table_name} with condition: {condition or 'ALL'}")

            except Exception as e:
                print(f"Unexpected error while deleting events: {e}")
                raise


class BatchProcessor:
    """Main batch processing orchestrator"""

    def __init__(self):
        self.posthog = PostHogConnector(POSTHOG_API_KEY, POSTHOG_PROJECT_ID)
        self.duckdb = DuckDBHandler(DUCKDB_PATH)

    def run_daily_sync(self, target_date: Optional[datetime] = None):
        if target_date is None:
            target_date = datetime.now(UTC)

        since = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        until = since + timedelta(days=1)

        print(f"Processing events for {since} → {until}")

        # 1. Clean existing rows for this day
        self.duckdb.delete_events(
            condition=f"timestamp >= '{since.isoformat()}' AND timestamp < '{until.isoformat()}'"
        )

        # 2. Fetch from PostHog
        all_events = []
        offset = 0
        while True:
            events = self.posthog.fetch_events_batch(since, until, BATCH_SIZE, offset)
            if not events:
                break
            all_events.extend(events)
            offset += BATCH_SIZE

        # 3. Insert into DuckDB
        inserted = self.duckdb.insert_events(all_events)
        print(f"Inserted {inserted} events.")
        return inserted


if __name__ == "__main__":
    processor = BatchProcessor()
    n_events = processor.run_daily_sync()
    print(f"Total events loaded: {n_events}")
