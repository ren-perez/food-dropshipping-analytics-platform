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

# MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
# RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

def fetch_events(since: datetime, until: datetime):
    url = f"https://us.posthog.com/api/projects/{POSTHOG_PROJECT_ID}/query/"
    headers = {
        "Authorization": f"Bearer {POSTHOG_API_KEY}",
        "Content-Type": "application/json"
    }

    query_body = {
        "query": {
            "kind": "HogQLQuery",
            # you can add filtering with since/until if needed
            "query": "select * from events LIMIT 5"
        }
    }

    resp = requests.post(url, headers=headers, json=query_body)
    resp.raise_for_status()
    payload = resp.json()

    # PostHog returns columns + results
    columns = payload.get("columns", [])
    results = payload.get("results", [])

    # Turn into list of dicts
    events = [dict(zip(columns, row)) for row in results]
    return events

class PostHogConnector:
    def __init__(self, api_key: str, project_id: str):
        self.api_key = api_key
        self.project_id = project_id
        self.base_url = f"https://us.posthog.com/api/projects"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
    def fetch_events_batch(self, since: datetime, until: datetime, batch_size: int = BATCH_SIZE, offset: int = 0) -> List[Dict]:
        url = f"https://us.posthog.com/api/projects/{self.project_id}/query/"
        
        since_str = since.strftime('%Y-%m-%d %H:%M:%S')
        until_str = until.strftime('%Y-%m-%d %H:%M:%S')
        
        query_body = {
            "query": {
                "kind": "HogQLQuery",
                "query": f"""
                    select * 
                    from events
                    WHERE timestamp >= '{since_str}'
                    AND timestamp < '{until_str}'
                    ORDER BY timestamp
                    LIMIT {batch_size} OFFSET {offset}
                """
            }
        }
        
        return self._make_request(url, query_body)
        
    def _make_request(self, url:str, payload: Dict) -> List[Dict]:
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            columns = data.get("columns", [])
            results = data.get("results", [])
            
            return [dict(zip(columns, row)) for row in results]
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return []

class DuckDBHandler:
    def __init__(self, db_path: str = DUCKDB_PATH):
        self.db_path = db_path
        Path(db_path).parent.mkdir(exist_ok=True)
        self._initialize_schemas()
        
    def _initialize_schemas(self):
        with duckdb.connect(self.db_path) as conn:
            # Create schemas
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            # conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
            # conn.execute("CREATE SCHEMA IF NOT EXISTS metadata;")
            
    def upsert_events(self, events: List[Dict], table_name: str = "raw.posthog_events"):
        if not events:
            print("No events to upsert.")
            return 0
        
        df = pd.DataFrame(events)
        
        with duckdb.connect(self.db_path) as conn:
            # Create table if not exists
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM df LIMIT 0
            """)
            
            # Insert new rows
            try:
                conn.execute(f"INSERT OR IGNORE INTO {table_name} SELECT * FROM df")
                return len(events)
            except Exception as e:
                # Fallback to manual deduplication
                conn.execute(f"""
                             INSERT INTO {table_name}
                             SELECT * FROM df
                             WHERE NOT EXISTS (
                                SELECT 1 FROM {table_name} t
                                WHERE t.uuid = df.uuid
                            )
                            """)
                return len(events)

def load_into_duckdb(events):
    if not events:
        print("No events to load.")
        return

    # Convert to DataFrame (clean schema inference)
    df = pd.DataFrame(events)

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # Create or append into table
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.posthog_events AS 
        SELECT * FROM df
    """)
    # Insert new rows
    con.execute("INSERT INTO raw.posthog_events SELECT * FROM df")
    con.close()
    
def delete_events(condition: Optional[str] = None):
    con = duckdb.connect(DUCKDB_PATH)
    
    if condition:
        query = f"DELETE FROM raw.posthog_events WHERE {condition};"
    else:
        query = "DELETE FROM raw.posthog_events;"
    
    con.execute(query)
    con.close()
    
    
class BatchProcessor:
    def __init__(self):
        self.posthog = PostHogConnector(POSTHOG_API_KEY, POSTHOG_PROJECT_ID)
        self.duckdb = DuckDBHandler(DUCKDB_PATH)
        
    def run_daily_sync(self, target_date: Optional[datetime] = None):
        if target_date is None:
            target_date = datetime.now(UTC)
        
        since = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        until = since + timedelta(days=1)
        
        all_events = []
        offset = 0
        
        while True:
            events = self.posthog.fetch_events_batch(since, until, BATCH_SIZE, offset)
            
            if not events:
                break
            
            all_events.extend(events)
            offset += BATCH_SIZE
            
        events_loaded = self.duckdb.upsert_events(all_events)
        return events_loaded
        # return load_into_duckdb(all_events)

    
if __name__ == "__main__":
    # since = datetime.now(UTC) - timedelta(days=10)
    # until = datetime.now(UTC)
    # events = fetch_events(since, until)

    # if events:
    #     print(f"Fetched {len(events)} events")
    #     load_into_duckdb(events)
    # else:
    #     print("No new events")
    
    proccessor = BatchProcessor()
    n_events = proccessor.run_daily_sync()
    print(f"Total events loaded: {n_events}")
    

