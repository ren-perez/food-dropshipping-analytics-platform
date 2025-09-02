import os
import argparse
import logging
import requests
import boto3
import pandas as pd
import json
import io
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from typing import List, Dict, Optional
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

POSTHOG_API_KEY = os.getenv("POSTHOG_API_KEY", "")
POSTHOG_PROJECT_ID = os.getenv("POSTHOG_PROJECT_ID", "")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
RAW_DIR = Path("data/raw/posthog")  # Bronze layer root

# --- R2 Config ---
R2_BUCKET = os.getenv("R2_BUCKET", "posthog-raw")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")  # e.g. https://<accountid>.r2.cloudflarestorage.com
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

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
            logger.info(f"Fetching events from {since_str} to {until_str}, offset {offset}")
            response = requests.post(self.base_url, headers=self.headers, json=query_body)
            response.raise_for_status()
            data = response.json()
            columns = data.get("columns", [])
            results = data.get("results", [])
            return [dict(zip(columns, row)) for row in results]
        except requests.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            return []

    def _create_parquet_buffer(self, df: pd.DataFrame) -> io.BytesIO:
        """Create a parquet file buffer with Snappy compression"""
        buffer = io.BytesIO()
        df.to_parquet(
            buffer, 
            index=False, 
            compression='snappy',
            engine='pyarrow'
        )
        buffer.seek(0)
        return buffer

    def _upload_to_r2(self, buffer: io.BytesIO, relative_key: str) -> None:
        """Upload parquet buffer to R2"""
        try:
            s3.put_object(
                Bucket=R2_BUCKET,
                Key=f"posthog/{relative_key}",
                Body=buffer.getvalue()
            )
            logger.info(f"Uploaded to R2 → s3://{R2_BUCKET}/posthog/{relative_key}")
        except Exception as e:
            logger.error(f"Failed to upload to R2: {e}")
            raise

    def _save_to_local(self, df: pd.DataFrame, file_path: Path) -> None:
        """Save DataFrame to local parquet file with Snappy compression"""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(
            file_path, 
            index=False, 
            compression='snappy',
            engine='pyarrow'
        )
        logger.info(f"Saved {len(df)} events → {file_path}")

    def save_events_to_parquet(
        self, 
        target_date: datetime, 
        save_local: bool = False
    ) -> int:
        """
        Fetch events for a date and save to R2 (default) or local disk
        
        Args:
            target_date: Date to fetch events for
            save_local: If True, save to local disk. If False (default), save directly to R2
            
        Returns:
            Total number of events processed
        """
        since = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        until = since + timedelta(days=1)

        logger.info(f"Processing events for {since.strftime('%Y-%m-%d')}")
        logger.info(f"Storage mode: {'Local disk' if save_local else 'Direct to R2'}")

        batch_idx = 0
        total_events = 0
        
        while True:
            events = self.fetch_events_batch(since, until, BATCH_SIZE, batch_idx * BATCH_SIZE)
            if not events:
                logger.info(f"No more events found. Processed {batch_idx} batches.")
                break
                
            # Process DataFrame
            df = pd.DataFrame(events)
            if 'properties' in df.columns:
                df['properties'] = df['properties'].apply(
                    lambda x: json.dumps(x) if isinstance(x, dict) else x
                )

            # Generate file path/key
            relative_key = f"{since.year}/{since.month:02d}/{since.day:02d}/batch_{batch_idx+1:04d}.parquet"
            
            if save_local:
                # Save to local disk
                local_path = RAW_DIR / relative_key
                self._save_to_local(df, local_path)
                
                # Also upload to R2
                parquet_buffer = self._create_parquet_buffer(df)
                self._upload_to_r2(parquet_buffer, relative_key)
            else:
                # Save directly to R2 (default behavior)
                parquet_buffer = self._create_parquet_buffer(df)
                self._upload_to_r2(parquet_buffer, relative_key)
                logger.info(f"Processed {len(df)} events (batch {batch_idx+1})")

            total_events += len(df)
            batch_idx += 1

        return total_events


def parse_args():
    parser = argparse.ArgumentParser(description="PostHog Events Extractor")
    parser.add_argument(
        "--save-local", 
        action="store_true",
        help="Save files to local disk in addition to R2 (default: save only to R2)"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=0,
        help="Number of days back from today to process (default: 8)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Specific date to process (YYYY-MM-DD format). Overrides --days-back"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Validate required environment variables
    if not POSTHOG_API_KEY or not POSTHOG_PROJECT_ID:
        logger.error("POSTHOG_API_KEY and POSTHOG_PROJECT_ID must be set")
        return 1
    
    if not args.save_local and not all([R2_BUCKET, R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY]):
        logger.error("R2 configuration is required when not saving locally")
        return 1

    # Determine target date
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=UTC)
            logger.info(f"Processing specific date: {args.date}")
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            return 1
    else:
        target_date = datetime.now(UTC) - timedelta(days=args.days_back)
        logger.info(f"Processing date {args.days_back} days back: {target_date.strftime('%Y-%m-%d')}")

    # Execute extraction
    try:
        connector = PostHogConnector(POSTHOG_API_KEY, POSTHOG_PROJECT_ID)
        events_count = connector.save_events_to_parquet(target_date, save_local=args.save_local)
        logger.info(f"✅ Successfully processed {events_count} total events")
        return 0
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())