import os
import json
import logging
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from enum import Enum
import hashlib
import time

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Configuration
POSTHOG_API_KEY = os.getenv("POSTHOG_API_KEY")
POSTHOG_PROJECT_ID = os.getenv("POSTHOG_PROJECT_ID", "")
DUCKDB_PATH = "data/warehouse.duckdb"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))


class JobStatus(Enum):
    """Enumeration for batch job statuses"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class BatchJobMetadata:
    """Data class for batch job metadata tracking"""
    job_id: str
    job_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: JobStatus = JobStatus.PENDING
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0
    data_hash: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage"""
        data = asdict(self)
        data['start_time'] = self.start_time.isoformat()
        data['end_time'] = self.end_time.isoformat() if self.end_time else None
        data['status'] = self.status.value
        return data


class BatchLogger:
    """Enhanced logging system for batch processing"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Setup logging configuration
        log_file = self.log_dir / f"batch_processing_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def log_job_start(self, job_id: str, job_name: str):
        """Log batch job start"""
        self.logger.info(f"Starting batch job: {job_name} (ID: {job_id})")
    
    def log_job_end(self, metadata: BatchJobMetadata):
        """Log batch job completion"""
        duration = (metadata.end_time - metadata.start_time).total_seconds()
        self.logger.info(
            f"Completed batch job: {metadata.job_name} "
            f"(Status: {metadata.status.value}, "
            f"Duration: {duration:.2f}s, "
            f"Records: {metadata.records_processed})"
        )
    
    def log_error(self, job_id: str, error: Exception):
        """Log batch job errors"""
        self.logger.error(f"Job {job_id} failed: {str(error)}", exc_info=True)


class DataQualityChecker:
    """Data quality validation and cleaning"""
    
    @staticmethod
    def validate_events(events: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Validate and clean events data"""
        valid_events = []
        invalid_events = []
        
        for event in events:
            if DataQualityChecker._is_valid_event(event):
                # Clean and normalize the event
                cleaned_event = DataQualityChecker._clean_event(event)
                valid_events.append(cleaned_event)
            else:
                invalid_events.append(event)
        
        return valid_events, invalid_events
    
    @staticmethod
    def _is_valid_event(event: Dict) -> bool:
        """Check if event meets basic quality requirements"""
        required_fields = ['timestamp', 'event']
        return all(field in event and event[field] is not None for field in required_fields)
    
    @staticmethod
    def _clean_event(event: Dict) -> Dict:
        """Clean and normalize event data"""
        cleaned = event.copy()
        
        # Ensure timestamp is in ISO format
        if 'timestamp' in cleaned:
            try:
                if isinstance(cleaned['timestamp'], str):
                    cleaned['timestamp'] = pd.to_datetime(cleaned['timestamp']).isoformat()
            except:
                cleaned['timestamp'] = datetime.now(UTC).isoformat()
        
        # Remove null values from properties if it exists
        if 'properties' in cleaned and cleaned['properties']:
            cleaned['properties'] = {k: v for k, v in cleaned['properties'].items() if v is not None}
        
        return cleaned
    
    @staticmethod
    def calculate_data_hash(events: List[Dict]) -> str:
        """Calculate hash for data integrity checking"""
        data_str = json.dumps(events, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()


class PostHogConnector:
    """Enhanced connector for PostHog API with retry logic and error handling"""
    
    def __init__(self, api_key: str, project_id: str):
        self.api_key = api_key
        self.project_id = project_id
        self.base_url = "https://us.posthog.com/api/projects"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def fetch_events_batch(self, since: datetime, until: datetime, 
                          limit: int = BATCH_SIZE, offset: int = 0) -> List[Dict]:
        """Fetch events with pagination and retry logic"""
        url = f"{self.base_url}/{self.project_id}/query/"
        
        # Build HogQL query with date filtering
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
                    LIMIT {limit} OFFSET {offset}
                """
            }
        }
        
        return self._make_request_with_retry(url, query_body)
    
    def _make_request_with_retry(self, url: str, payload: Dict) -> List[Dict]:
        """Make API request with exponential backoff retry"""
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(url, json=payload, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                columns = data.get("columns", [])
                results = data.get("results", [])
                
                # Convert to list of dictionaries
                events = [dict(zip(columns, row)) for row in results]
                return events
                
            except requests.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    raise e
                
                wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                time.sleep(wait_time)
        
        return []


class DuckDBWarehouse:
    """Enhanced DuckDB warehouse operations with schema management"""
    
    def __init__(self, db_path: str = DUCKDB_PATH):
        self.db_path = db_path
        # Ensure data directory exists
        Path(db_path).parent.mkdir(exist_ok=True)
        self._initialize_schemas()
    
    def _initialize_schemas(self):
        """Initialize database schemas and tables"""
        with duckdb.connect(self.db_path) as conn:
            # Create schemas
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS metadata;")
            
            # Create metadata tables
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata.batch_jobs (
                    job_id VARCHAR PRIMARY KEY,
                    job_name VARCHAR NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status VARCHAR NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    records_failed INTEGER DEFAULT 0,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    data_hash VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create data quality log table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata.data_quality_log (
                    id INTEGER PRIMARY KEY,
                    job_id VARCHAR NOT NULL,
                    validation_type VARCHAR NOT NULL,
                    records_valid INTEGER DEFAULT 0,
                    records_invalid INTEGER DEFAULT 0,
                    issues TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
    
    def upsert_events(self, events: List[Dict], table_name: str = "raw.posthog_events"):
        """Insert events with deduplication based on event ID"""
        if not events:
            return 0
        
        df = pd.DataFrame(events)
        
        with duckdb.connect(self.db_path) as conn:
            # Create table if it doesn't exist
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM df LIMIT 0
            """)
            
            # Use INSERT OR IGNORE for deduplication (assumes unique constraint exists)
            try:
                conn.execute(f"INSERT OR IGNORE INTO {table_name} SELECT * FROM df")
                return len(events)
            except Exception as e:
                # Fallback: manual deduplication
                conn.execute(f"""
                    INSERT INTO {table_name} 
                    SELECT * FROM df 
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table_name} t 
                        WHERE t.uuid = df.uuid
                    )
                """)
                return len(events)
    
    def save_job_metadata(self, metadata: BatchJobMetadata):
        """Save batch job metadata"""
        with duckdb.connect(self.db_path) as conn:
            data = metadata.to_dict()
            placeholders = ', '.join(['?' for _ in data.keys()])
            columns = ', '.join(data.keys())
            
            conn.execute(f"""
                INSERT OR REPLACE INTO metadata.batch_jobs ({columns})
                VALUES ({placeholders})
            """, list(data.values()))
    
    def get_last_successful_run(self, job_name: str) -> Optional[datetime]:
        """Get timestamp of last successful batch run"""
        with duckdb.connect(self.db_path) as conn:
            result = conn.execute("""
                SELECT MAX(end_time) as last_run
                FROM metadata.batch_jobs 
                WHERE job_name = ? AND status = 'success'
            """, [job_name]).fetchone()
            
            if result and result[0]:
                return datetime.fromisoformat(result[0])
            return None
    
    def log_data_quality(self, job_id: str, validation_type: str, 
                        valid_count: int, invalid_count: int, issues: str = ""):
        """Log data quality metrics"""
        with duckdb.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO metadata.data_quality_log 
                (job_id, validation_type, records_valid, records_invalid, issues)
                VALUES (?, ?, ?, ?, ?)
            """, [job_id, validation_type, valid_count, invalid_count, issues])


class BatchProcessor:
    """Main batch processing orchestrator"""
    
    def __init__(self):
        self.logger = BatchLogger()
        self.posthog = PostHogConnector(POSTHOG_API_KEY, POSTHOG_PROJECT_ID)
        self.warehouse = DuckDBWarehouse()
        self.quality_checker = DataQualityChecker()
    
    def run_daily_sync(self, target_date: Optional[datetime] = None) -> BatchJobMetadata:
        """Run daily synchronization job"""
        if target_date is None:
            target_date = datetime.now(UTC).date()
        
        job_id = f"daily_sync_{target_date.strftime('%Y%m%d')}_{int(time.time())}"
        job_name = "daily_posthog_sync"
        
        # Initialize job metadata
        metadata = BatchJobMetadata(
            job_id=job_id,
            job_name=job_name,
            start_time=datetime.now(UTC)
        )
        
        self.logger.log_job_start(job_id, job_name)
        
        try:
            metadata.status = JobStatus.RUNNING
            self.warehouse.save_job_metadata(metadata)
            
            # Determine date range for processing
            since = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=UTC)
            until = since + timedelta(days=1)
            
            # Process data in batches
            all_events = []
            offset = 0
            
            while True:
                batch_events = self.posthog.fetch_events_batch(
                    since=since, until=until, limit=BATCH_SIZE, offset=offset
                )
                
                if not batch_events:
                    break
                
                # Data quality validation
                valid_events, invalid_events = self.quality_checker.validate_events(batch_events)
                
                if invalid_events:
                    self.warehouse.log_data_quality(
                        job_id, "validation", len(valid_events), 
                        len(invalid_events), f"Invalid events found: {len(invalid_events)}"
                    )
                
                all_events.extend(valid_events)
                metadata.records_failed += len(invalid_events)
                
                offset += BATCH_SIZE
                
                # Log progress
                self.logger.logger.info(f"Processed batch: {len(valid_events)} valid, {len(invalid_events)} invalid")
            
            # Calculate data hash for integrity
            if all_events:
                metadata.data_hash = self.quality_checker.calculate_data_hash(all_events)
                
                # Load into warehouse
                records_inserted = self.warehouse.upsert_events(all_events)
                metadata.records_processed = records_inserted
            
            # Mark job as successful
            metadata.status = JobStatus.SUCCESS
            metadata.end_time = datetime.now(UTC)
            
        except Exception as e:
            metadata.status = JobStatus.FAILED
            metadata.error_message = str(e)
            metadata.end_time = datetime.now(UTC)
            self.logger.log_error(job_id, e)
            
        finally:
            self.warehouse.save_job_metadata(metadata)
            self.logger.log_job_end(metadata)
        
        return metadata
    
    def run_incremental_sync(self) -> BatchJobMetadata:
        """Run incremental sync since last successful run"""
        job_name = "incremental_posthog_sync"
        
        # Get last successful run
        last_run = self.warehouse.get_last_successful_run(job_name)
        
        if last_run is None:
            # First run - sync last 7 days
            since = datetime.now(UTC) - timedelta(days=7)
        else:
            since = last_run
        
        until = datetime.now(UTC)
        
        job_id = f"incremental_sync_{int(time.time())}"
        metadata = BatchJobMetadata(
            job_id=job_id,
            job_name=job_name,
            start_time=datetime.now(UTC)
        )
        
        self.logger.log_job_start(job_id, job_name)
        
        try:
            metadata.status = JobStatus.RUNNING
            self.warehouse.save_job_metadata(metadata)
            
            # Fetch and process events
            events = self.posthog.fetch_events_batch(since=since, until=until, limit=10000)
            
            if events:
                valid_events, invalid_events = self.quality_checker.validate_events(events)
                
                if valid_events:
                    records_inserted = self.warehouse.upsert_events(valid_events)
                    metadata.records_processed = records_inserted
                    metadata.data_hash = self.quality_checker.calculate_data_hash(valid_events)
                
                metadata.records_failed = len(invalid_events)
            
            metadata.status = JobStatus.SUCCESS
            metadata.end_time = datetime.now(UTC)
            
        except Exception as e:
            metadata.status = JobStatus.FAILED
            metadata.error_message = str(e)
            metadata.end_time = datetime.now(UTC)
            self.logger.log_error(job_id, e)
            
        finally:
            self.warehouse.save_job_metadata(metadata)
            self.logger.log_job_end(metadata)
        
        return metadata
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get status of a specific job"""
        with duckdb.connect(self.warehouse.db_path) as conn:
            result = conn.execute("""
                SELECT * FROM metadata.batch_jobs WHERE job_id = ?
            """, [job_id]).fetchone()
            
            if result:
                columns = [desc[0] for desc in conn.description]
                return dict(zip(columns, result))
            return None
    
    def cleanup_old_jobs(self, days_to_keep: int = 30):
        """Clean up old job metadata"""
        cutoff_date = datetime.now(UTC) - timedelta(days=days_to_keep)
        
        with duckdb.connect(self.warehouse.db_path) as conn:
            deleted = conn.execute("""
                DELETE FROM metadata.batch_jobs 
                WHERE created_at < ?
            """, [cutoff_date.isoformat()]).fetchone()
            
            self.logger.logger.info(f"Cleaned up old job records")


# CLI Interface and Usage Examples
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch Processing for Web Analytics')
    parser.add_argument('--mode', choices=['daily', 'incremental', 'status'], 
                       default='daily', help='Processing mode')
    parser.add_argument('--date', type=str, help='Target date for daily sync (YYYY-MM-DD)')
    parser.add_argument('--job-id', type=str, help='Job ID for status check')
    
    args = parser.parse_args()
    
    processor = BatchProcessor()
    
    if args.mode == 'daily':
        target_date = None
        if args.date:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        
        metadata = processor.run_daily_sync(target_date)
        print(f"Daily sync completed: {metadata.status.value}")
        print(f"Records processed: {metadata.records_processed}")
        
    elif args.mode == 'incremental':
        metadata = processor.run_incremental_sync()
        print(f"Incremental sync completed: {metadata.status.value}")
        print(f"Records processed: {metadata.records_processed}")
        
    elif args.mode == 'status':
        if not args.job_id:
            print("Please provide --job-id for status check")
        else:
            status = processor.get_job_status(args.job_id)
            if status:
                print(f"Job Status: {status}")
            else:
                print(f"Job {args.job_id} not found")