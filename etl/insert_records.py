import os
import duckdb
import pandas as pd
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

DUCKDB_CONN = duckdb.connect()

def create_schema_and_table(engine):
    """Create the raw schema and posthog_events table if they don't exist."""
    try:
        with engine.connect() as conn:
            # Create schema
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
            
            # Create table with corrected structure based on sample data
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS raw.posthog_events (
                uuid UUID PRIMARY KEY,
                event TEXT NOT NULL,
                properties JSONB,
                timestamp TIMESTAMPTZ NOT NULL,
                distinct_id TEXT,
                elements_chain TEXT,
                created_at TIMESTAMPTZ,
                session_id TEXT,
                window_id TEXT,
                person_id TEXT,
                group_0 TEXT,
                group_1 TEXT,
                group_2 TEXT,
                group_3 TEXT,
                group_4 TEXT,
                elements_chain_href TEXT,
                elements_chain_texts JSONB,
                elements_chain_ids JSONB,
                elements_chain_elements JSONB,
                event_person_id TEXT,
                event_issue_id TEXT,
                issue_id TEXT,
                inserted_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info("Schema and table created successfully")
    except SQLAlchemyError as e:
        logger.error(f"Failed to create schema/table: {e}")
        raise

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all fields are SQL-compatible for Postgres."""
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Convert 'properties' from JSON string to dict if it's a string
    if "properties" in df.columns:
        def parse_properties(x):
            if isinstance(x, str):
                try:
                    return json.loads(x)
                except (json.JSONDecodeError, TypeError):
                    return None
            elif isinstance(x, dict):
                return x
            else:
                return None
        
        df["properties"] = df["properties"].apply(parse_properties)

    # Handle column name mapping (PostHog uses $ prefixes which need to be renamed)
    column_mapping = {
        '$session_id': 'session_id',
        '$window_id': 'window_id',
        '$group_0': 'group_0',
        '$group_1': 'group_1', 
        '$group_2': 'group_2',
        '$group_3': 'group_3',
        '$group_4': 'group_4'
    }
    
    # Rename columns if they exist
    df = df.rename(columns=column_mapping)
    
    # Convert timestamp columns to proper datetime format
    timestamp_columns = ['timestamp', 'created_at']
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    
    # Handle array columns that should be JSONB
    array_columns = ['elements_chain_texts', 'elements_chain_ids', 'elements_chain_elements']
    for col in array_columns:
        if col in df.columns:
            def serialize_array(val):
                if isinstance(val, (list, dict)):
                    return val
                elif isinstance(val, str):
                    try:
                        return json.loads(val)
                    except:
                        return []
                else:
                    return []
            df[col] = df[col].apply(serialize_array)
    
    # Fill NaN values with None for cleaner SQL insertion
    df = df.where(pd.notnull(df), None)
    
    logger.info(f"DataFrame cleaned. Shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    
    return df

def _insert_dataframe(df: pd.DataFrame):
    """Insert dataframe into Postgres using proper JSONB handling."""
    try:
        engine = create_engine(
            "postgresql://db_user:db_password@localhost:5432/db",
            pool_pre_ping=True,
            pool_recycle=300
        )
        
        # Ensure schema and table exist with proper types
        create_schema_and_table(engine)
        
        # Use manual insertion for better JSONB control
        insert_sql = """
        INSERT INTO raw.posthog_events (
            uuid, event, properties, timestamp, distinct_id, elements_chain, created_at,
            session_id, window_id, person_id, group_0, group_1, group_2, group_3, group_4,
            elements_chain_href, elements_chain_texts, elements_chain_ids, elements_chain_elements,
            event_person_id, event_issue_id, issue_id
        ) VALUES (
            :uuid, :event, :properties, :timestamp, :distinct_id, :elements_chain, :created_at,
            :session_id, :window_id, :person_id, :group_0, :group_1, :group_2, :group_3, :group_4,
            :elements_chain_href, :elements_chain_texts, :elements_chain_ids, :elements_chain_elements,
            :event_person_id, :event_issue_id, :issue_id
        )
        ON CONFLICT (uuid) DO NOTHING
        """
        
        with engine.connect() as conn:
            # Convert to records
            records = df.to_dict('records')
            
            # Process in batches
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                result = conn.execute(text(insert_sql), batch)
                batch_count = len(batch)
                total_inserted += batch_count
                logger.info(f"Processed batch {i//batch_size + 1}: {batch_count} rows")
            
            conn.commit()
            logger.info(f"Successfully processed {total_inserted} rows")
            return total_inserted
        
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert data: {e}")
        logger.error(f"DataFrame shape: {df.shape}")
        logger.error(f"DataFrame columns: {list(df.columns)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during insertion: {e}")
        raise

def load_posthog_to_postgres():
    """Load data from R2 (S3-compatible API) into Postgres via DuckDB."""
    r2_access_key = os.getenv("R2_ACCESS_KEY")
    r2_secret_key = os.getenv("R2_SECRET_KEY")
    r2_endpoint = os.getenv("R2_ENDPOINT")
    r2_bucket = os.getenv("R2_BUCKET")

    if not all([r2_access_key, r2_secret_key, r2_endpoint, r2_bucket]):
        logger.error("Missing R2 configuration")
        return

    # Configure DuckDB to use S3 credentials
    try:
        DUCKDB_CONN.execute(f"""
            SET s3_region='auto';
            SET s3_access_key_id='{r2_access_key}';
            SET s3_secret_access_key='{r2_secret_key}';
            SET s3_endpoint='{r2_endpoint}';
            SET s3_url_style='path';
        """)

        # Use a more specific path pattern to avoid issues
        s3_parquet_path = f"s3://{r2_bucket}/posthog/**/*.parquet"

        df = DUCKDB_CONN.execute(f"""
            SELECT * FROM read_parquet('{s3_parquet_path}')
            ORDER BY timestamp
        """).df()
        
        logger.info(f"Loaded {len(df)} records from R2")
        
    except Exception as e:
        logger.error(f"Failed to read from R2: {e}")
        return

    if len(df) == 0:
        logger.warning("No data loaded from R2")
        return

    # Clean and insert data
    df_cleaned = _clean_dataframe(df)
    _insert_dataframe(df_cleaned)

def load_from_mock_data():
    """Load mock parquet data from local disk into Postgres via DuckDB."""
    mock_path = os.path.join(
        os.path.dirname(__file__),
        "../data/raw/posthog/2025/09/02/batch_0001.parquet"
    )

    if not os.path.exists(mock_path):
        logger.error(f"Mock data file not found: {mock_path}")
        return

    try:
        df = DUCKDB_CONN.execute(f"""
            SELECT * FROM read_parquet('{mock_path}')
        """).df()
        
        logger.info(f"Loaded {len(df)} records from mock parquet")
        
    except Exception as e:
        logger.error(f"Failed to read local mock data: {e}")
        return

    if len(df) == 0:
        logger.warning("No data in mock file")
        return

    # Clean and insert data
    df_cleaned = _clean_dataframe(df)
    _insert_dataframe(df_cleaned)

if __name__ == "__main__":
    use_mock = True

    if use_mock:
        load_from_mock_data()
    else:
        load_posthog_to_postgres()