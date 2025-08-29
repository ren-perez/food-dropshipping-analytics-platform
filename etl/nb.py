

import duckdb
from pathlib import Path

# Get the path to the current script
BASE_DIR = Path(__file__).resolve().parent.parent
print(BASE_DIR)
# Construct path to the DuckDB file relative to this script
DB_PATH = BASE_DIR / "data" / "warehouse.duckdb"
print(DB_PATH)

# Connect to DuckDB
con = duckdb.connect(str(DB_PATH))

# print(con.sql("SELECT schema_name FROM information_schema.schemata;"))

# print(con.sql("SELECT count(*) FROM raw.posthog_events;"))
# print(con.sql("SELECT * FROM raw.posthog_events limit 10;").columns)
print(con.sql("""select distinct event
from main_staging.raw_posthog_events
order by 1;
"""))


# con.sql("delete FROM raw.posthog_events;")
# con.sql("DROP TABLE IF EXISTS raw.posthog_events;")

con.close()
