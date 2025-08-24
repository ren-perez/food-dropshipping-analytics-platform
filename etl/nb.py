

import duckdb

con = duckdb.connect('data/warehouse.duckdb')
# print(con.sql("SELECT schema_name FROM information_schema.schemata;"))

print(con.sql("SELECT count(*) FROM raw.posthog_events;"))

# con.sql("delete FROM raw.posthog_events;")
