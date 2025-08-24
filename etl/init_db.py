# etl/init_db.py
import duckdb, os


os.makedirs('data', exist_ok=True)
con = duckdb.connect('data/warehouse.duckdb')
con.execute("create schema if not exists raw; create schema if not exists analytics;")
con.close()

