
# Food Dropshipping Analytics

An end-to-end data engineering project simulating a Goldbelly-style food dropshipping platform. 
It demonstrates a modern data stack with event ingestion, transformation, and analytics.

## Architecture

[Insert architecture diagram here: Shop → PostHog → ETL → DuckDB → dbt → Metabase]

## Components
- **Shop (Next.js)** → Fake e-commerce storefront with PostHog tracking
- **ETL (Python)** → Scripts to fetch PostHog events, load into DuckDB
- **dbt** → Transformations and analytics models
- **Metabase** → BI dashboards
- **DuckDB** → Local warehouse

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Node.js 20+

### Setup
1. Clone repo
2. Copy `.env.example` to `.env` in each service and update configs
3. Run `docker-compose up` (starts shop + metabase)
4. Run ETL: `python etl/fetch_posthog.py`

## Data Flow
1. User interacts with Shop → PostHog captures events
2. ETL fetches PostHog events → loads DuckDB
3. dbt models transform raw → analytics-ready tables
4. Metabase connects to DuckDB for dashboards

## Project Showcase
- [Dashboards screenshots]
- [Sample queries / models]

## Tech Stack
- Next.js, PostHog, Python, DuckDB, dbt, Metabase, Docker


## Project structure

```
food-dropship-analytics/
├── docker-compose.yml           # Root Compose file orchestrating all services
├── README.md                    # Project overview and usage instructions

├── shop/                        # Frontend: Fake shop built with Next.js
│   └── ...                      # Next.js source code (pages/, components/, etc.)

├── etl/                         # ETL scripts for data ingestion
│   ├── ingestion.py             # Entry point for ETL job
│   ├── fetch_api.py             # Fetches data from PostHog API
│   └── utils.py                 # (Optional) Shared ETL utilities

├── dbt/                         # dbt project for transformations
│   └── ...                      # dbt models, profiles, etc.

├── data/                        # Local data storage
│   ├── warehouse.duckdb         # DuckDB local warehouse
│   └── raw/
│       └── posthog/             # Raw PostHog data in Parquet format

├── metabase/                    # Metabase analytics/BI
│   ├── Dockerfile               # Custom Metabase image (Debian-based)
│   └── plugins/                 # Metabase plugins (e.g., DuckDB driver)

└── .git/                        # Git repository initialized at project root

```

### dbt Models structure

```
dbt/models/
├── staging/
│   └── raw_posthog_events.sql
│   └── stg_posthog_events.sql
├── core/
│   ├── fact_events.sql
│   ├── fact_orders.sql
│   └── dim_date.sql
└── marts/
    ├── funnel_conversion.sql
    └── daily_sales.sql
```
