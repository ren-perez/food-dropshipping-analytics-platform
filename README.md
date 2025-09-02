
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
├── docker-compose.yml          # root compose orchestrating all containers
├── shop/                       # Next.js app for fake shop
├── etl/                        # ingestion.py
├── dbt/                        # dbt project
├── data/
│   ├── warehouse.duckdb        # DuckDB warehouse
│   └── raw/posthog/...         # Parquet files
├── metabase/
│   ├── Dockerfile              # custom Debian image
│   └── plugins/
└── README.md

```

### dbt Models structure

```
dbt/models/
├── staging/
│   └── stg_posthog_events.sql
├── core/
│   ├── fact_events.sql
│   ├── fact_orders.sql
│   └── dim_date.sql
└── marts/
    ├── funnel_conversion.sql
    └── daily_sales.sql
```
