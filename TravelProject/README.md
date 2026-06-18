# 🏨 Hotel Booking Intelligence Platform
## Snowflake · Spark · Dashboard

---

## Business Objectives

| # | Objective | KPI |
|---|-----------|-----|
| BO-1 | **Increase Premium Package Revenue** | % revenue from premium bookings, upsell conversion rate |
| BO-2 | **Improve Traveler Satisfaction & Reduce Booking Friction** | Cancellation rate, avg lead time, repeat booking rate |

---

## Project Structure

```
hotel_revenue_project/
├── data/
│   ├── generate_fake_data.py        # Fake data generator (no LLM data)
│   └── schema.md                    # Dataset schema documentation
├── etl/
│   ├── ingest_to_snowflake.py       # Load CSV → Snowflake raw layer
│   └── spark_transform.py           # PySpark transformations (Databricks-ready)
├── sql/
│   └── snowflake/
│       ├── 01_setup_database.sql    # DB + schema creation
│       ├── 02_create_tables.sql     # Raw + curated tables
│       ├── 03_bo1_premium_revenue.sql   # BO-1 analysis queries
│       └── 04_bo2_satisfaction.sql      # BO-2 analysis queries
├── dashboard/
│   └── dashboard.html               # Self-contained HTML dashboard
├── tests/
│   ├── test_data_quality.py         # Data quality checks
│   └── test_pipeline.py             # Pipeline unit tests
├── docs/
│   └── report.docx                  # Formal project report
├── screenshots/                     # Milestone screenshots (git-tracked)
│   └── .gitkeep
├── requirements.txt
├── submission.txt                   # Submission summary
└── README.md
```

---

## Data Source

**Dataset:** [Hotel Booking Demand](https://www.kaggle.com/datasets/mojtaba142/hotel-booking)
- **Citation:** Antonio, A., de Almeida Rodrigues, A., & Nunes, L. (2019). *Hotel booking demand datasets.* Data in Brief, 22, 41–49.
- ~119,000 rows · 32 columns · City Hotel + Resort Hotel
- Fields: booking dates, lead time, stay duration, ADR, market segment, deposit type, cancellation, country, etc.

**Fake supplementary data** generated via `data/generate_fake_data.py` (no LLM-generated data).

---

## Setup & Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Download dataset
Place `hotel_bookings.csv` into `data/` from Kaggle.

### 3. Generate fake enrichment data
```bash
python data/generate_fake_data.py
```

### 4. Set Snowflake credentials
```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
```

### 5. Setup Snowflake
```bash
snowsql -f sql/snowflake/01_setup_database.sql
snowsql -f sql/snowflake/02_create_tables.sql
```

### 6. Run ETL
```bash
python etl/ingest_to_snowflake.py        # Load raw data
python etl/spark_transform.py            # Run Spark transformations
```

### 7. Run BO queries
```bash
snowsql -f sql/snowflake/03_bo1_premium_revenue.sql
snowsql -f sql/snowflake/04_bo2_satisfaction.sql
```

### 8. Run tests
```bash
pytest tests/ -v
```

### 9. Open dashboard
Open `dashboard/dashboard.html` in a browser.

---

## Testing Strategy

When new data is added to the source:
1. Re-run `etl/ingest_to_snowflake.py` → raw table refreshed
2. Spark transformations re-run → curated layer updated
3. BO queries re-execute → dashboard reflects new metrics
4. JSON export updated automatically
5. All test assertions re-validated via `pytest`
