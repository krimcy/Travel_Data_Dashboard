# Hotel Premium Package Revenue Pipeline
### Medallion Architecture on Databricks | Data Engineering Project

## Project Overview

This project implements a **Medallion Architecture** data pipeline on **Databricks Free Tier (Serverless + Unity Catalog)** to answer two business objectives using hotel booking data.

| Objective | Question |
|-----------|----------|
| **Objective 1** | How can we increase premium package revenue? |
| **Objective 2** | How can we improve traveler satisfaction and reduce booking friction? |

A partner student implements the same objectives on **Snowflake** for comparison.

---

## Architecture

```
Object Store (Unity Catalog Volume)        Database (SQLite)
hotel_booking.csv                          hotel_guests.db
        │                                   │         │
        ▼                                   ▼         ▼
┌─────────────────────────────────────────────────────────┐
│                     🥉 BRONZE LAYER                      │
│   bronze_bookings | bronze_guests | bronze_packages     │
│              Raw ingestion — no transforms              │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                     🥈 SILVER LAYER                      │
│                    silver_bookings                      │
│         Cleaned + joined + enriched + derived           │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                     🥇 GOLD LAYER                        │
│  gold_revenue_by_package  | gold_premium_conversion     │
│  gold_revenue_by_loyalty  | gold_revenue_by_segment     │
│  gold_upsell_opportunity  | gold_revenue_by_room        │
│              Aggregated KPIs for dashboard              │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    📊 DASHBOARD                          │
│         6 interactive Plotly charts rendered            │
│              inline in Databricks notebook              │
└─────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Type | Description |
|--------|------|-------------|
| `hotel_booking.csv` | **Object Store** (Unity Catalog Volume) | 119,390 hotel booking transactions |
| `hotel_guests.db` → `guests` | **Database** (SQLite) | 115,889 guest profiles with loyalty tiers |
| `hotel_guests.db` → `packages` | **Database** (SQLite) | 17 premium package catalogue entries |

**Citation:**
> Mojtaba, "Hotel Booking Dataset," Kaggle, 2020.
> https://www.kaggle.com/datasets/mojtaba142/hotel-booking/data
>
> Guest loyalty and package catalogue data generated synthetically,
> derived from guest identifiers in the above dataset.

---

## Repository Structure

```
hotel-revenue-pipeline/
│
├── README.md
│
├── notebooks/
│   ├── 01_bronze_layer.py      # Raw ingestion from both sources
│   ├── 02_silver_layer.py      # Cleaning, joining, enrichment
│   ├── 03_gold_layer.py        # KPI aggregations (6 tables)
│   ├── 04_dashboard.py         # Interactive Plotly dashboard
│   └── 05_auto_update.py       # One-click pipeline refresh
│
├── scripts/
│   └── generate_db.py          # Generates SQLite DB from Kaggle data
│
└── data/
    ├── packages.csv             # Premium package catalogue (17 rows)
    └── guests.csv               # Guest profiles sample
```

---

## Setup Instructions

### Prerequisites
- Databricks Free Tier account
- Unity Catalog enabled
- Kaggle dataset downloaded: [hotel_booking.csv](https://www.kaggle.com/datasets/mojtaba142/hotel-booking/data)

### 1. Generate the Database
Run locally:
```bash
pip install pandas faker
python scripts/generate_db.py
```
This produces `hotel_guests.db` with `guests` and `packages` tables.

### 2. Create Unity Catalog Structure
In a Databricks notebook:
```python
spark.sql("CREATE CATALOG IF NOT EXISTS hotel_catalog")
spark.sql("CREATE DATABASE IF NOT EXISTS hotel_catalog.hotel_project")
spark.sql("CREATE VOLUME IF NOT EXISTS hotel_catalog.hotel_project.raw_data")
```

### 3. Upload Files to Volume
Upload to `/Volumes/hotel_catalog/hotel_project/raw_data/`:
- `hotel_booking.csv`
- `hotel_guests.db`

### 4. Run Notebooks in Order
```
01_bronze_layer.py   →   02_silver_layer.py   →   03_gold_layer.py   →   04_dashboard.py
```

### 5. Auto-Update
When new data is added to any source, run:
```
05_auto_update.py
```
This detects new rows, updates all layers, and refreshes the dashboard automatically.

---

## Dashboard — Objective 1: How can we increase premium package revenue?

| Chart | Business Question |
|-------|------------------|
| KPI Cards | Total revenue, premium revenue, conversion rate, upsell targets |
| Monthly Revenue by Meal Package | Which packages (BB/HB/FB) drive the most revenue? |
| Premium Conversion Rate Over Time | Is the premium booking rate growing month-on-month? |
| Revenue by Loyalty Tier | Which guest tiers spend most on premium? |
| ADR by Market Segment | Which booking channels have the highest daily rate? |
| Revenue & ADR by Room Type | Are premium rooms (F/G/H) being fully utilised? |
| Upsell Opportunity | Which loyal guests are still booking standard packages? |

### Key Findings

| Finding | Insight |
|---------|---------|
| FB guests generate **2× revenue** per booking vs BB | Push Full Board upgrades at checkout |
| Premium conversion **peaks in summer** (38%) and crashes Oct–Jan | Launch off-peak premium promotions |
| Platinum guests have **52% premium rate** vs 12.5% Bronze | Segment upsell campaigns by loyalty tier |
| Room H: **€181 ADR** but only 356 bookings | Increase Room H visibility in booking flow |
| Direct + Online TA: **€114 ADR** (highest of all segments) | Concentrate premium promotions on these channels |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Platform | Databricks Free Tier (Serverless) |
| Storage | Unity Catalog Volumes (Object Store) |
| Database | SQLite (hosted in Volume) |
| Processing | Apache Spark 4.1.0 (PySpark) |
| Table Format | Delta Lake |
| Visualisation | Plotly |
| Language | Python 3 |

---

## Team

| Student | Platform | Role |
|---------|----------|------|
| Ishani | **Databricks** (Medallion Architecture) | This repository |
| [Partner] | **Snowflake** | Separate repository |

---

*SRH Campus Hamburg — Data Engineering Project*
