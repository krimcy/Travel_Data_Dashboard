# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  BRONZE LAYER — Objective 2: Traveler Satisfaction & Booking Friction
#  Project  : Flight Passenger Analysis
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Sources  : [1] Volume → synthetic_flight_passenger_data.csv → bronze_flights
#             [2] SQLite → passengers table                    → bronze_passengers
#             [3] SQLite → routes table                        → bronze_routes
#  Citation : Keatonballard, "Synthetic Airline Passenger and Flight Data,"
#             Kaggle, 2024.
#             https://www.kaggle.com/datasets/keatonballard/synthetic-airline-passenger-and-flight-data
#             Passenger profile and route data generated synthetically from above.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Objective 2: Traveler Satisfaction & Booking Friction
# MAGIC | Layer | Role | Status |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion | ← You are here |
# MAGIC | 🥈 Silver | Cleaned, joined, enriched | next |
# MAGIC | 🥇 Gold   | Aggregated KPIs for dashboard | later |
# MAGIC
# MAGIC ### Sources
# MAGIC | # | Type | What | Table |
# MAGIC |---|------|------|-------|
# MAGIC | 1 | Object Store (Volume) | Flight transactions CSV | `bronze_flights` |
# MAGIC | 2 | Database (SQLite) | Passenger profiles | `bronze_passengers` |
# MAGIC | 3 | Database (SQLite) | Route performance | `bronze_routes` |

# COMMAND ----------
# MAGIC %md ### 0 · Imports & Setup

# COMMAND ----------

import sqlite3
import pandas as pd
from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

print("✅ Spark session ready")
print(f"   Spark version : {spark.version}")

# COMMAND ----------
# MAGIC %md ### 1 · Configuration

# COMMAND ----------

# ── Volume paths ──────────────────────────────────────────────────────────────
VOLUME_ROOT      = "/Volumes/hotel_catalog/hotel_project/raw_data/flight_project"
FLIGHT_CSV_PATH  = f"{VOLUME_ROOT}/synthetic_flight_passenger_data.csv"
DB_LOCAL_PATH    = f"{VOLUME_ROOT}/flight_passengers.db"

# ── Unity Catalog ─────────────────────────────────────────────────────────────
CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "flight_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

print("✅ Configuration ready")
print(f"   Catalog   : {CATALOG_NAME}")
print(f"   Database  : {DATABASE_NAME}")
print(f"   CSV path  : {FLIGHT_CSV_PATH}")
print(f"   DB path   : {DB_LOCAL_PATH}")

# COMMAND ----------
# MAGIC %md ### 2 · Confirm Files in Volume

# COMMAND ----------

import os
files = os.listdir(VOLUME_ROOT)
print(f"Files in {VOLUME_ROOT}:")
for f in files:
    try:
        size_mb = os.path.getsize(f"{VOLUME_ROOT}/{f}") / 1024 / 1024
        print(f"  ✅ {f:<45} {size_mb:.1f} MB")
    except:
        print(f"  📁 {f}")

# COMMAND ----------
# MAGIC %md ### 3 · Column Type Definitions

# COMMAND ----------

# ── Flight CSV columns ────────────────────────────────────────────────────────
INT_COLS_F = [
    "Flight_Duration_Minutes", "Distance_Miles", "Age",
    "Bags_Checked", "No_Show", "Weather_Impact"
]
FLOAT_COLS_F = ["Price_USD", "Flight_Satisfaction_Score", "Delay_Minutes",
                "Booking_Days_In_Advance"]
STR_COLS_F   = [
    "Passenger_ID", "Flight_ID", "Airline", "Departure_Airport",
    "Arrival_Airport", "Departure_Time", "Flight_Status", "Gender",
    "Income_Level", "Travel_Purpose", "Seat_Class",
    "Frequent_Flyer_Status", "Check_in_Method", "Seat_Selected"
]

# ── Passengers DB columns ─────────────────────────────────────────────────────
INT_COLS_P   = ["age", "total_flights", "total_no_shows"]
FLOAT_COLS_P = ["no_show_rate", "avg_satisfaction",
                "avg_price_paid", "total_delay_minutes"]
STR_COLS_P   = [
    "Passenger_ID", "gender", "income_level", "frequent_flyer_status",
    "satisfaction_segment", "preferred_class", "preferred_checkin",
    "primary_travel_purpose", "preferred_airline"
]

# ── Routes DB columns ─────────────────────────────────────────────────────────
INT_COLS_R   = ["total_flights"]
FLOAT_COLS_R = [
    "avg_satisfaction", "avg_delay_minutes", "no_show_rate",
    "pct_cancelled", "pct_delayed", "pct_on_time",
    "avg_price_usd", "avg_duration_minutes",
    "avg_distance_miles", "weather_impact_rate"
]
STR_COLS_R   = [
    "route_id", "Departure_Airport", "Arrival_Airport",
    "Airline", "performance_tier"
]

def cast_types(df_pd, int_cols, float_cols, str_cols):
    """Cast pandas DataFrame columns to correct types."""
    for col in int_cols:
        if col in df_pd.columns:
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce").astype("Int64")
    for col in float_cols:
        if col in df_pd.columns:
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce")
    for col in str_cols:
        if col in df_pd.columns:
            df_pd[col] = df_pd[col].astype(str).replace("nan", None)
    return df_pd

print("✅ Column types defined")

# COMMAND ----------
# MAGIC %md ### 4 · Ingest Source 1 — Volume CSV → `bronze_flights`
# MAGIC
# MAGIC Raw flight transaction records from the object store.

# COMMAND ----------

log.info("Ingesting flight CSV from Volume …")

flights_pd = pd.read_csv(
    FLIGHT_CSV_PATH,
    keep_default_na=True,
    na_values=["NULL", ""]
)
flights_pd = cast_types(flights_pd, INT_COLS_F, FLOAT_COLS_F, STR_COLS_F)
print(f"   CSV read: {len(flights_pd):,} rows, {len(flights_pd.columns)} cols")

df_bronze_flights = (
    spark.createDataFrame(flights_pd)
    .withColumns({
        "_ingested_at" : F.current_timestamp(),
        "_source_file" : F.lit(FLIGHT_CSV_PATH),
    })
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.bronze_flights")
df_bronze_flights.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_flights")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_flights").count()
print(f"✅ bronze_flights → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 5 · Ingest Source 2 — SQLite DB → `bronze_passengers`
# MAGIC
# MAGIC Passenger profiles generated from flight data.

# COMMAND ----------

log.info("Ingesting passengers from SQLite …")

conn          = sqlite3.connect(DB_LOCAL_PATH)
passengers_pd = pd.read_sql("SELECT * FROM passengers", conn)
conn.close()
passengers_pd = cast_types(passengers_pd, INT_COLS_P, FLOAT_COLS_P, STR_COLS_P)
print(f"   SQLite read: {len(passengers_pd):,} rows")

df_bronze_passengers = (
    spark.createDataFrame(passengers_pd)
    .withColumns({
        "_ingested_at" : F.current_timestamp(),
        "_source_file" : F.lit(f"sqlite::{DB_LOCAL_PATH}::passengers"),
    })
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers")
df_bronze_passengers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers").count()
print(f"✅ bronze_passengers → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 6 · Ingest Source 3 — SQLite DB → `bronze_routes`
# MAGIC
# MAGIC Route performance KPIs generated from flight data.

# COMMAND ----------

log.info("Ingesting routes from SQLite …")

conn      = sqlite3.connect(DB_LOCAL_PATH)
routes_pd = pd.read_sql("SELECT * FROM routes", conn)
conn.close()
routes_pd = cast_types(routes_pd, INT_COLS_R, FLOAT_COLS_R, STR_COLS_R)
print(f"   SQLite read: {len(routes_pd):,} rows")

df_bronze_routes = (
    spark.createDataFrame(routes_pd)
    .withColumns({
        "_ingested_at" : F.current_timestamp(),
        "_source_file" : F.lit(f"sqlite::{DB_LOCAL_PATH}::routes"),
    })
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.bronze_routes")
df_bronze_routes.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_routes")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_routes").count()
print(f"✅ bronze_routes → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 7 · Validation

# COMMAND ----------

print("=" * 55)
print("BRONZE LAYER — VALIDATION")
print("=" * 55)

tables = ["bronze_flights", "bronze_passengers", "bronze_routes"]

print(f"\n{'Table':<25} {'Rows':>10}  {'Cols':>6}")
print("-" * 45)
for t in tables:
    df  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{t}")
    print(f"{t:<25} {df.count():>10,}  {len(df.columns):>6}")

print("\n── Null check on join keys ──")
df_f = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_flights")
df_p = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers")
print(f"  flights.Passenger_ID  nulls : {df_f.filter(F.col('Passenger_ID').isNull()).count()}")
print(f"  flights.Flight_ID     nulls : {df_f.filter(F.col('Flight_ID').isNull()).count()}")
print(f"  passengers.Passenger_ID nulls: {df_p.filter(F.col('Passenger_ID').isNull()).count()}")

print("\n── Sample: bronze_flights (3 rows) ──")
spark.sql(f"""
    SELECT Passenger_ID, Flight_ID, Airline,
           Departure_Airport, Arrival_Airport,
           Flight_Status, Seat_Class,
           Flight_Satisfaction_Score, Delay_Minutes,
           No_Show, _ingested_at
    FROM {CATALOG_NAME}.{DATABASE_NAME}.bronze_flights
    LIMIT 3
""").show(truncate=False)

print("\n── Sample: bronze_passengers (3 rows) ──")
spark.sql(f"""
    SELECT Passenger_ID, age, gender, frequent_flyer_status,
           satisfaction_segment, avg_satisfaction,
           preferred_class, no_show_rate, _ingested_at
    FROM {CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers
    LIMIT 3
""").show(truncate=False)

print("\n── Sample: bronze_routes (5 rows) ──")
spark.sql(f"""
    SELECT route_id, avg_satisfaction, avg_delay_minutes,
           pct_on_time, no_show_rate, performance_tier
    FROM {CATALOG_NAME}.{DATABASE_NAME}.bronze_routes
    ORDER BY avg_satisfaction DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Bronze Complete → Run 07_flight_silver next
