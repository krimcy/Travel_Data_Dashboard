# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  BRONZE LAYER — Raw Data Ingestion
#  Project  : Hotel Premium Package Revenue (Objective 1)
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Sources  : [1] Volume → hotel_booking.csv  → bronze_bookings
#             [2] SQLite → guests table        → bronze_guests
#             [3] SQLite → packages table      → bronze_packages
#  Citation : Mojtaba, "Hotel Booking Dataset," Kaggle 2020.
#             https://www.kaggle.com/datasets/mojtaba142/hotel-booking/data
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC | Layer | Role | Status |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion | ← You are here |
# MAGIC | 🥈 Silver | Cleaned, joined, enriched | next |
# MAGIC | 🥇 Gold   | Aggregated KPIs for dashboard | later |

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

# ── Volume path ───────────────────────────────────────────────────────────────
VOLUME_ROOT      = "/Volumes/hotel_catalog/hotel_project/raw_data"
BOOKING_CSV_PATH = f"{VOLUME_ROOT}/hotel_booking.csv"
DB_LOCAL_PATH    = f"{VOLUME_ROOT}/hotel_guests.db"

# ── Unity Catalog ─────────────────────────────────────────────────────────────
CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "hotel_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

print("✅ Configuration ready")
print(f"   Catalog   : {CATALOG_NAME}")
print(f"   Database  : {DATABASE_NAME}")
print(f"   CSV path  : {BOOKING_CSV_PATH}")
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
        print(f"  ✅ {f:<35} {size_mb:.1f} MB")
    except:
        print(f"  📁 {f}")

# COMMAND ----------
# MAGIC %md ### 3 · Ingest Source 1 — Volume CSV → `bronze_bookings`
# MAGIC
# MAGIC Read CSV with pandas (handles Volume paths natively on Serverless),
# MAGIC cast columns to correct types, then convert to Spark DataFrame.

# COMMAND ----------

log.info("Ingesting hotel_booking.csv from Volume …")

# ── Read with pandas ──────────────────────────────────────────────────────────
bookings_pd = pd.read_csv(
    BOOKING_CSV_PATH,
    keep_default_na=True,
    na_values=["NULL", ""]
)

print(f"   Pandas read: {len(bookings_pd):,} rows, {len(bookings_pd.columns)} cols")

# ── Cast each column to the correct type ──────────────────────────────────────
# Integer columns
int_cols = [
    "is_canceled", "lead_time", "arrival_date_year",
    "arrival_date_week_number", "arrival_date_day_of_month",
    "stays_in_weekend_nights", "stays_in_week_nights",
    "adults", "babies", "is_repeated_guest",
    "previous_cancellations", "previous_bookings_not_canceled",
    "booking_changes", "days_in_waiting_list",
    "required_car_parking_spaces", "total_of_special_requests"
]

# Float/nullable columns
float_cols = ["children", "agent", "company", "adr"]

# String columns
str_cols = [
    "hotel", "arrival_date_month", "meal", "country",
    "market_segment", "distribution_channel",
    "reserved_room_type", "assigned_room_type",
    "deposit_type", "customer_type", "reservation_status",
    "reservation_status_date", "name", "email",
    "phone-number", "credit_card"
]

for col in int_cols:
    bookings_pd[col] = pd.to_numeric(bookings_pd[col], errors="coerce").astype("Int64")

for col in float_cols:
    bookings_pd[col] = pd.to_numeric(bookings_pd[col], errors="coerce")

for col in str_cols:
    bookings_pd[col] = bookings_pd[col].astype(str).replace("nan", None)

print("   Column types cast successfully")

# ── Convert to Spark (let Spark infer from pandas dtypes) ────────────────────
df_bronze_bookings = (
    spark.createDataFrame(bookings_pd)
    .withColumns({
        "_ingested_at" : F.current_timestamp(),
        "_source_file" : F.lit(BOOKING_CSV_PATH),
    })
)

# ── Write as managed Unity Catalog Delta table ────────────────────────────────
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings")

df_bronze_bookings.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings").count()
print(f"✅ bronze_bookings → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 4 · Ingest Source 2 — SQLite DB → `bronze_guests`

# COMMAND ----------

log.info("Ingesting guests from SQLite …")

conn      = sqlite3.connect(DB_LOCAL_PATH)
guests_pd = pd.read_sql("SELECT * FROM guests", conn)
conn.close()

print(f"   SQLite read: {len(guests_pd):,} rows")

# Cast types
int_cols_g   = ["total_stays", "total_cancellations", "total_special_requests"]
float_cols_g = ["lifetime_spend", "avg_adr"]
str_cols_g   = ["guest_id", "name", "email", "phone", "country",
                "loyalty_tier", "preferred_meal", "preferred_room", "joined_date"]

for col in int_cols_g:
    guests_pd[col] = pd.to_numeric(guests_pd[col], errors="coerce").astype("Int64")
for col in float_cols_g:
    guests_pd[col] = pd.to_numeric(guests_pd[col], errors="coerce")
for col in str_cols_g:
    guests_pd[col] = guests_pd[col].astype(str).replace("nan", None)

df_bronze_guests = (
    spark.createDataFrame(guests_pd)
    .withColumns({
        "_ingested_at" : F.current_timestamp(),
        "_source_file" : F.lit(f"sqlite::{DB_LOCAL_PATH}::guests"),
    })
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.bronze_guests")

df_bronze_guests.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests").count()
print(f"✅ bronze_guests → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 5 · Ingest Source 3 — SQLite DB → `bronze_packages`

# COMMAND ----------

log.info("Ingesting packages from SQLite …")

conn         = sqlite3.connect(DB_LOCAL_PATH)
packages_pd  = pd.read_sql("SELECT * FROM packages", conn)
conn.close()

print(f"   SQLite read: {len(packages_pd):,} rows")

# Cast types
int_cols_p   = ["includes_spa", "includes_transfer", "is_premium"]
float_cols_p = ["base_price"]
str_cols_p   = ["package_id", "package_name", "meal_type",
                "room_type", "hotel_type"]

for col in int_cols_p:
    packages_pd[col] = pd.to_numeric(packages_pd[col], errors="coerce").astype("Int64")
for col in float_cols_p:
    packages_pd[col] = pd.to_numeric(packages_pd[col], errors="coerce")
for col in str_cols_p:
    packages_pd[col] = packages_pd[col].astype(str).replace("nan", None)

df_bronze_packages = (
    spark.createDataFrame(packages_pd)
    .withColumns({
        "_ingested_at" : F.current_timestamp(),
        "_source_file" : F.lit(f"sqlite::{DB_LOCAL_PATH}::packages"),
    })
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.bronze_packages")

df_bronze_packages.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_packages")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_packages").count()
print(f"✅ bronze_packages → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 6 · Validation

# COMMAND ----------

print("=" * 55)
print("BRONZE LAYER — VALIDATION")
print("=" * 55)

tables = ["bronze_bookings", "bronze_guests", "bronze_packages"]

print(f"\n{'Table':<25} {'Rows':>10}  {'Cols':>6}")
print("-" * 45)
for t in tables:
    df  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{t}")
    print(f"{t:<25} {df.count():>10,}  {len(df.columns):>6}")

print("\n── Null check on join keys ──")
df_b = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings")
df_g = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests")
print(f"  bookings.email  nulls : {df_b.filter(F.col('email').isNull()).count()}")
print(f"  bookings.adr    nulls : {df_b.filter(F.col('adr').isNull()).count()}")
print(f"  guests.email    nulls : {df_g.filter(F.col('email').isNull()).count()}")

print("\n── Sample: bronze_bookings (3 rows) ──")
spark.sql(f"""
    SELECT hotel, meal, reserved_room_type, adr,
           is_canceled, email, _ingested_at
    FROM {CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings
    LIMIT 3
""").show(truncate=False)

print("\n── Sample: bronze_guests (3 rows) ──")
spark.sql(f"""
    SELECT guest_id, name, country, loyalty_tier,
           total_stays, lifetime_spend, _ingested_at
    FROM {CATALOG_NAME}.{DATABASE_NAME}.bronze_guests
    LIMIT 3
""").show(truncate=False)

print("\n── bronze_packages (all rows) ──")
spark.sql(f"""
    SELECT package_id, package_name, meal_type,
           room_type, base_price, is_premium, hotel_type
    FROM {CATALOG_NAME}.{DATABASE_NAME}.bronze_packages
    ORDER BY hotel_type, base_price
""").show(20, truncate=False)

# COMMAND ----------
# MAGIC %md ### 7 · Auto-Update (MERGE pattern)
# MAGIC
# MAGIC Run the functions below when new data has been added and re-uploaded.

# COMMAND ----------

from delta.tables import DeltaTable

def update_bronze_bookings():
    """Re-reads CSV and MERGEs new/changed rows into bronze_bookings."""
    print("Updating bronze_bookings …")

    df_new_pd = pd.read_csv(BOOKING_CSV_PATH, keep_default_na=True, na_values=["NULL",""])

    for col in int_cols:
        df_new_pd[col] = pd.to_numeric(df_new_pd[col], errors="coerce").astype("Int64")
    for col in float_cols:
        df_new_pd[col] = pd.to_numeric(df_new_pd[col], errors="coerce")
    for col in str_cols:
        df_new_pd[col] = df_new_pd[col].astype(str).replace("nan", None)

    df_new = (
        spark.createDataFrame(df_new_pd)
        .withColumns({
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.lit(BOOKING_CSV_PATH),
        })
    )

    delta_table = DeltaTable.forName(spark, f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings")
    (
        delta_table.alias("existing")
        .merge(df_new.alias("incoming"),
            """existing.email                     = incoming.email AND
               existing.arrival_date_year         = incoming.arrival_date_year AND
               existing.arrival_date_month        = incoming.arrival_date_month AND
               existing.arrival_date_day_of_month = incoming.arrival_date_day_of_month""")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings").count()
    print(f"✅ bronze_bookings updated → {cnt:,} rows")


def update_bronze_guests():
    """Re-reads SQLite and MERGEs new/changed guests into bronze_guests."""
    print("Updating bronze_guests …")

    conn      = sqlite3.connect(DB_LOCAL_PATH)
    g_pd      = pd.read_sql("SELECT * FROM guests", conn)
    conn.close()

    for col in int_cols_g:
        g_pd[col] = pd.to_numeric(g_pd[col], errors="coerce").astype("Int64")
    for col in float_cols_g:
        g_pd[col] = pd.to_numeric(g_pd[col], errors="coerce")
    for col in str_cols_g:
        g_pd[col] = g_pd[col].astype(str).replace("nan", None)

    df_new = (
        spark.createDataFrame(g_pd)
        .withColumns({
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.lit(f"sqlite::{DB_LOCAL_PATH}::guests"),
        })
    )

    delta_table = DeltaTable.forName(spark, f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests")
    (
        delta_table.alias("existing")
        .merge(df_new.alias("incoming"), "existing.guest_id = incoming.guest_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests").count()
    print(f"✅ bronze_guests updated → {cnt:,} rows")


# ── Uncomment when new data arrives ───────────────────────────────────────────
# update_bronze_bookings()
# update_bronze_guests()
print("✅ Auto-update functions ready")
print("   Uncomment the last 2 lines and re-run this cell when new data arrives.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Bronze Complete → Run 02_silver_layer next
