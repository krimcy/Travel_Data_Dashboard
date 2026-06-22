# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  SILVER LAYER — Objective 2: Traveler Satisfaction & Booking Friction
#  Project  : Flight Passenger Analysis
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Input    : bronze_flights, bronze_passengers, bronze_routes
#  Output   : silver_flights (clean, joined, enriched)
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Traveler Satisfaction & Booking Friction
# MAGIC | Layer | Role | Status |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion | ✅ Done |
# MAGIC | 🥈 Silver | Cleaned, joined, enriched | ← You are here |
# MAGIC | 🥇 Gold   | Aggregated KPIs for dashboard | next |
# MAGIC
# MAGIC ### What this layer does
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | Clean | Fix nulls, parse dates, cap outliers |
# MAGIC | Join 1 | flights × passengers on Passenger_ID |
# MAGIC | Join 2 | flights × routes on Departure + Arrival + Airline |
# MAGIC | Derive | Satisfaction segments, friction flags, delay buckets |

# COMMAND ----------
# MAGIC %md ### 0 · Imports & Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "flight_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

print("✅ Setup complete")

# COMMAND ----------
# MAGIC %md ### 1 · Load Bronze Tables

# COMMAND ----------

df_flights    = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_flights")
df_passengers = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers")
df_routes     = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_routes")

print(f"✅ bronze_flights    : {df_flights.count():,} rows")
print(f"✅ bronze_passengers : {df_passengers.count():,} rows")
print(f"✅ bronze_routes     : {df_routes.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 2 · Clean Flights
# MAGIC
# MAGIC Fix nulls, parse departure datetime, cap delay outliers,
# MAGIC fill missing Frequent_Flyer_Status.

# COMMAND ----------

log.info("Cleaning flights …")

df_clean = (
    df_flights

    # ── Fix nulls ─────────────────────────────────────────────────────────────
    .fillna({
        "Frequent_Flyer_Status" : "Non-Member",
        "Delay_Minutes"         : 0.0,
        "Bags_Checked"          : 0,
    })

    # ── Cap delay outliers (anything > 300 min is extreme) ───────────────────
    .withColumn("Delay_Minutes",
        F.when(F.col("Delay_Minutes") < 0,   F.lit(0.0))
         .when(F.col("Delay_Minutes") > 300, F.lit(300.0))
         .otherwise(F.col("Delay_Minutes")))

    # ── Parse Departure_Time to proper timestamp ──────────────────────────────
    .withColumn("departure_ts",
        F.to_timestamp(F.col("Departure_Time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("departure_date",
        F.to_date(F.col("departure_ts")))
    .withColumn("departure_hour",
        F.hour(F.col("departure_ts")))
    .withColumn("departure_month",
        F.month(F.col("departure_ts")))
    .withColumn("departure_year",
        F.year(F.col("departure_ts")))

    # ── Drop raw audit columns from Bronze ────────────────────────────────────
    .drop("_ingested_at", "_source_file", "Departure_Time")
)

print(f"✅ Flights cleaned: {df_clean.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 3 · Prepare Lookups

# COMMAND ----------

# ── Passenger lookup — prefix with p_ to avoid collisions ────────────────────
df_passengers_l = df_passengers.select(
    F.col("Passenger_ID"),
    F.col("frequent_flyer_status")   .alias("p_frequent_flyer_status"),
    F.col("satisfaction_segment")    .alias("p_satisfaction_segment"),
    F.col("avg_satisfaction")        .alias("p_avg_satisfaction"),
    F.col("total_flights")           .alias("p_total_flights"),
    F.col("no_show_rate")            .alias("p_no_show_rate"),
    F.col("preferred_class")         .alias("p_preferred_class"),
    F.col("preferred_checkin")       .alias("p_preferred_checkin"),
    F.col("primary_travel_purpose")  .alias("p_primary_travel_purpose"),
    F.col("income_level")            .alias("p_income_level"),
    F.col("total_delay_minutes")     .alias("p_total_delay_minutes"),
)

# ── Route lookup — prefix with r_ ─────────────────────────────────────────────
df_routes_l = df_routes.select(
    F.col("Departure_Airport"),
    F.col("Arrival_Airport"),
    F.col("Airline"),
    F.col("route_id")                .alias("r_route_id"),
    F.col("avg_satisfaction")        .alias("r_avg_satisfaction"),
    F.col("avg_delay_minutes")       .alias("r_avg_delay_minutes"),
    F.col("no_show_rate")            .alias("r_no_show_rate"),
    F.col("pct_on_time")             .alias("r_pct_on_time"),
    F.col("pct_cancelled")           .alias("r_pct_cancelled"),
    F.col("weather_impact_rate")     .alias("r_weather_impact_rate"),
    F.col("performance_tier")        .alias("r_performance_tier"),
    F.col("avg_price_usd")           .alias("r_avg_price_usd"),
)

print(f"✅ Passenger lookup : {df_passengers_l.count():,} rows")
print(f"✅ Route lookup     : {df_routes_l.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 4 · Join 1 — Flights × Passengers (on Passenger_ID)

# COMMAND ----------

log.info("Joining flights × passengers …")

df_with_passengers = df_clean.join(
    df_passengers_l,
    on  = "Passenger_ID",
    how = "left"
)

matched   = df_with_passengers.filter(F.col("p_frequent_flyer_status").isNotNull()).count()
total     = df_with_passengers.count()
print(f"✅ Flights × Passengers join complete")
print(f"   Total rows     : {total:,}")
print(f"   Matched        : {matched:,} ({matched/total*100:.1f}%)")

# COMMAND ----------
# MAGIC %md ### 5 · Join 2 — Flights × Routes (on Departure + Arrival + Airline)

# COMMAND ----------

log.info("Joining flights × routes …")

df_joined = df_with_passengers.join(
    df_routes_l,
    on  = ["Departure_Airport", "Arrival_Airport", "Airline"],
    how = "left"
)

matched = df_joined.filter(F.col("r_route_id").isNotNull()).count()
total   = df_joined.count()
print(f"✅ Flights × Routes join complete")
print(f"   Total rows     : {total:,}")
print(f"   Matched routes : {matched:,} ({matched/total*100:.1f}%)")

# COMMAND ----------
# MAGIC %md ### 6 · Add Derived Columns
# MAGIC
# MAGIC All columns needed directly by the Gold KPI tables.

# COMMAND ----------

log.info("Adding derived columns …")

df_silver = (
    df_joined

    # ── Satisfaction flags ─────────────────────────────────────────────────────
    .withColumn("is_satisfied",
        F.col("Flight_Satisfaction_Score") >= 7.0)

    .withColumn("is_highly_satisfied",
        F.col("Flight_Satisfaction_Score") >= 8.0)

    .withColumn("is_dissatisfied",
        F.col("Flight_Satisfaction_Score") < 5.0)

    .withColumn("satisfaction_band",
        F.when(F.col("Flight_Satisfaction_Score") >= 8.0, "Highly Satisfied")
         .when(F.col("Flight_Satisfaction_Score") >= 7.0, "Satisfied")
         .when(F.col("Flight_Satisfaction_Score") >= 5.0, "Neutral")
         .otherwise("Dissatisfied"))

    # ── Delay flags ───────────────────────────────────────────────────────────
    .withColumn("is_delayed",
        F.col("Delay_Minutes") > 0)

    .withColumn("delay_bucket",
        F.when(F.col("Delay_Minutes") == 0,   "No Delay")
         .when(F.col("Delay_Minutes") <= 15,  "< 15 min")
         .when(F.col("Delay_Minutes") <= 30,  "15-30 min")
         .when(F.col("Delay_Minutes") <= 60,  "30-60 min")
         .otherwise("> 60 min"))

    # ── Booking friction flags ────────────────────────────────────────────────
    .withColumn("booking_bucket",
        F.when(F.col("Booking_Days_In_Advance") <= 7,   "< 1 week")
         .when(F.col("Booking_Days_In_Advance") <= 30,  "1-4 weeks")
         .when(F.col("Booking_Days_In_Advance") <= 60,  "1-2 months")
         .when(F.col("Booking_Days_In_Advance") <= 90,  "2-3 months")
         .otherwise("3+ months"))

    .withColumn("is_last_minute",
        F.col("Booking_Days_In_Advance") <= 7)

    .withColumn("is_no_show",
        F.col("No_Show") == 1)

    # ── Premium flags ─────────────────────────────────────────────────────────
    .withColumn("is_premium_class",
        F.col("Seat_Class").isin(["Business", "First"]))

    .withColumn("is_loyal_passenger",
        F.col("p_frequent_flyer_status").isin(["Gold", "Platinum"]))

    # ── Time of day bucket ────────────────────────────────────────────────────
    .withColumn("time_of_day",
        F.when(F.col("departure_hour") < 6,  "Night (0-6)")
         .when(F.col("departure_hour") < 12, "Morning (6-12)")
         .when(F.col("departure_hour") < 18, "Afternoon (12-18)")
         .otherwise("Evening (18-24)"))

    # ── Price vs route average (value signal) ─────────────────────────────────
    .withColumn("price_vs_route_avg",
        F.round(F.col("Price_USD") - F.col("r_avg_price_usd"), 2))

    # ── Silver audit ──────────────────────────────────────────────────────────
    .withColumn("_silver_processed_at", F.current_timestamp())
)

print(f"✅ Derived columns added")
print(f"   Total rows           : {df_silver.count():,}")
print(f"   Satisfied passengers : {df_silver.filter(F.col('is_satisfied')).count():,}")
print(f"   Dissatisfied         : {df_silver.filter(F.col('is_dissatisfied')).count():,}")
print(f"   No-shows             : {df_silver.filter(F.col('is_no_show')).count():,}")
print(f"   Delayed flights      : {df_silver.filter(F.col('is_delayed')).count():,}")
print(f"   Premium class        : {df_silver.filter(F.col('is_premium_class')).count():,}")
print(f"   Last-minute bookings : {df_silver.filter(F.col('is_last_minute')).count():,}")

# COMMAND ----------
# MAGIC %md ### 7 · Write Silver Table

# COMMAND ----------

log.info("Writing silver_flights …")

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.silver_flights")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_flights")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_flights").count()
print(f"✅ silver_flights written → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 8 · Validation

# COMMAND ----------

df_s = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_flights")

print("=" * 60)
print("SILVER LAYER — VALIDATION")
print("=" * 60)

print(f"\nTotal columns : {len(df_s.columns)}")

print("\n── Satisfaction breakdown ──")
df_s.groupBy("satisfaction_band").count() \
    .orderBy("count", ascending=False).show()

print("── No-show by booking bucket ──")
df_s.groupBy("booking_bucket").agg(
    F.count("*").alias("passengers"),
    F.round(F.avg("No_Show"), 3).alias("no_show_rate"),
    F.round(F.avg("Flight_Satisfaction_Score"), 2).alias("avg_satisfaction")
).orderBy("no_show_rate", ascending=False).show()

print("── Satisfaction by seat class ──")
df_s.groupBy("Seat_Class").agg(
    F.count("*").alias("passengers"),
    F.round(F.avg("Flight_Satisfaction_Score"), 2).alias("avg_satisfaction"),
    F.round(F.avg("No_Show"), 3).alias("no_show_rate")
).orderBy("avg_satisfaction", ascending=False).show()

print("── Sample: silver_flights (3 rows) ──")
spark.sql(f"""
    SELECT Passenger_ID, Flight_ID, Airline,
           Seat_Class, Flight_Satisfaction_Score,
           satisfaction_band, Delay_Minutes, delay_bucket,
           No_Show, booking_bucket, is_last_minute,
           r_performance_tier, p_frequent_flyer_status
    FROM {CATALOG_NAME}.{DATABASE_NAME}.silver_flights
    LIMIT 3
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Silver Complete → Run 08_flight_gold next
