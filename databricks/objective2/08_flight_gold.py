# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  GOLD LAYER — Objective 2: Traveler Satisfaction & Booking Friction
#  Project  : Flight Passenger Analysis
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Input    : silver_flights
#  Output   : 6 Gold managed Delta tables
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥇 Gold Layer — Traveler Satisfaction & Booking Friction
# MAGIC | Layer | Role | Status |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion | ✅ Done |
# MAGIC | 🥈 Silver | Cleaned, joined, enriched | ✅ Done |
# MAGIC | 🥇 Gold   | Aggregated KPIs for dashboard | ← You are here |

# COMMAND ----------
# MAGIC %md ### 0 · Setup

# COMMAND ----------

from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark.conf.set("spark.sql.shuffle.partitions", "8")

CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "flight_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

print("✅ Setup complete")

# COMMAND ----------
# MAGIC %md ### 1 · Load Silver

# COMMAND ----------

df_silver = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_flights")
total = df_silver.count()

print(f"✅ silver_flights loaded: {total:,} rows")
print(f"   Satisfied    : {df_silver.filter(F.col('is_satisfied')).count():,}")
print(f"   Dissatisfied : {df_silver.filter(F.col('is_dissatisfied')).count():,}")
print(f"   No-shows     : {df_silver.filter(F.col('is_no_show')).count():,}")
print(f"   Delayed      : {df_silver.filter(F.col('is_delayed')).count():,}")

# COMMAND ----------
# MAGIC %md ### 2 · Helper — Write Gold Table

# COMMAND ----------

def write_gold(df, table_name):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.{table_name}")
    (
        df.withColumn("_gold_updated_at", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}")
    )
    cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}").count()
    print(f"  ✅ {table_name:<42} {cnt:>8,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 1 — Satisfaction by Passenger Segment

# COMMAND ----------

log.info("Building gold_satisfaction_by_segment …")

g1 = (
    df_silver
    .groupBy(
        "p_frequent_flyer_status",
        "Seat_Class",
        "p_income_level",
        "Travel_Purpose",
        "Gender",
    )
    .agg(
        F.count("*")                                          .alias("passengers"),
        F.round(F.avg("Flight_Satisfaction_Score"), 2)        .alias("avg_satisfaction"),
        F.round(F.avg("No_Show"), 3)                          .alias("no_show_rate"),
        F.round(F.avg("Delay_Minutes"), 2)                    .alias("avg_delay"),
        F.sum(F.col("is_satisfied").cast("int"))              .alias("satisfied_count"),
        F.sum(F.col("is_highly_satisfied").cast("int"))       .alias("highly_satisfied_count"),
        F.sum(F.col("is_dissatisfied").cast("int"))           .alias("dissatisfied_count"),
        F.round(F.avg("Price_USD"), 2)                        .alias("avg_price_paid"),
    )
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count") / F.col("passengers") * 100, 2))
    .withColumn("dissatisfaction_rate_pct",
        F.round(F.col("dissatisfied_count") / F.col("passengers") * 100, 2))
    .orderBy("avg_satisfaction", ascending=False)
)

write_gold(g1, "gold_satisfaction_by_segment")

print("\nPreview (top 5 by satisfaction):")
spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_satisfaction_by_segment") \
    .select("p_frequent_flyer_status","Seat_Class","Travel_Purpose",
            "passengers","avg_satisfaction","no_show_rate","satisfaction_rate_pct") \
    .show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 2 — Satisfaction by Route

# COMMAND ----------

log.info("Building gold_satisfaction_by_route …")

g2 = (
    df_silver
    .groupBy(
        "r_route_id",
        "Departure_Airport",
        "Arrival_Airport",
        "Airline",
        "r_performance_tier",
    )
    .agg(
        F.count("*")                                          .alias("total_flights"),
        F.round(F.avg("Flight_Satisfaction_Score"), 2)        .alias("avg_satisfaction"),
        F.round(F.avg("Delay_Minutes"), 2)                    .alias("avg_delay_minutes"),
        F.round(F.avg("No_Show"), 3)                          .alias("no_show_rate"),
        F.round(F.avg("Weather_Impact"), 3)                   .alias("weather_impact_rate"),
        F.sum(F.col("is_satisfied").cast("int"))              .alias("satisfied_count"),
        F.sum(F.col("is_dissatisfied").cast("int"))           .alias("dissatisfied_count"),
        F.round(F.avg("Price_USD"), 2)                        .alias("avg_price"),
        # Fixed: use F.when instead of .equalTo()
        F.sum(F.when(F.col("Flight_Status") == "Cancelled", 1)
               .otherwise(0))                                 .alias("cancelled_flights"),
    )
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count") / F.col("total_flights") * 100, 2))
    .withColumn("cancellation_rate_pct",
        F.round(F.col("cancelled_flights") / F.col("total_flights") * 100, 2))
    .filter(F.col("total_flights") >= 5)
    .orderBy("avg_satisfaction", ascending=False)
)

write_gold(g2, "gold_satisfaction_by_route")

print("\nPreview (top 5 routes):")
spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_satisfaction_by_route") \
    .select("r_route_id","Airline","avg_satisfaction",
            "avg_delay_minutes","no_show_rate","r_performance_tier") \
    .show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 3 — No-Show by Booking Pattern

# COMMAND ----------

log.info("Building gold_noshow_by_booking_pattern …")

g3 = (
    df_silver
    .groupBy(
        "booking_bucket",
        "Travel_Purpose",
        "p_frequent_flyer_status",
    )
    .agg(
        F.count("*")                                          .alias("passengers"),
        F.sum(F.col("is_no_show").cast("int"))                .alias("no_shows"),
        F.round(F.avg("No_Show"), 3)                          .alias("no_show_rate"),
        F.round(F.avg("Flight_Satisfaction_Score"), 2)        .alias("avg_satisfaction"),
        F.round(F.avg("Booking_Days_In_Advance"), 1)          .alias("avg_booking_days"),
        F.round(F.avg("Price_USD"), 2)                        .alias("avg_price"),
        F.sum(F.col("is_last_minute").cast("int"))            .alias("last_minute_bookings"),
    )
    .withColumn("no_show_pct",
        F.round(F.col("no_shows") / F.col("passengers") * 100, 2))
    .orderBy("no_show_rate", ascending=False)
)

write_gold(g3, "gold_noshow_by_booking_pattern")

print("\nPreview (highest no-show rates):")
spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_noshow_by_booking_pattern") \
    .select("booking_bucket","Travel_Purpose","p_frequent_flyer_status",
            "passengers","no_shows","no_show_rate","avg_satisfaction") \
    .show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 4 — Friction by Check-in Method

# COMMAND ----------

log.info("Building gold_friction_by_checkin …")

g4 = (
    df_silver
    .groupBy(
        "Check_in_Method",
        "Seat_Class",
        "p_frequent_flyer_status",
    )
    .agg(
        F.count("*")                                          .alias("passengers"),
        F.round(F.avg("No_Show"), 3)                          .alias("no_show_rate"),
        F.round(F.avg("Flight_Satisfaction_Score"), 2)        .alias("avg_satisfaction"),
        F.round(F.avg("Delay_Minutes"), 2)                    .alias("avg_delay"),
        F.round(F.avg("Booking_Days_In_Advance"), 1)          .alias("avg_booking_days"),
        F.sum(F.col("is_no_show").cast("int"))                .alias("no_shows"),
        F.sum(F.col("is_satisfied").cast("int"))              .alias("satisfied_count"),
        F.sum(F.col("is_dissatisfied").cast("int"))           .alias("dissatisfied_count"),
    )
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count") / F.col("passengers") * 100, 2))
    .withColumn("no_show_pct",
        F.round(F.col("no_shows") / F.col("passengers") * 100, 2))
    .orderBy("no_show_rate", ascending=False)
)

write_gold(g4, "gold_friction_by_checkin")

print("\nPreview:")
spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_friction_by_checkin") \
    .groupBy("Check_in_Method") \
    .agg(
        F.sum("passengers").alias("total"),
        F.round(F.avg("no_show_rate"), 3).alias("avg_no_show_rate"),
        F.round(F.avg("avg_satisfaction"), 2).alias("avg_satisfaction"),
    ) \
    .orderBy("avg_no_show_rate", ascending=False).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 5 — Delay Impact on Satisfaction

# COMMAND ----------

log.info("Building gold_delay_impact …")

g5 = (
    df_silver
    .groupBy(
        "delay_bucket",
        "Flight_Status",
        "Weather_Impact",
        "Airline",
    )
    .agg(
        F.count("*")                                          .alias("flights"),
        F.round(F.avg("Flight_Satisfaction_Score"), 2)        .alias("avg_satisfaction"),
        F.round(F.avg("Delay_Minutes"), 2)                    .alias("avg_delay_minutes"),
        F.round(F.avg("No_Show"), 3)                          .alias("no_show_rate"),
        F.sum(F.col("is_satisfied").cast("int"))              .alias("satisfied_count"),
        F.sum(F.col("is_dissatisfied").cast("int"))           .alias("dissatisfied_count"),
        F.round(F.avg("Price_USD"), 2)                        .alias("avg_price"),
    )
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count") / F.col("flights") * 100, 2))
    .withColumn("dissatisfaction_rate_pct",
        F.round(F.col("dissatisfied_count") / F.col("flights") * 100, 2))
    .withColumn("delay_order",
        F.when(F.col("delay_bucket") == "No Delay",  1)
         .when(F.col("delay_bucket") == "< 15 min",  2)
         .when(F.col("delay_bucket") == "15-30 min", 3)
         .when(F.col("delay_bucket") == "30-60 min", 4)
         .otherwise(5))
    .orderBy("delay_order", "Airline")
    .drop("delay_order")
)

write_gold(g5, "gold_delay_impact")

print("\nPreview (delay bucket summary):")
spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_delay_impact") \
    .groupBy("delay_bucket") \
    .agg(
        F.sum("flights").alias("total_flights"),
        F.round(F.avg("avg_satisfaction"), 2).alias("avg_satisfaction"),
        F.round(F.avg("no_show_rate"), 3).alias("avg_no_show_rate"),
    ) \
    .orderBy("avg_satisfaction", ascending=False).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 6 — Traveler Friction Profile

# COMMAND ----------

log.info("Building gold_traveler_profile …")

g6 = (
    df_silver
    .groupBy(
        "p_frequent_flyer_status",
        "Seat_Class",
        "Check_in_Method",
        "Travel_Purpose",
        "booking_bucket",
        "time_of_day",
    )
    .agg(
        F.count("*")                                          .alias("passengers"),
        F.round(F.avg("Flight_Satisfaction_Score"), 2)        .alias("avg_satisfaction"),
        F.round(F.avg("No_Show"), 3)                          .alias("no_show_rate"),
        F.round(F.avg("Delay_Minutes"), 2)                    .alias("avg_delay"),
        F.round(F.avg("Price_USD"), 2)                        .alias("avg_price"),
        F.round(F.avg("Booking_Days_In_Advance"), 1)          .alias("avg_booking_days"),
        F.sum(F.col("is_satisfied").cast("int"))              .alias("satisfied_count"),
        F.sum(F.col("is_no_show").cast("int"))                .alias("no_show_count"),
        F.sum(F.col("is_last_minute").cast("int"))            .alias("last_minute_count"),
        F.sum(F.col("is_premium_class").cast("int"))          .alias("premium_class_count"),
    )
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count") / F.col("passengers") * 100, 2))
    .withColumn("no_show_pct",
        F.round(F.col("no_show_count") / F.col("passengers") * 100, 2))
    .withColumn("friction_score",
        F.round(
            (F.lit(1) - F.col("avg_satisfaction") / F.lit(10)) * 50 +
            F.col("no_show_rate") * 50, 2))
    .orderBy("friction_score", ascending=False)
)

write_gold(g6, "gold_traveler_profile")

print("\nPreview (highest friction profiles):")
spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_traveler_profile") \
    .select("p_frequent_flyer_status","Seat_Class","Check_in_Method",
            "Travel_Purpose","booking_bucket","passengers",
            "avg_satisfaction","no_show_rate","friction_score") \
    .show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ### 3 · Validation Summary

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER — VALIDATION SUMMARY")
print("=" * 60)

gold_tables = [
    "gold_satisfaction_by_segment",
    "gold_satisfaction_by_route",
    "gold_noshow_by_booking_pattern",
    "gold_friction_by_checkin",
    "gold_delay_impact",
    "gold_traveler_profile",
]

print(f"\n{'Table':<42} {'Rows':>8}  {'Cols':>6}")
print("-" * 58)
for t in gold_tables:
    df_g = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{t}")
    print(f"{t:<42} {df_g.count():>8,}  {len(df_g.columns):>6}")

print("\n── Headline KPIs ──")
df_silver.select(
    F.count("*")                                         .alias("total_passengers"),
    F.round(F.avg("Flight_Satisfaction_Score"), 2)       .alias("overall_avg_satisfaction"),
    F.round(F.avg("No_Show"), 3)                         .alias("overall_no_show_rate"),
    F.round(F.avg("Delay_Minutes"), 2)                   .alias("overall_avg_delay"),
    F.sum(F.col("is_satisfied").cast("int"))             .alias("satisfied_count"),
    F.sum(F.col("is_dissatisfied").cast("int"))          .alias("dissatisfied_count"),
    F.sum(F.col("is_no_show").cast("int"))               .alias("total_no_shows"),
).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Gold Complete → Run 09_flight_dashboard next
