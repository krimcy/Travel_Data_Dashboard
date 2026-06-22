# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  SILVER LAYER — Cleaning, Joining & Enrichment
#  Project  : Hotel Premium Package Revenue (Objective 1)
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Input    : bronze_bookings, bronze_guests, bronze_packages (managed tables)
#  Output   : silver_bookings (managed Delta table)
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Clean, Join & Enrich
# MAGIC | Layer | Role | Status |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion | ✅ Done |
# MAGIC | 🥈 Silver | Cleaned, joined, enriched | ← You are here |
# MAGIC | 🥇 Gold   | Aggregated KPIs | next |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "hotel_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

print("✅ Setup complete")

# COMMAND ----------
# MAGIC %md ### 1 · Load Bronze Tables

# COMMAND ----------

df_bookings = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings")
df_guests   = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests")
df_packages = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_packages")

print(f"✅ bronze_bookings  : {df_bookings.count():,} rows")
print(f"✅ bronze_guests    : {df_guests.count():,} rows")
print(f"✅ bronze_packages  : {df_packages.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 2 · Clean Bookings

# COMMAND ----------

log.info("Cleaning bookings …")

month_map = F.create_map(
    F.lit("January"),   F.lit(1),  F.lit("February"),  F.lit(2),
    F.lit("March"),     F.lit(3),  F.lit("April"),     F.lit(4),
    F.lit("May"),       F.lit(5),  F.lit("June"),      F.lit(6),
    F.lit("July"),      F.lit(7),  F.lit("August"),    F.lit(8),
    F.lit("September"), F.lit(9),  F.lit("October"),   F.lit(10),
    F.lit("November"),  F.lit(11), F.lit("December"),  F.lit(12),
)

df_clean = (
    df_bookings
    .fillna({"children": 0.0, "country": "Unknown",
             "agent": 0.0, "company": 0.0})
    .withColumn("meal",
        F.when(F.col("meal") == "Undefined", "SC").otherwise(F.col("meal")))
    .withColumn("adr",
        F.when(F.col("adr") < 0,    F.lit(0.0))
         .when(F.col("adr") > 1000, F.lit(1000.0))
         .otherwise(F.col("adr")))
    .withColumn("children", F.col("children").cast(IntegerType()))
    .withColumn("arrival_month_num", month_map[F.col("arrival_date_month")])
    .withColumn("arrival_date",
        F.to_date(
            F.concat_ws("-",
                F.col("arrival_date_year"),
                F.col("arrival_month_num"),
                F.col("arrival_date_day_of_month")
            ), "yyyy-M-d"))
    .withColumn("reservation_status_date",
        F.to_date(F.col("reservation_status_date"), "yyyy-MM-dd"))
    .withColumn("total_nights",
        F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights"))
    .withColumn("is_day_use",
        F.when(F.col("total_nights") == 0, True).otherwise(False))
    .drop("_ingested_at", "_source_file", "arrival_month_num")
)

print(f"✅ Bookings cleaned: {df_clean.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 3 · Prepare Lookups

# COMMAND ----------

df_guests_lookup = df_guests.select(
    "email",
    F.col("guest_id"),
    F.col("loyalty_tier")          .alias("g_loyalty_tier"),
    F.col("lifetime_spend")        .alias("g_lifetime_spend"),
    F.col("total_stays")           .alias("g_total_stays"),
    F.col("total_cancellations")   .alias("g_total_cancellations"),
    F.col("preferred_meal")        .alias("g_preferred_meal"),
    F.col("preferred_room")        .alias("g_preferred_room"),
    F.col("total_special_requests").alias("g_total_special_requests"),
    F.col("joined_date")           .alias("g_joined_date"),
)

df_packages_lookup = df_packages.select(
    "meal_type", "room_type", "hotel_type",
    F.col("package_id")        .alias("p_package_id"),
    F.col("package_name")      .alias("p_package_name"),
    F.col("base_price")        .alias("p_base_price"),
    F.col("includes_spa")      .alias("p_includes_spa"),
    F.col("includes_transfer") .alias("p_includes_transfer"),
    F.col("is_premium")        .alias("p_is_premium"),
)

print(f"✅ Guest lookup  : {df_guests_lookup.count():,} rows")
print(f"✅ Package lookup: {df_packages_lookup.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 4 · Joins

# COMMAND ----------

log.info("Joining bookings × guests × packages …")

df_joined = (
    df_clean
    .join(df_guests_lookup, on="email", how="left")
    .join(
        df_packages_lookup,
        on=[
            df_clean["meal"]               == df_packages_lookup["meal_type"],
            df_clean["reserved_room_type"] == df_packages_lookup["room_type"],
            df_clean["hotel"]              == df_packages_lookup["hotel_type"],
        ],
        how="left"
    )
    .drop("meal_type", "room_type", "hotel_type")
)

print(f"✅ Joined: {df_joined.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 5 · Derived Columns

# COMMAND ----------

PREMIUM_ROOMS = ["F", "G", "H"]
PREMIUM_MEALS = ["HB", "FB"]

df_silver = (
    df_joined
    .withColumn("total_revenue",
        F.round(F.col("adr") * F.col("total_nights"), 2))
    .withColumn("is_premium_meal",
        F.col("meal").isin(PREMIUM_MEALS))
    .withColumn("is_premium_room",
        F.col("reserved_room_type").isin(PREMIUM_ROOMS))
    .withColumn("is_premium_booking",
        F.col("is_premium_meal") | F.col("is_premium_room"))
    .withColumn("is_catalogue_premium",
        F.when(F.col("p_is_premium") == 1, True)
         .when(F.col("p_is_premium") == 0, False)
         .otherwise(None))
    .withColumn("price_gap_per_night",
        F.round(F.col("p_base_price") - F.col("adr"), 2))
    .withColumn("was_upgraded",
        F.col("reserved_room_type") != F.col("assigned_room_type"))
    .withColumn("is_upsell_opportunity",
        (~F.col("is_premium_booking")) &
        (F.col("g_loyalty_tier").isin(["Gold", "Platinum"])))
    .withColumn("lead_time_bucket",
        F.when(F.col("lead_time") <   7, "< 1 week")
         .when(F.col("lead_time") <  30, "1-4 weeks")
         .when(F.col("lead_time") <  90, "1-3 months")
         .when(F.col("lead_time") < 180, "3-6 months")
         .otherwise("6+ months"))
    .withColumn("guest_value_segment",
        F.when(F.col("g_loyalty_tier") == "Platinum", "High Value")
         .when(F.col("g_loyalty_tier") == "Gold",     "Mid-High Value")
         .when(F.col("g_loyalty_tier") == "Silver",   "Mid Value")
         .otherwise("Standard"))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

print(f"✅ Derived columns added")
print(f"   Total rows            : {df_silver.count():,}")
print(f"   Premium meal bookings : {df_silver.filter(F.col('is_premium_meal')).count():,}")
print(f"   Premium room bookings : {df_silver.filter(F.col('is_premium_room')).count():,}")
print(f"   Upsell opportunities  : {df_silver.filter(F.col('is_upsell_opportunity')).count():,}")

# COMMAND ----------
# MAGIC %md ### 6 · Write Silver Table

# COMMAND ----------

log.info("Writing silver_bookings …")

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.silver_bookings")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_bookings")

cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_bookings").count()
print(f"✅ silver_bookings written → {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### 7 · Validation

# COMMAND ----------

df_s = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_bookings")
df_c = df_s.filter(F.col("is_canceled") == 0)

print("── Revenue by meal package (non-cancelled) ──")
df_c.groupBy("meal").agg(
    F.count("*")                       .alias("bookings"),
    F.round(F.avg("adr"),         2)   .alias("avg_adr"),
    F.round(F.sum("total_revenue"),2)  .alias("total_revenue"),
).orderBy("total_revenue", ascending=False).show()

print("── Revenue by loyalty tier ──")
df_c.groupBy("g_loyalty_tier").agg(
    F.count("*")                       .alias("bookings"),
    F.round(F.avg("adr"),         2)   .alias("avg_adr"),
    F.round(F.sum("total_revenue"),2)  .alias("total_revenue"),
    F.sum(F.col("is_premium_booking").cast("int")).alias("premium"),
).orderBy("total_revenue", ascending=False).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Silver Complete → Run 03_gold_layer next
