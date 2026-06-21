# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  GOLD LAYER — KPI Aggregations
#  Project  : Hotel Premium Package Revenue (Objective 1)
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Input    : silver_bookings
#  Output   : 6 Gold managed Delta tables
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥇 Gold Layer — KPI Aggregations
# MAGIC | Layer | Role | Status |
# MAGIC |-------|------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion | ✅ Done |
# MAGIC | 🥈 Silver | Cleaned, joined, enriched | ✅ Done |
# MAGIC | 🥇 Gold   | Aggregated KPIs | ← You are here |

# COMMAND ----------

from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark.conf.set("spark.sql.shuffle.partitions", "8")

CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "hotel_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

print("✅ Setup complete")

# COMMAND ----------
# MAGIC %md ### 1 · Load Silver

# COMMAND ----------

df_silver    = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_bookings")
df_completed = df_silver.filter(F.col("is_canceled") == 0)

print(f"✅ silver_bookings loaded")
print(f"   Total rows       : {df_silver.count():,}")
print(f"   Completed stays  : {df_completed.count():,}")

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
    print(f"  ✅ {table_name:<35} {cnt:>8,} rows")

# COMMAND ----------
# MAGIC %md ### 3 · Build All Gold Tables

# COMMAND ----------

print("Building Gold tables …\n")

# ── Gold 1: Revenue by package (monthly) ─────────────────────────────────────
g1 = (
    df_completed
    .groupBy("arrival_date_year", "arrival_date_month",
             F.month("arrival_date").alias("arrival_month_num"),
             "meal", "is_premium_meal", "hotel")
    .agg(
        F.count("*")                              .alias("bookings"),
        F.round(F.avg("adr"),              2)     .alias("avg_adr"),
        F.round(F.avg("total_nights"),     2)     .alias("avg_nights"),
        F.round(F.sum("total_revenue"),    2)     .alias("total_revenue"),
        F.round(F.avg("p_base_price"),     2)     .alias("avg_catalogue_price"),
        F.round(F.avg("price_gap_per_night"), 2)  .alias("avg_price_gap"),
    )
    .orderBy("arrival_date_year", "arrival_month_num", "meal")
)
write_gold(g1, "gold_revenue_by_package")

# ── Gold 2: Premium conversion (monthly) ─────────────────────────────────────
g2 = (
    df_completed
    .groupBy("arrival_date_year", "arrival_date_month",
             F.month("arrival_date").alias("arrival_month_num"), "hotel")
    .agg(
        F.count("*")                                              .alias("total_bookings"),
        F.sum(F.col("is_premium_booking").cast("int"))            .alias("premium_bookings"),
        F.sum(F.col("is_premium_meal").cast("int"))               .alias("premium_meal_bookings"),
        F.sum(F.col("is_premium_room").cast("int"))               .alias("premium_room_bookings"),
        F.round(F.sum("total_revenue"),    2)                     .alias("total_revenue"),
        F.round(F.sum(
            F.when(F.col("is_premium_booking"),
                   F.col("total_revenue")).otherwise(0)), 2)      .alias("premium_revenue"),
        F.round(F.avg("adr"), 2)                                  .alias("avg_adr"),
    )
    .withColumn("premium_conversion_pct",
        F.round(F.col("premium_bookings") / F.col("total_bookings") * 100, 2))
    .withColumn("premium_revenue_pct",
        F.round(F.col("premium_revenue") / F.col("total_revenue") * 100, 2))
    .orderBy("arrival_date_year", "arrival_month_num")
)
write_gold(g2, "gold_premium_conversion")

# ── Gold 3: Revenue by loyalty tier ──────────────────────────────────────────
g3 = (
    df_completed
    .groupBy("g_loyalty_tier", "hotel")
    .agg(
        F.count("*")                                               .alias("total_bookings"),
        F.sum(F.col("is_premium_booking").cast("int"))             .alias("premium_bookings"),
        F.round(F.avg("adr"),            2)                        .alias("avg_adr"),
        F.round(F.avg("total_nights"),   2)                        .alias("avg_nights"),
        F.round(F.sum("total_revenue"),  2)                        .alias("total_revenue"),
        F.round(F.avg("g_lifetime_spend"), 2)                      .alias("avg_lifetime_spend"),
        F.sum(F.col("is_upsell_opportunity").cast("int"))          .alias("upsell_opportunities"),
    )
    .withColumn("premium_rate_pct",
        F.round(F.col("premium_bookings") / F.col("total_bookings") * 100, 2))
    .withColumn("revenue_per_booking",
        F.round(F.col("total_revenue") / F.col("total_bookings"), 2))
)
write_gold(g3, "gold_revenue_by_loyalty")

# ── Gold 4: Revenue by market segment ────────────────────────────────────────
g4 = (
    df_completed
    .filter(F.col("market_segment") != "Undefined")
    .groupBy("market_segment", "hotel")
    .agg(
        F.count("*")                                           .alias("total_bookings"),
        F.sum(F.col("is_premium_booking").cast("int"))         .alias("premium_bookings"),
        F.round(F.avg("adr"),            2)                    .alias("avg_adr"),
        F.round(F.avg("total_nights"),   2)                    .alias("avg_nights"),
        F.round(F.sum("total_revenue"),  2)                    .alias("total_revenue"),
        F.round(F.avg("total_of_special_requests"), 2)         .alias("avg_special_requests"),
    )
    .withColumn("premium_rate_pct",
        F.round(F.col("premium_bookings") / F.col("total_bookings") * 100, 2))
    .withColumn("revenue_per_booking",
        F.round(F.col("total_revenue") / F.col("total_bookings"), 2))
    .orderBy("avg_adr", ascending=False)
)
write_gold(g4, "gold_revenue_by_segment")

# ── Gold 5: Upsell opportunity ────────────────────────────────────────────────
g5 = (
    df_completed
    .filter(~F.col("is_premium_booking"))
    .groupBy("g_loyalty_tier", "hotel", "meal", "guest_value_segment")
    .agg(
        F.count("*")                                 .alias("standard_bookings"),
        F.round(F.avg("adr"),            2)          .alias("avg_current_adr"),
        F.round(F.avg("p_base_price"),   2)          .alias("avg_catalogue_price"),
        F.round(F.sum("total_revenue"),  2)          .alias("current_revenue"),
        F.round(F.avg("price_gap_per_night"), 2)     .alias("avg_price_gap"),
        F.round(F.avg("total_nights"),   2)          .alias("avg_nights"),
    )
    .withColumn("estimated_uplift_30pct",
        F.round(
            F.col("standard_bookings") * 0.30 *
            F.col("avg_price_gap") *
            F.col("avg_nights"), 2))
)
write_gold(g5, "gold_upsell_opportunity")

# ── Gold 6: Revenue by room type ─────────────────────────────────────────────
g6 = (
    df_completed
    .groupBy("reserved_room_type", "is_premium_room", "hotel")
    .agg(
        F.count("*")                                       .alias("total_bookings"),
        F.round(F.avg("adr"),            2)                .alias("avg_adr"),
        F.round(F.avg("total_nights"),   2)                .alias("avg_nights"),
        F.round(F.sum("total_revenue"),  2)                .alias("total_revenue"),
        F.sum(F.col("was_upgraded").cast("int"))           .alias("upgrades_received"),
        F.round(F.avg("p_base_price"),   2)                .alias("avg_catalogue_price"),
    )
    .withColumn("upgrade_rate_pct",
        F.round(F.col("upgrades_received") / F.col("total_bookings") * 100, 2))
    .withColumn("revenue_per_booking",
        F.round(F.col("total_revenue") / F.col("total_bookings"), 2))
    .orderBy("avg_adr", ascending=False)
)
write_gold(g6, "gold_revenue_by_room")

# COMMAND ----------
# MAGIC %md ### 4 · Validation Summary

# COMMAND ----------

print("=" * 55)
print("GOLD LAYER — VALIDATION SUMMARY")
print("=" * 55)

gold_tables = [
    "gold_revenue_by_package",
    "gold_premium_conversion",
    "gold_revenue_by_loyalty",
    "gold_revenue_by_segment",
    "gold_upsell_opportunity",
    "gold_revenue_by_room",
]

print(f"\n{'Table':<35} {'Rows':>8}  {'Cols':>6}")
print("-" * 53)
for t in gold_tables:
    df_g = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{t}")
    print(f"{t:<35} {df_g.count():>8,}  {len(df_g.columns):>6}")

print("\n── Headline KPIs ──")
df_completed.select(
    F.count("*")                                        .alias("total_bookings"),
    F.round(F.sum("total_revenue"), 2)                  .alias("total_revenue"),
    F.round(F.sum(
        F.when(F.col("is_premium_booking"),
               F.col("total_revenue")).otherwise(0)), 2).alias("premium_revenue"),
    F.sum(F.col("is_premium_booking").cast("int"))       .alias("premium_bookings"),
).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Gold Complete → Run 04_dashboard next
