# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  AUTO-UPDATE NOTEBOOK
#  Project  : Hotel Premium Package Revenue (Objective 1)
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Purpose  : Run when new data is added. Detects new rows, updates Bronze,
#             rebuilds Silver + Gold, refreshes dashboard.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔄 Auto-Update Pipeline
# MAGIC **Run this notebook whenever new data is added to any source**
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Detect new rows in source files |
# MAGIC | 2 | Overwrite bronze_bookings with full updated CSV |
# MAGIC | 3 | Overwrite bronze_guests with full updated DB |
# MAGIC | 4 | Rebuild silver_bookings |
# MAGIC | 5 | Rebuild all 6 Gold KPI tables |
# MAGIC | 6 | Show updated dashboard |

# COMMAND ----------

%pip install plotly --quiet

# COMMAND ----------

import sqlite3
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

CATALOG_NAME     = "hotel_catalog"
DATABASE_NAME    = "hotel_project"
VOLUME_ROOT      = "/Volumes/hotel_catalog/hotel_project/raw_data"
BOOKING_CSV_PATH = f"{VOLUME_ROOT}/hotel_booking.csv"
DB_LOCAL_PATH    = f"{VOLUME_ROOT}/hotel_guests.db"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

# ── Column type lists ─────────────────────────────────────────────────────────
INT_COLS = [
    "is_canceled", "lead_time", "arrival_date_year",
    "arrival_date_week_number", "arrival_date_day_of_month",
    "stays_in_weekend_nights", "stays_in_week_nights",
    "adults", "babies", "is_repeated_guest",
    "previous_cancellations", "previous_bookings_not_canceled",
    "booking_changes", "days_in_waiting_list",
    "required_car_parking_spaces", "total_of_special_requests"
]
FLOAT_COLS = ["children", "agent", "company", "adr"]
STR_COLS   = [
    "hotel", "arrival_date_month", "meal", "country",
    "market_segment", "distribution_channel",
    "reserved_room_type", "assigned_room_type",
    "deposit_type", "customer_type", "reservation_status",
    "reservation_status_date", "name", "email",
    "phone-number", "credit_card"
]
INT_COLS_G   = ["total_stays", "total_cancellations", "total_special_requests"]
FLOAT_COLS_G = ["lifetime_spend", "avg_adr"]
STR_COLS_G   = ["guest_id", "name", "email", "phone", "country",
                "loyalty_tier", "preferred_meal", "preferred_room", "joined_date"]

def cast_bookings(df_pd):
    for col in INT_COLS:
        df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce").astype("Int64")
    for col in FLOAT_COLS:
        df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce")
    for col in STR_COLS:
        df_pd[col] = df_pd[col].astype(str).replace("nan", None)
    return df_pd

def cast_guests(df_pd):
    for col in INT_COLS_G:
        df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce").astype("Int64")
    for col in FLOAT_COLS_G:
        df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce")
    for col in STR_COLS_G:
        df_pd[col] = df_pd[col].astype(str).replace("nan", None)
    return df_pd

def overwrite_table(df, table_name):
    """Drop and recreate a managed Delta table."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.{table_name}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}")
    cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}").count()
    return cnt

RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"✅ Setup complete — Run timestamp: {RUN_TIMESTAMP}")

# COMMAND ----------
# MAGIC %md ### Step 1 — Detect New Data

# COMMAND ----------

print("=" * 55)
print("STEP 1 — DETECTING NEW DATA")
print("=" * 55)

source_csv_count    = len(pd.read_csv(BOOKING_CSV_PATH, usecols=["hotel"]))
conn                = sqlite3.connect(DB_LOCAL_PATH)
source_guests_count = pd.read_sql("SELECT COUNT(*) AS n FROM guests", conn).iloc[0,0]
conn.close()

bronze_bookings_count = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings").count()
bronze_guests_count   = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests").count()

new_bookings = source_csv_count - bronze_bookings_count
new_guests   = source_guests_count - bronze_guests_count

print(f"\n{'Source':<25} {'In Source':>12} {'In Bronze':>12} {'New Rows':>10}")
print("-" * 63)
print(f"{'hotel_booking.csv':<25} {source_csv_count:>12,} {bronze_bookings_count:>12,} {new_bookings:>10,}")
print(f"{'hotel_guests.db':<25}   {source_guests_count:>12,} {bronze_guests_count:>12,} {new_guests:>10,}")

if new_bookings <= 0 and new_guests <= 0:
    print("\n⚠️  No new rows detected in sources.")
    print("    Pipeline will still rebuild Silver + Gold + Dashboard.")
else:
    print(f"\n✅ New data detected — proceeding with full update")

# COMMAND ----------
# MAGIC %md ### Step 2 — Update Bronze: Bookings (Full Overwrite)

# COMMAND ----------

print("STEP 2 — UPDATING BRONZE BOOKINGS")

bookings_pd = pd.read_csv(BOOKING_CSV_PATH, keep_default_na=True, na_values=["NULL",""])
bookings_pd = cast_bookings(bookings_pd)
print(f"   CSV read   : {len(bookings_pd):,} rows, types cast ✅")

df_new_bookings = (
    spark.createDataFrame(bookings_pd)
    .withColumns({
        "_ingested_at": F.current_timestamp(),
        "_source_file": F.lit(BOOKING_CSV_PATH),
    })
)

cnt = overwrite_table(df_new_bookings, "bronze_bookings")
print(f"✅ bronze_bookings: {bronze_bookings_count:,} → {cnt:,} rows  (+{cnt - bronze_bookings_count})")

# COMMAND ----------
# MAGIC %md ### Step 3 — Update Bronze: Guests (Full Overwrite)

# COMMAND ----------

print("STEP 3 — UPDATING BRONZE GUESTS")

conn      = sqlite3.connect(DB_LOCAL_PATH)
guests_pd = pd.read_sql("SELECT * FROM guests", conn)
conn.close()
guests_pd = cast_guests(guests_pd)
print(f"   SQLite read: {len(guests_pd):,} rows, types cast ✅")

df_new_guests = (
    spark.createDataFrame(guests_pd)
    .withColumns({
        "_ingested_at": F.current_timestamp(),
        "_source_file": F.lit(f"sqlite::{DB_LOCAL_PATH}::guests"),
    })
)

cnt = overwrite_table(df_new_guests, "bronze_guests")
print(f"✅ bronze_guests: {bronze_guests_count:,} → {cnt:,} rows  (+{cnt - bronze_guests_count})")

# COMMAND ----------
# MAGIC %md ### Step 4 — Rebuild Silver

# COMMAND ----------

print("STEP 4 — REBUILDING SILVER")

df_bookings = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_bookings")
df_guests   = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_guests")
df_packages = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_packages")

month_map = F.create_map(
    F.lit("January"),F.lit(1),   F.lit("February"),F.lit(2),
    F.lit("March"),F.lit(3),     F.lit("April"),F.lit(4),
    F.lit("May"),F.lit(5),       F.lit("June"),F.lit(6),
    F.lit("July"),F.lit(7),      F.lit("August"),F.lit(8),
    F.lit("September"),F.lit(9), F.lit("October"),F.lit(10),
    F.lit("November"),F.lit(11), F.lit("December"),F.lit(12),
)

df_clean = (
    df_bookings
    .fillna({"children":0.0,"country":"Unknown","agent":0.0,"company":0.0})
    .withColumn("meal",
        F.when(F.col("meal")=="Undefined","SC").otherwise(F.col("meal")))
    .withColumn("adr",
        F.when(F.col("adr")<0,F.lit(0.0))
         .when(F.col("adr")>1000,F.lit(1000.0))
         .otherwise(F.col("adr")))
    .withColumn("children", F.col("children").cast(IntegerType()))
    .withColumn("arrival_month_num", month_map[F.col("arrival_date_month")])
    .withColumn("arrival_date",
        F.to_date(F.concat_ws("-",
            F.col("arrival_date_year"),
            F.col("arrival_month_num"),
            F.col("arrival_date_day_of_month")), "yyyy-M-d"))
    .withColumn("reservation_status_date",
        F.to_date(F.col("reservation_status_date"), "yyyy-MM-dd"))
    .withColumn("total_nights",
        F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights"))
    .withColumn("is_day_use",
        F.when(F.col("total_nights")==0,True).otherwise(False))
    .drop("_ingested_at","_source_file","arrival_month_num")
)

df_guests_l = df_guests.select(
    "email", F.col("guest_id"),
    F.col("loyalty_tier")          .alias("g_loyalty_tier"),
    F.col("lifetime_spend")        .alias("g_lifetime_spend"),
    F.col("total_stays")           .alias("g_total_stays"),
    F.col("total_cancellations")   .alias("g_total_cancellations"),
    F.col("preferred_meal")        .alias("g_preferred_meal"),
    F.col("preferred_room")        .alias("g_preferred_room"),
    F.col("total_special_requests").alias("g_total_special_requests"),
    F.col("joined_date")           .alias("g_joined_date"),
)

df_packages_l = df_packages.select(
    "meal_type","room_type","hotel_type",
    F.col("package_id")        .alias("p_package_id"),
    F.col("package_name")      .alias("p_package_name"),
    F.col("base_price")        .alias("p_base_price"),
    F.col("includes_spa")      .alias("p_includes_spa"),
    F.col("includes_transfer") .alias("p_includes_transfer"),
    F.col("is_premium")        .alias("p_is_premium"),
)

df_joined = (
    df_clean
    .join(df_guests_l, on="email", how="left")
    .join(df_packages_l,
        on=[df_clean["meal"]               == df_packages_l["meal_type"],
            df_clean["reserved_room_type"] == df_packages_l["room_type"],
            df_clean["hotel"]              == df_packages_l["hotel_type"]],
        how="left")
    .drop("meal_type","room_type","hotel_type")
)

df_silver = (
    df_joined
    .withColumn("total_revenue",
        F.round(F.col("adr") * F.col("total_nights"), 2))
    .withColumn("is_premium_meal",
        F.col("meal").isin(["HB","FB"]))
    .withColumn("is_premium_room",
        F.col("reserved_room_type").isin(["F","G","H"]))
    .withColumn("is_premium_booking",
        F.col("is_premium_meal") | F.col("is_premium_room"))
    .withColumn("is_catalogue_premium",
        F.when(F.col("p_is_premium")==1,True)
         .when(F.col("p_is_premium")==0,False)
         .otherwise(None))
    .withColumn("price_gap_per_night",
        F.round(F.col("p_base_price") - F.col("adr"), 2))
    .withColumn("was_upgraded",
        F.col("reserved_room_type") != F.col("assigned_room_type"))
    .withColumn("is_upsell_opportunity",
        (~F.col("is_premium_booking")) &
        (F.col("g_loyalty_tier").isin(["Gold","Platinum"])))
    .withColumn("lead_time_bucket",
        F.when(F.col("lead_time")<7,   "< 1 week")
         .when(F.col("lead_time")<30,  "1-4 weeks")
         .when(F.col("lead_time")<90,  "1-3 months")
         .when(F.col("lead_time")<180, "3-6 months")
         .otherwise("6+ months"))
    .withColumn("guest_value_segment",
        F.when(F.col("g_loyalty_tier")=="Platinum","High Value")
         .when(F.col("g_loyalty_tier")=="Gold",    "Mid-High Value")
         .when(F.col("g_loyalty_tier")=="Silver",  "Mid Value")
         .otherwise("Standard"))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

cnt = overwrite_table(df_silver, "silver_bookings")
print(f"✅ silver_bookings rebuilt: {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### Step 5 — Rebuild Gold

# COMMAND ----------

print("STEP 5 — REBUILDING GOLD")

df_completed = df_silver.filter(F.col("is_canceled") == 0)

def write_gold(df, table_name):
    cnt = overwrite_table(
        df.withColumn("_gold_updated_at", F.current_timestamp()),
        table_name
    )
    print(f"  ✅ {table_name:<35} {cnt:>8,} rows")

write_gold(
    df_completed
    .groupBy("arrival_date_year","arrival_date_month",
             F.month("arrival_date").alias("arrival_month_num"),
             "meal","is_premium_meal","hotel")
    .agg(F.count("*").alias("bookings"),
         F.round(F.avg("adr"),2).alias("avg_adr"),
         F.round(F.avg("total_nights"),2).alias("avg_nights"),
         F.round(F.sum("total_revenue"),2).alias("total_revenue"),
         F.round(F.avg("p_base_price"),2).alias("avg_catalogue_price"),
         F.round(F.avg("price_gap_per_night"),2).alias("avg_price_gap"))
    .orderBy("arrival_date_year","arrival_month_num","meal"),
    "gold_revenue_by_package")

write_gold(
    df_completed
    .groupBy("arrival_date_year","arrival_date_month",
             F.month("arrival_date").alias("arrival_month_num"),"hotel")
    .agg(F.count("*").alias("total_bookings"),
         F.sum(F.col("is_premium_booking").cast("int")).alias("premium_bookings"),
         F.sum(F.col("is_premium_meal").cast("int")).alias("premium_meal_bookings"),
         F.sum(F.col("is_premium_room").cast("int")).alias("premium_room_bookings"),
         F.round(F.sum("total_revenue"),2).alias("total_revenue"),
         F.round(F.sum(F.when(F.col("is_premium_booking"),
             F.col("total_revenue")).otherwise(0)),2).alias("premium_revenue"),
         F.round(F.avg("adr"),2).alias("avg_adr"))
    .withColumn("premium_conversion_pct",
        F.round(F.col("premium_bookings")/F.col("total_bookings")*100,2))
    .withColumn("premium_revenue_pct",
        F.round(F.col("premium_revenue")/F.col("total_revenue")*100,2))
    .orderBy("arrival_date_year","arrival_month_num"),
    "gold_premium_conversion")

write_gold(
    df_completed
    .groupBy("g_loyalty_tier","hotel")
    .agg(F.count("*").alias("total_bookings"),
         F.sum(F.col("is_premium_booking").cast("int")).alias("premium_bookings"),
         F.round(F.avg("adr"),2).alias("avg_adr"),
         F.round(F.avg("total_nights"),2).alias("avg_nights"),
         F.round(F.sum("total_revenue"),2).alias("total_revenue"),
         F.round(F.avg("g_lifetime_spend"),2).alias("avg_lifetime_spend"),
         F.sum(F.col("is_upsell_opportunity").cast("int")).alias("upsell_opportunities"))
    .withColumn("premium_rate_pct",
        F.round(F.col("premium_bookings")/F.col("total_bookings")*100,2))
    .withColumn("revenue_per_booking",
        F.round(F.col("total_revenue")/F.col("total_bookings"),2)),
    "gold_revenue_by_loyalty")

write_gold(
    df_completed
    .filter(F.col("market_segment")!="Undefined")
    .groupBy("market_segment","hotel")
    .agg(F.count("*").alias("total_bookings"),
         F.sum(F.col("is_premium_booking").cast("int")).alias("premium_bookings"),
         F.round(F.avg("adr"),2).alias("avg_adr"),
         F.round(F.avg("total_nights"),2).alias("avg_nights"),
         F.round(F.sum("total_revenue"),2).alias("total_revenue"),
         F.round(F.avg("total_of_special_requests"),2).alias("avg_special_requests"))
    .withColumn("premium_rate_pct",
        F.round(F.col("premium_bookings")/F.col("total_bookings")*100,2))
    .withColumn("revenue_per_booking",
        F.round(F.col("total_revenue")/F.col("total_bookings"),2))
    .orderBy("avg_adr",ascending=False),
    "gold_revenue_by_segment")

write_gold(
    df_completed
    .filter(~F.col("is_premium_booking"))
    .groupBy("g_loyalty_tier","hotel","meal","guest_value_segment")
    .agg(F.count("*").alias("standard_bookings"),
         F.round(F.avg("adr"),2).alias("avg_current_adr"),
         F.round(F.avg("p_base_price"),2).alias("avg_catalogue_price"),
         F.round(F.sum("total_revenue"),2).alias("current_revenue"),
         F.round(F.avg("price_gap_per_night"),2).alias("avg_price_gap"),
         F.round(F.avg("total_nights"),2).alias("avg_nights"))
    .withColumn("estimated_uplift_30pct",
        F.round(F.col("standard_bookings")*0.30*
                F.col("avg_price_gap")*F.col("avg_nights"),2)),
    "gold_upsell_opportunity")

write_gold(
    df_completed
    .groupBy("reserved_room_type","is_premium_room","hotel")
    .agg(F.count("*").alias("total_bookings"),
         F.round(F.avg("adr"),2).alias("avg_adr"),
         F.round(F.avg("total_nights"),2).alias("avg_nights"),
         F.round(F.sum("total_revenue"),2).alias("total_revenue"),
         F.sum(F.col("was_upgraded").cast("int")).alias("upgrades_received"),
         F.round(F.avg("p_base_price"),2).alias("avg_catalogue_price"))
    .withColumn("upgrade_rate_pct",
        F.round(F.col("upgrades_received")/F.col("total_bookings")*100,2))
    .withColumn("revenue_per_booking",
        F.round(F.col("total_revenue")/F.col("total_bookings"),2))
    .orderBy("avg_adr",ascending=False),
    "gold_revenue_by_room")

print(f"\n✅ All 6 Gold tables rebuilt at {RUN_TIMESTAMP}")

# COMMAND ----------
# MAGIC %md ### Step 6 — Refreshed Dashboard

# COMMAND ----------

COLORS = {
    "premium":"#2563EB","standard":"#94A3B8","revenue":"#0EA5E9","gold":"#F59E0B",
    "platinum":"#8B5CF6","silver":"#64748B","bronze":"#B45309","positive":"#10B981",
    "bg":"#0F172A","card_bg":"#1E293B","text":"#F1F5F9","subtext":"#94A3B8",
}
MEAL_COLORS = {"BB":"#94A3B8","SC":"#CBD5E1","HB":"#3B82F6","FB":"#1D4ED8"}

df_pkg  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_package").toPandas()
df_conv = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_premium_conversion").toPandas()
df_loy  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_loyalty").toPandas()
df_seg  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_segment").toPandas()
df_ups  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_upsell_opportunity").toPandas()
df_room = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_room").toPandas()

df_pkg["year_month"] = (
    df_pkg["arrival_date_year"].astype(str) + "-" +
    df_pkg["arrival_month_num"].astype(str).str.zfill(2)
)
df_pkg_agg = df_pkg.groupby(["year_month","meal"])["total_revenue"] \
    .sum().reset_index().sort_values("year_month")

df_conv["year_month"] = (
    df_conv["arrival_date_year"].astype(str) + "-" +
    df_conv["arrival_month_num"].astype(str).str.zfill(2)
)
df_conv_agg = (
    df_conv.groupby("year_month")
    .agg(total_bookings=("total_bookings","sum"),
         premium_bookings=("premium_bookings","sum"),
         total_revenue=("total_revenue","sum"),
         premium_revenue=("premium_revenue","sum"))
    .reset_index().sort_values("year_month")
)
df_conv_agg["premium_conversion_pct"] = (
    df_conv_agg["premium_bookings"]/df_conv_agg["total_bookings"]*100).round(1)
df_conv_agg["premium_revenue_pct"] = (
    df_conv_agg["premium_revenue"]/df_conv_agg["total_revenue"]*100).round(1)

tier_order  = ["Platinum","Gold","Silver","Bronze"]
tier_colors = [COLORS["platinum"],COLORS["gold"],COLORS["silver"],COLORS["bronze"]]
df_loy_agg = (
    df_loy.groupby("g_loyalty_tier")
    .agg(total_bookings=("total_bookings","sum"),
         premium_bookings=("premium_bookings","sum"),
         total_revenue=("total_revenue","sum"),
         avg_adr=("avg_adr","mean"))
    .reset_index()
)
df_loy_agg["premium_rate_pct"] = (
    df_loy_agg["premium_bookings"]/df_loy_agg["total_bookings"]*100).round(1)
df_loy_agg["tier_order"] = df_loy_agg["g_loyalty_tier"].map(
    {t:i for i,t in enumerate(tier_order)})
df_loy_agg = df_loy_agg.sort_values("tier_order")

df_seg_agg = (
    df_seg[df_seg["market_segment"]!="Complementary"]
    .groupby("market_segment")
    .agg(avg_adr=("avg_adr","mean"),total_revenue=("total_revenue","sum"))
    .reset_index().sort_values("avg_adr",ascending=True)
)

df_room_agg = (
    df_room.groupby(["reserved_room_type","is_premium_room"])
    .agg(avg_adr=("avg_adr","mean"),total_revenue=("total_revenue","sum"))
    .reset_index().sort_values("avg_adr",ascending=False)
)
room_colors = [COLORS["premium"] if p else COLORS["standard"]
               for p in df_room_agg["is_premium_room"]]

df_ups_agg = (
    df_ups.groupby("g_loyalty_tier")
    .agg(standard_bookings=("standard_bookings","sum"),
         estimated_uplift=("estimated_uplift_30pct","sum"))
    .reset_index()
)
df_ups_agg["tier_order"] = df_ups_agg["g_loyalty_tier"].map(
    {t:i for i,t in enumerate(tier_order)})
df_ups_agg = df_ups_agg.sort_values("tier_order")

total_revenue    = df_conv_agg["total_revenue"].sum()
premium_revenue  = df_conv_agg["premium_revenue"].sum()
total_bookings   = df_conv_agg["total_bookings"].sum()
premium_bookings = df_conv_agg["premium_bookings"].sum()
upsell_total     = df_ups_agg["standard_bookings"].sum()

print(f"✅ Dashboard data loaded")
print(f"   Total bookings   : {total_bookings:,}")
print(f"   Premium bookings : {premium_bookings:,}")
print(f"   Total revenue    : €{total_revenue:,.2f}")
print(f"   Premium revenue  : €{premium_revenue:,.2f}")

# COMMAND ----------

# KPI Cards
fig_kpi = go.Figure()
kpis = [
    ("Total Revenue",        f"€{total_revenue/1e6:.2f}M",
     "All completed bookings",                              COLORS["revenue"]),
    ("Premium Revenue",      f"€{premium_revenue/1e6:.2f}M",
     f"{premium_revenue/total_revenue*100:.1f}% of total", COLORS["premium"]),
    ("Premium Conv. Rate",   f"{premium_bookings/total_bookings*100:.1f}%",
     "HB / FB / F / G / H bookings",                       COLORS["positive"]),
    ("Upsell Opportunities", f"{upsell_total:,}",
     "Loyal guests on standard packages",                   COLORS["gold"]),
]
for i,(label,value,subtitle,color) in enumerate(kpis):
    x = i/4+0.125
    for y,txt,sz,col in [
        (0.65, value,             32, color),
        (0.30, f"<b>{label}</b>", 13, COLORS["text"]),
        (0.08, subtitle,          10, COLORS["subtext"]),
    ]:
        fig_kpi.add_annotation(x=x, y=y, text=txt,
            font=dict(size=sz, color=col,
                      family="Arial Black" if sz==32 else "Arial"),
            showarrow=False, xref="paper", yref="paper")
fig_kpi.update_layout(
    title=dict(
        text=f"📊 Premium Package Revenue Dashboard — Updated: {RUN_TIMESTAMP}",
        font=dict(size=18,color=COLORS["text"]), x=0.5),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
    height=180, margin=dict(l=20,r=20,t=50,b=10),
    xaxis=dict(visible=False), yaxis=dict(visible=False))
for x in [0.25,0.50,0.75]:
    fig_kpi.add_vline(x=x, line_color="#334155", line_width=1)
fig_kpi.show()

# COMMAND ----------

# Chart 1 — Monthly Revenue by Meal Package
fig1 = go.Figure()
meal_labels = {"BB":"Bed & Breakfast","HB":"Half Board",
               "FB":"Full Board","SC":"Self Catering"}
for meal in ["FB","HB","BB","SC"]:
    d = df_pkg_agg[df_pkg_agg["meal"]==meal]
    fig1.add_trace(go.Scatter(
        x=d["year_month"], y=d["total_revenue"],
        name=meal_labels.get(meal,meal), mode="lines+markers",
        line=dict(color=MEAL_COLORS.get(meal,"#888"), width=2.5),
        marker=dict(size=5)))
fig1.update_layout(
    title=dict(text="📈 Monthly Revenue by Meal Package",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(gridcolor="#334155",tickangle=45,tickfont=dict(size=9)),
    yaxis=dict(title="Revenue (€)",gridcolor="#334155",tickformat="€,.0f"),
    legend=dict(bgcolor=COLORS["bg"]),
    hovermode="x unified", height=400, margin=dict(l=60,r=20,t=50,b=80))
fig1.add_annotation(
    text="💡 HB/FB guests generate 1.6–2× more revenue per booking than BB",
    xref="paper", yref="paper", x=0.01, y=-0.22,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False)
fig1.show()

# COMMAND ----------

# Chart 2 — Premium Conversion Rate
fig2 = make_subplots(specs=[[{"secondary_y":True}]])
fig2.add_trace(go.Scatter(
    x=df_conv_agg["year_month"], y=df_conv_agg["premium_conversion_pct"],
    name="Booking Conv. %", mode="lines+markers",
    line=dict(color=COLORS["premium"],width=2.5), marker=dict(size=6)),
    secondary_y=False)
fig2.add_trace(go.Bar(
    x=df_conv_agg["year_month"], y=df_conv_agg["total_bookings"],
    name="Total Bookings", marker_color="rgba(148,163,184,0.15)"),
    secondary_y=True)
fig2.update_layout(
    title=dict(text="📈 Premium Conversion Rate Over Time",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(gridcolor="#334155",tickangle=45,tickfont=dict(size=9)),
    hovermode="x unified", height=400,
    margin=dict(l=60,r=60,t=50,b=80), barmode="overlay")
fig2.update_yaxes(title_text="Conv %",gridcolor="#334155",
    ticksuffix="%",secondary_y=False)
fig2.update_yaxes(title_text="Total Bookings",showgrid=False,secondary_y=True)
fig2.add_annotation(
    text="💡 Peak Jul–Aug (38%). Oct–Jan dips below 15% — push premium in off-peak",
    xref="paper", yref="paper", x=0.01, y=-0.22,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False)
fig2.show()

# COMMAND ----------

# Chart 3 — Revenue by Loyalty Tier
fig3 = make_subplots(specs=[[{"secondary_y":True}]])
fig3.add_trace(go.Bar(
    x=df_loy_agg["g_loyalty_tier"], y=df_loy_agg["total_revenue"],
    name="Revenue", marker_color=tier_colors), secondary_y=False)
fig3.add_trace(go.Scatter(
    x=df_loy_agg["g_loyalty_tier"], y=df_loy_agg["premium_rate_pct"],
    name="Premium Rate %", mode="lines+markers",
    line=dict(color=COLORS["positive"],width=3),
    marker=dict(size=10,symbol="diamond")), secondary_y=True)
fig3.update_layout(
    title=dict(text="💎 Revenue & Premium Rate by Loyalty Tier",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    height=400, margin=dict(l=60,r=60,t=50,b=60), bargap=0.35)
fig3.update_yaxes(title_text="Revenue (€)",gridcolor="#334155",
    tickformat="€,.0f",secondary_y=False)
fig3.update_yaxes(title_text="Premium Rate %",
    ticksuffix="%",showgrid=False,secondary_y=True)
fig3.add_annotation(
    text="💡 Platinum: 52% premium rate vs Bronze: 12.5% — tier is the strongest predictor",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False)
fig3.show()

# COMMAND ----------

# Chart 4 — ADR by Market Segment
seg_colors = [COLORS["premium"] if a>=110 else
              COLORS["revenue"] if a>=80  else
              COLORS["standard"] for a in df_seg_agg["avg_adr"]]
fig4 = go.Figure(go.Bar(
    x=df_seg_agg["avg_adr"], y=df_seg_agg["market_segment"],
    orientation="h", marker_color=seg_colors,
    text=df_seg_agg["avg_adr"].apply(lambda x:f"€{x:.0f}"),
    textposition="outside"))
fig4.add_vline(x=df_seg_agg["avg_adr"].mean(), line_dash="dash",
    line_color=COLORS["gold"], line_width=1.5,
    annotation_text=f"Avg: €{df_seg_agg['avg_adr'].mean():.0f}",
    annotation_font_color=COLORS["gold"], annotation_position="top right")
fig4.update_layout(
    title=dict(text="🎯 ADR by Market Segment",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Avg ADR (€)",gridcolor="#334155",tickprefix="€"),
    height=360, margin=dict(l=130,r=60,t=50,b=60))
fig4.add_annotation(
    text="💡 Direct & Online TA have highest ADR (€114) — focus premium promotions here",
    xref="paper", yref="paper", x=0.01, y=-0.15,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False)
fig4.show()

# COMMAND ----------

# Chart 5 — Revenue by Room Type
fig5 = make_subplots(specs=[[{"secondary_y":True}]])
fig5.add_trace(go.Bar(
    x=df_room_agg["reserved_room_type"], y=df_room_agg["total_revenue"],
    name="Revenue", marker_color=room_colors), secondary_y=False)
fig5.add_trace(go.Scatter(
    x=df_room_agg["reserved_room_type"], y=df_room_agg["avg_adr"],
    name="Avg ADR", mode="lines+markers",
    line=dict(color=COLORS["gold"],width=3), marker=dict(size=9)),
    secondary_y=True)
fig5.update_layout(
    title=dict(text="🏨 Revenue & ADR by Room Type  ■ Premium (F/G/H)  ■ Standard",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Room Type",gridcolor="#334155",
        categoryorder="array",
        categoryarray=df_room_agg["reserved_room_type"].tolist()),
    height=400, margin=dict(l=60,r=60,t=60,b=60), bargap=0.3)
fig5.update_yaxes(title_text="Revenue (€)",gridcolor="#334155",
    tickformat="€,.0f",secondary_y=False)
fig5.update_yaxes(title_text="ADR (€)",tickprefix="€",
    showgrid=False,secondary_y=True)
fig5.add_annotation(
    text="💡 Room H: €181 ADR but only 356 bookings — premium rooms are massively underbooked",
    xref="paper", yref="paper", x=0.01, y=-0.20,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False)
fig5.show()

# COMMAND ----------

# Chart 6 — Upsell Opportunity
fig6 = make_subplots(specs=[[{"secondary_y":True}]])
fig6.add_trace(go.Bar(
    x=df_ups_agg["g_loyalty_tier"], y=df_ups_agg["standard_bookings"],
    name="Upsell Targets",
    marker_color=[COLORS["platinum"],COLORS["gold"],
                  COLORS["silver"],COLORS["bronze"]]),
    secondary_y=False)
fig6.add_trace(go.Scatter(
    x=df_ups_agg["g_loyalty_tier"], y=df_ups_agg["estimated_uplift"],
    name="Est. Uplift €", mode="lines+markers",
    line=dict(color=COLORS["positive"],width=3),
    marker=dict(size=10,symbol="diamond")), secondary_y=True)
fig6.update_layout(
    title=dict(text="🚀 Upsell Opportunity by Loyalty Tier",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    height=400, margin=dict(l=60,r=80,t=50,b=60), bargap=0.35)
fig6.update_yaxes(title_text="Standard Bookings",secondary_y=False)
fig6.update_yaxes(title_text="Est. Uplift (€)",tickformat="€,.0f",
    showgrid=False,secondary_y=True)
fig6.add_annotation(
    text="💡 34,851 Bronze guests on standard packages — converting 30% = major revenue uplift",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False)
fig6.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Auto-Update Complete
# MAGIC
# MAGIC | Step | Action | Status |
# MAGIC |------|--------|--------|
# MAGIC | 1 | Detect new data | ✅ |
# MAGIC | 2 | Bronze overwrite (bookings) | ✅ |
# MAGIC | 3 | Bronze overwrite (guests) | ✅ |
# MAGIC | 4 | Silver rebuild | ✅ |
# MAGIC | 5 | Gold rebuild (6 tables) | ✅ |
# MAGIC | 6 | Dashboard refresh | ✅ |
