# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  AUTO-UPDATE NOTEBOOK — Objective 2: Traveler Satisfaction & Booking Friction
#  Project  : Flight Passenger Analysis
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Purpose  : Run when new data is added. Detects new rows, updates Bronze,
#             rebuilds Silver + Gold, refreshes dashboard.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔄 Auto-Update Pipeline — Objective 2
# MAGIC **Run this notebook whenever new flight data is added**
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Detect new rows in source files |
# MAGIC | 2 | Overwrite bronze_flights with full updated CSV |
# MAGIC | 3 | Overwrite bronze_passengers + bronze_routes from DB |
# MAGIC | 4 | Rebuild silver_flights |
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
DATABASE_NAME    = "flight_project"
VOLUME_ROOT      = "/Volumes/hotel_catalog/hotel_project/raw_data/flight_project"
FLIGHT_CSV_PATH  = f"{VOLUME_ROOT}/synthetic_flight_passenger_data.csv"
DB_LOCAL_PATH    = f"{VOLUME_ROOT}/flight_passengers.db"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

# ── Column type lists ─────────────────────────────────────────────────────────
INT_COLS_F   = ["Flight_Duration_Minutes","Distance_Miles","Age",
                "Bags_Checked","No_Show","Weather_Impact"]
FLOAT_COLS_F = ["Price_USD","Flight_Satisfaction_Score","Delay_Minutes",
                "Booking_Days_In_Advance"]
STR_COLS_F   = ["Passenger_ID","Flight_ID","Airline","Departure_Airport",
                "Arrival_Airport","Departure_Time","Flight_Status","Gender",
                "Income_Level","Travel_Purpose","Seat_Class",
                "Frequent_Flyer_Status","Check_in_Method","Seat_Selected"]

INT_COLS_P   = ["age","total_flights","total_no_shows"]
FLOAT_COLS_P = ["no_show_rate","avg_satisfaction","avg_price_paid","total_delay_minutes"]
STR_COLS_P   = ["Passenger_ID","gender","income_level","frequent_flyer_status",
                "satisfaction_segment","preferred_class","preferred_checkin",
                "primary_travel_purpose","preferred_airline"]

INT_COLS_R   = ["total_flights"]
FLOAT_COLS_R = ["avg_satisfaction","avg_delay_minutes","no_show_rate",
                "pct_cancelled","pct_delayed","pct_on_time",
                "avg_price_usd","avg_duration_minutes",
                "avg_distance_miles","weather_impact_rate"]
STR_COLS_R   = ["route_id","Departure_Airport","Arrival_Airport",
                "Airline","performance_tier"]

def cast_types(df_pd, int_cols, float_cols, str_cols):
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

def overwrite_table(df, table_name):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.{table_name}")
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema","true") \
        .saveAsTable(f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}")
    cnt = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}").count()
    return cnt

RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"✅ Setup complete — {RUN_TIMESTAMP}")

# COMMAND ----------
# MAGIC %md ### Step 1 — Detect New Data

# COMMAND ----------

print("=" * 55)
print("STEP 1 — DETECTING NEW DATA")
print("=" * 55)

source_csv_count    = len(pd.read_csv(FLIGHT_CSV_PATH, usecols=["Passenger_ID"]))
conn                = sqlite3.connect(DB_LOCAL_PATH)
source_pass_count   = pd.read_sql("SELECT COUNT(*) AS n FROM passengers", conn).iloc[0,0]
source_route_count  = pd.read_sql("SELECT COUNT(*) AS n FROM routes",     conn).iloc[0,0]
conn.close()

bronze_flights_count    = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_flights").count()
bronze_pass_count       = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers").count()

new_flights     = source_csv_count   - bronze_flights_count
new_passengers  = source_pass_count  - bronze_pass_count

print(f"\n{'Source':<35} {'In Source':>10} {'In Bronze':>10} {'New Rows':>10}")
print("-" * 68)
print(f"{'synthetic_flight_passenger_data.csv':<35} {source_csv_count:>10,} {bronze_flights_count:>10,} {new_flights:>10,}")
print(f"{'flight_passengers.db → passengers':<35} {source_pass_count:>10,} {bronze_pass_count:>10,} {new_passengers:>10,}")
print(f"{'flight_passengers.db → routes':<35} {source_route_count:>10,} {'—':>10} {'—':>10}")

if new_flights <= 0 and new_passengers <= 0:
    print("\n⚠️  No new rows detected.")
    print("    Pipeline will still rebuild Silver + Gold + Dashboard.")
else:
    print(f"\n✅ New data detected — proceeding with full update")

# COMMAND ----------
# MAGIC %md ### Step 2 — Update Bronze: Flights (Full Overwrite)

# COMMAND ----------

print("STEP 2 — UPDATING BRONZE FLIGHTS")

flights_pd = pd.read_csv(FLIGHT_CSV_PATH, keep_default_na=True, na_values=["NULL",""])
flights_pd = cast_types(flights_pd, INT_COLS_F, FLOAT_COLS_F, STR_COLS_F)
print(f"   CSV read: {len(flights_pd):,} rows, types cast ✅")

df_new_flights = (
    spark.createDataFrame(flights_pd)
    .withColumns({
        "_ingested_at": F.current_timestamp(),
        "_source_file": F.lit(FLIGHT_CSV_PATH),
    })
)

cnt = overwrite_table(df_new_flights, "bronze_flights")
print(f"✅ bronze_flights: {bronze_flights_count:,} → {cnt:,} rows  (+{cnt - bronze_flights_count})")

# COMMAND ----------
# MAGIC %md ### Step 3 — Update Bronze: Passengers + Routes (Full Overwrite)

# COMMAND ----------

print("STEP 3 — UPDATING BRONZE PASSENGERS + ROUTES")

conn          = sqlite3.connect(DB_LOCAL_PATH)
passengers_pd = pd.read_sql("SELECT * FROM passengers", conn)
routes_pd     = pd.read_sql("SELECT * FROM routes",     conn)
conn.close()

passengers_pd = cast_types(passengers_pd, INT_COLS_P, FLOAT_COLS_P, STR_COLS_P)
routes_pd     = cast_types(routes_pd,     INT_COLS_R, FLOAT_COLS_R, STR_COLS_R)

print(f"   Passengers read: {len(passengers_pd):,} rows ✅")
print(f"   Routes read    : {len(routes_pd):,} rows ✅")

df_new_pass = (
    spark.createDataFrame(passengers_pd)
    .withColumns({
        "_ingested_at": F.current_timestamp(),
        "_source_file": F.lit(f"sqlite::{DB_LOCAL_PATH}::passengers"),
    })
)

df_new_routes = (
    spark.createDataFrame(routes_pd)
    .withColumns({
        "_ingested_at": F.current_timestamp(),
        "_source_file": F.lit(f"sqlite::{DB_LOCAL_PATH}::routes"),
    })
)

cnt_p = overwrite_table(df_new_pass,   "bronze_passengers")
cnt_r = overwrite_table(df_new_routes, "bronze_routes")
print(f"✅ bronze_passengers: {bronze_pass_count:,} → {cnt_p:,} rows  (+{cnt_p - bronze_pass_count})")
print(f"✅ bronze_routes    : → {cnt_r:,} rows")

# COMMAND ----------
# MAGIC %md ### Step 4 — Rebuild Silver

# COMMAND ----------

print("STEP 4 — REBUILDING SILVER")

df_flights    = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_flights")
df_passengers = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_passengers")
df_routes     = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.bronze_routes")

# ── Clean ─────────────────────────────────────────────────────────────────────
df_clean = (
    df_flights
    .fillna({"Frequent_Flyer_Status":"Non-Member","Delay_Minutes":0.0,"Bags_Checked":0})
    .withColumn("Delay_Minutes",
        F.when(F.col("Delay_Minutes")<0,   F.lit(0.0))
         .when(F.col("Delay_Minutes")>300, F.lit(300.0))
         .otherwise(F.col("Delay_Minutes")))
    .withColumn("departure_ts",
        F.to_timestamp(F.col("Departure_Time"),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("departure_date",   F.to_date(F.col("departure_ts")))
    .withColumn("departure_hour",   F.hour(F.col("departure_ts")))
    .withColumn("departure_month",  F.month(F.col("departure_ts")))
    .withColumn("departure_year",   F.year(F.col("departure_ts")))
    .drop("_ingested_at","_source_file","Departure_Time")
)

# ── Lookups ───────────────────────────────────────────────────────────────────
df_pass_l = df_passengers.select(
    "Passenger_ID",
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

df_routes_l = df_routes.select(
    "Departure_Airport","Arrival_Airport","Airline",
    F.col("route_id")            .alias("r_route_id"),
    F.col("avg_satisfaction")    .alias("r_avg_satisfaction"),
    F.col("avg_delay_minutes")   .alias("r_avg_delay_minutes"),
    F.col("no_show_rate")        .alias("r_no_show_rate"),
    F.col("pct_on_time")         .alias("r_pct_on_time"),
    F.col("pct_cancelled")       .alias("r_pct_cancelled"),
    F.col("weather_impact_rate") .alias("r_weather_impact_rate"),
    F.col("performance_tier")    .alias("r_performance_tier"),
    F.col("avg_price_usd")       .alias("r_avg_price_usd"),
)

# ── Joins ─────────────────────────────────────────────────────────────────────
df_joined = (
    df_clean
    .join(df_pass_l, on="Passenger_ID", how="left")
    .join(df_routes_l,
        on=["Departure_Airport","Arrival_Airport","Airline"],
        how="left")
)

# ── Derived columns ───────────────────────────────────────────────────────────
df_silver = (
    df_joined
    .withColumn("is_satisfied",       F.col("Flight_Satisfaction_Score") >= 7.0)
    .withColumn("is_highly_satisfied",F.col("Flight_Satisfaction_Score") >= 8.0)
    .withColumn("is_dissatisfied",    F.col("Flight_Satisfaction_Score") <  5.0)
    .withColumn("satisfaction_band",
        F.when(F.col("Flight_Satisfaction_Score") >= 8.0, "Highly Satisfied")
         .when(F.col("Flight_Satisfaction_Score") >= 7.0, "Satisfied")
         .when(F.col("Flight_Satisfaction_Score") >= 5.0, "Neutral")
         .otherwise("Dissatisfied"))
    .withColumn("is_delayed",    F.col("Delay_Minutes") > 0)
    .withColumn("delay_bucket",
        F.when(F.col("Delay_Minutes") == 0,  "No Delay")
         .when(F.col("Delay_Minutes") <= 15, "< 15 min")
         .when(F.col("Delay_Minutes") <= 30, "15-30 min")
         .when(F.col("Delay_Minutes") <= 60, "30-60 min")
         .otherwise("> 60 min"))
    .withColumn("booking_bucket",
        F.when(F.col("Booking_Days_In_Advance") <= 7,  "< 1 week")
         .when(F.col("Booking_Days_In_Advance") <= 30, "1-4 weeks")
         .when(F.col("Booking_Days_In_Advance") <= 60, "1-2 months")
         .when(F.col("Booking_Days_In_Advance") <= 90, "2-3 months")
         .otherwise("3+ months"))
    .withColumn("is_last_minute",   F.col("Booking_Days_In_Advance") <= 7)
    .withColumn("is_no_show",       F.col("No_Show") == 1)
    .withColumn("is_premium_class", F.col("Seat_Class").isin(["Business","First"]))
    .withColumn("is_loyal_passenger",
        F.col("p_frequent_flyer_status").isin(["Gold","Platinum"]))
    .withColumn("time_of_day",
        F.when(F.col("departure_hour") <  6, "Night (0-6)")
         .when(F.col("departure_hour") < 12, "Morning (6-12)")
         .when(F.col("departure_hour") < 18, "Afternoon (12-18)")
         .otherwise("Evening (18-24)"))
    .withColumn("price_vs_route_avg",
        F.round(F.col("Price_USD") - F.col("r_avg_price_usd"), 2))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

cnt = overwrite_table(df_silver, "silver_flights")
print(f"✅ silver_flights rebuilt: {cnt:,} rows")

# COMMAND ----------
# MAGIC %md ### Step 5 — Rebuild Gold

# COMMAND ----------

print("STEP 5 — REBUILDING GOLD")

def write_gold(df, table_name):
    cnt = overwrite_table(
        df.withColumn("_gold_updated_at", F.current_timestamp()),
        table_name
    )
    print(f"  ✅ {table_name:<42} {cnt:>8,} rows")

write_gold(
    df_silver
    .groupBy("p_frequent_flyer_status","Seat_Class",
             "p_income_level","Travel_Purpose","Gender")
    .agg(F.count("*").alias("passengers"),
         F.round(F.avg("Flight_Satisfaction_Score"),2).alias("avg_satisfaction"),
         F.round(F.avg("No_Show"),3).alias("no_show_rate"),
         F.round(F.avg("Delay_Minutes"),2).alias("avg_delay"),
         F.sum(F.col("is_satisfied").cast("int")).alias("satisfied_count"),
         F.sum(F.col("is_highly_satisfied").cast("int")).alias("highly_satisfied_count"),
         F.sum(F.col("is_dissatisfied").cast("int")).alias("dissatisfied_count"),
         F.round(F.avg("Price_USD"),2).alias("avg_price_paid"))
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count")/F.col("passengers")*100,2))
    .withColumn("dissatisfaction_rate_pct",
        F.round(F.col("dissatisfied_count")/F.col("passengers")*100,2))
    .orderBy("avg_satisfaction",ascending=False),
    "gold_satisfaction_by_segment")

write_gold(
    df_silver
    .groupBy("r_route_id","Departure_Airport","Arrival_Airport",
             "Airline","r_performance_tier")
    .agg(F.count("*").alias("total_flights"),
         F.round(F.avg("Flight_Satisfaction_Score"),2).alias("avg_satisfaction"),
         F.round(F.avg("Delay_Minutes"),2).alias("avg_delay_minutes"),
         F.round(F.avg("No_Show"),3).alias("no_show_rate"),
         F.round(F.avg("Weather_Impact"),3).alias("weather_impact_rate"),
         F.sum(F.col("is_satisfied").cast("int")).alias("satisfied_count"),
         F.sum(F.col("is_dissatisfied").cast("int")).alias("dissatisfied_count"),
         F.round(F.avg("Price_USD"),2).alias("avg_price"),
         F.sum(F.when(F.col("Flight_Status")=="Cancelled",1)
                .otherwise(0)).alias("cancelled_flights"))
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count")/F.col("total_flights")*100,2))
    .withColumn("cancellation_rate_pct",
        F.round(F.col("cancelled_flights")/F.col("total_flights")*100,2))
    .filter(F.col("total_flights") >= 5)
    .orderBy("avg_satisfaction",ascending=False),
    "gold_satisfaction_by_route")

write_gold(
    df_silver
    .groupBy("booking_bucket","Travel_Purpose","p_frequent_flyer_status")
    .agg(F.count("*").alias("passengers"),
         F.sum(F.col("is_no_show").cast("int")).alias("no_shows"),
         F.round(F.avg("No_Show"),3).alias("no_show_rate"),
         F.round(F.avg("Flight_Satisfaction_Score"),2).alias("avg_satisfaction"),
         F.round(F.avg("Booking_Days_In_Advance"),1).alias("avg_booking_days"),
         F.round(F.avg("Price_USD"),2).alias("avg_price"),
         F.sum(F.col("is_last_minute").cast("int")).alias("last_minute_bookings"))
    .withColumn("no_show_pct",
        F.round(F.col("no_shows")/F.col("passengers")*100,2))
    .orderBy("no_show_rate",ascending=False),
    "gold_noshow_by_booking_pattern")

write_gold(
    df_silver
    .groupBy("Check_in_Method","Seat_Class","p_frequent_flyer_status")
    .agg(F.count("*").alias("passengers"),
         F.round(F.avg("No_Show"),3).alias("no_show_rate"),
         F.round(F.avg("Flight_Satisfaction_Score"),2).alias("avg_satisfaction"),
         F.round(F.avg("Delay_Minutes"),2).alias("avg_delay"),
         F.round(F.avg("Booking_Days_In_Advance"),1).alias("avg_booking_days"),
         F.sum(F.col("is_no_show").cast("int")).alias("no_shows"),
         F.sum(F.col("is_satisfied").cast("int")).alias("satisfied_count"),
         F.sum(F.col("is_dissatisfied").cast("int")).alias("dissatisfied_count"))
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count")/F.col("passengers")*100,2))
    .withColumn("no_show_pct",
        F.round(F.col("no_shows")/F.col("passengers")*100,2))
    .orderBy("no_show_rate",ascending=False),
    "gold_friction_by_checkin")

write_gold(
    df_silver
    .groupBy("delay_bucket","Flight_Status","Weather_Impact","Airline")
    .agg(F.count("*").alias("flights"),
         F.round(F.avg("Flight_Satisfaction_Score"),2).alias("avg_satisfaction"),
         F.round(F.avg("Delay_Minutes"),2).alias("avg_delay_minutes"),
         F.round(F.avg("No_Show"),3).alias("no_show_rate"),
         F.sum(F.col("is_satisfied").cast("int")).alias("satisfied_count"),
         F.sum(F.col("is_dissatisfied").cast("int")).alias("dissatisfied_count"),
         F.round(F.avg("Price_USD"),2).alias("avg_price"))
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count")/F.col("flights")*100,2))
    .withColumn("dissatisfaction_rate_pct",
        F.round(F.col("dissatisfied_count")/F.col("flights")*100,2))
    .withColumn("delay_order",
        F.when(F.col("delay_bucket")=="No Delay",  1)
         .when(F.col("delay_bucket")=="< 15 min",  2)
         .when(F.col("delay_bucket")=="15-30 min", 3)
         .when(F.col("delay_bucket")=="30-60 min", 4)
         .otherwise(5))
    .orderBy("delay_order","Airline")
    .drop("delay_order"),
    "gold_delay_impact")

write_gold(
    df_silver
    .groupBy("p_frequent_flyer_status","Seat_Class","Check_in_Method",
             "Travel_Purpose","booking_bucket","time_of_day")
    .agg(F.count("*").alias("passengers"),
         F.round(F.avg("Flight_Satisfaction_Score"),2).alias("avg_satisfaction"),
         F.round(F.avg("No_Show"),3).alias("no_show_rate"),
         F.round(F.avg("Delay_Minutes"),2).alias("avg_delay"),
         F.round(F.avg("Price_USD"),2).alias("avg_price"),
         F.round(F.avg("Booking_Days_In_Advance"),1).alias("avg_booking_days"),
         F.sum(F.col("is_satisfied").cast("int")).alias("satisfied_count"),
         F.sum(F.col("is_no_show").cast("int")).alias("no_show_count"),
         F.sum(F.col("is_last_minute").cast("int")).alias("last_minute_count"),
         F.sum(F.col("is_premium_class").cast("int")).alias("premium_class_count"))
    .withColumn("satisfaction_rate_pct",
        F.round(F.col("satisfied_count")/F.col("passengers")*100,2))
    .withColumn("no_show_pct",
        F.round(F.col("no_show_count")/F.col("passengers")*100,2))
    .withColumn("friction_score",
        F.round(
            (F.lit(1) - F.col("avg_satisfaction")/F.lit(10))*50 +
            F.col("no_show_rate")*50, 2))
    .orderBy("friction_score",ascending=False),
    "gold_traveler_profile")

print(f"\n✅ All 6 Gold tables rebuilt at {RUN_TIMESTAMP}")

# COMMAND ----------
# MAGIC %md ### Step 6 — Refreshed Dashboard

# COMMAND ----------

COLORS = {
    "primary":"#2563EB","satisfied":"#10B981","neutral":"#F59E0B",
    "dissatisfied":"#EF4444","noshow":"#8B5CF6","premium":"#0EA5E9",
    "standard":"#94A3B8","bg":"#0F172A","card_bg":"#1E293B",
    "text":"#F1F5F9","subtext":"#94A3B8",
}
TIER_COLORS = {"Platinum":"#8B5CF6","Gold":"#F59E0B",
               "Silver":"#64748B","Non-Member":"#94A3B8"}

df_seg    = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_satisfaction_by_segment").toPandas()
df_route  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_satisfaction_by_route").toPandas()
df_noshow = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_noshow_by_booking_pattern").toPandas()
df_checkin= spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_friction_by_checkin").toPandas()
df_delay  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_delay_impact").toPandas()

# Prepare aggregates
total_passengers   = df_seg["passengers"].sum()
avg_satisfaction   = (df_seg["avg_satisfaction"]*df_seg["passengers"]).sum()/total_passengers
satisfied_count    = df_seg["satisfied_count"].sum()
dissatisfied_count = df_seg["dissatisfied_count"].sum()
total_no_shows     = df_noshow["no_shows"].sum()
overall_no_show    = total_no_shows / df_noshow["passengers"].sum()

seg_tier = (df_seg.groupby("p_frequent_flyer_status")
    .agg(passengers=("passengers","sum"),
         avg_satisfaction=("avg_satisfaction","mean"),
         no_show_rate=("no_show_rate","mean"),
         satisfied_count=("satisfied_count","sum"),
         dissatisfied_count=("dissatisfied_count","sum"))
    .reset_index())
tier_order = ["Platinum","Gold","Silver","Non-Member"]
seg_tier["order"] = seg_tier["p_frequent_flyer_status"].map({t:i for i,t in enumerate(tier_order)})
seg_tier = seg_tier.sort_values("order")

df_silver_spark = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_flights")
band_data = df_silver_spark.groupBy("satisfaction_band").count().toPandas()
band_order = ["Highly Satisfied","Satisfied","Neutral","Dissatisfied"]
band_data["order"] = band_data["satisfaction_band"].map({b:i for i,b in enumerate(band_order)})
band_data = band_data.sort_values("order")

noshow_bucket = (df_noshow.groupby("booking_bucket")
    .agg(passengers=("passengers","sum"),
         no_shows=("no_shows","sum"),
         avg_satisfaction=("avg_satisfaction","mean"))
    .reset_index())
noshow_bucket["no_show_rate"] = (noshow_bucket["no_shows"]/noshow_bucket["passengers"]*100).round(2)
bucket_order = ["< 1 week","1-4 weeks","1-2 months","2-3 months","3+ months"]
noshow_bucket["order"] = noshow_bucket["booking_bucket"].map({b:i for i,b in enumerate(bucket_order)})
noshow_bucket = noshow_bucket.sort_values("order")

checkin_agg = (df_checkin.groupby("Check_in_Method")
    .agg(passengers=("passengers","sum"),no_shows=("no_shows","sum"),
         avg_satisfaction=("avg_satisfaction","mean"),satisfied_count=("satisfied_count","sum"))
    .reset_index())
checkin_agg["no_show_rate"] = (checkin_agg["no_shows"]/checkin_agg["passengers"]*100).round(2)
checkin_agg = checkin_agg.sort_values("no_show_rate",ascending=False)

delay_agg = (df_delay.groupby("delay_bucket")
    .agg(flights=("flights","sum"),avg_satisfaction=("avg_satisfaction","mean"),
         no_show_rate=("no_show_rate","mean"),dissatisfied_count=("dissatisfied_count","sum"))
    .reset_index())
delay_agg["order"] = delay_agg["delay_bucket"].map({b:i for i,b in enumerate(["No Delay","< 15 min","15-30 min","30-60 min","> 60 min"])})
delay_agg = delay_agg.sort_values("order")

top_routes    = df_route.nlargest(8,"avg_satisfaction")
bottom_routes = df_route.nsmallest(8,"avg_satisfaction")

print(f"✅ Dashboard data loaded — {RUN_TIMESTAMP}")
print(f"   Total passengers  : {total_passengers:,}")
print(f"   Avg satisfaction  : {avg_satisfaction:.2f}/10")
print(f"   No-show rate      : {overall_no_show*100:.1f}%")

# COMMAND ----------

# KPI Cards
fig_kpi = go.Figure()
kpis = [
    ("Total Passengers",     f"{total_passengers:,}",
     "Flight records analysed",                           COLORS["primary"]),
    ("Avg Satisfaction",     f"{avg_satisfaction:.2f}/10",
     f"{satisfied_count/total_passengers*100:.1f}% rated ≥ 7.0", COLORS["satisfied"]),
    ("Dissatisfaction Rate", f"{dissatisfied_count/total_passengers*100:.1f}%",
     f"{dissatisfied_count:,} passengers rated < 5.0",   COLORS["dissatisfied"]),
    ("No-Show Rate",         f"{overall_no_show*100:.1f}%",
     f"{total_no_shows:,} passengers did not board",      COLORS["noshow"]),
]
for i,(label,value,subtitle,color) in enumerate(kpis):
    x=i/4+0.125
    for y,txt,sz,col in [(0.65,value,32,color),(0.30,f"<b>{label}</b>",13,COLORS["text"]),(0.08,subtitle,10,COLORS["subtext"])]:
        fig_kpi.add_annotation(x=x,y=y,text=txt,
            font=dict(size=sz,color=col,family="Arial Black" if sz==32 else "Arial"),
            showarrow=False,xref="paper",yref="paper")
fig_kpi.update_layout(
    title=dict(text=f"📊 Traveler Satisfaction & Booking Friction — Updated: {RUN_TIMESTAMP}",
               font=dict(size=18,color=COLORS["text"]),x=0.5),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["bg"],
    height=180,margin=dict(l=20,r=20,t=50,b=10),
    xaxis=dict(visible=False),yaxis=dict(visible=False))
for x in [0.25,0.50,0.75]: fig_kpi.add_vline(x=x,line_color="#334155",line_width=1)
fig_kpi.show()

# COMMAND ----------

# Chart 1 — Satisfaction by Loyalty Tier
fig1 = make_subplots(specs=[[{"secondary_y":True}]])
fig1.add_trace(go.Bar(
    x=seg_tier["p_frequent_flyer_status"],y=seg_tier["avg_satisfaction"],
    name="Avg Satisfaction",
    marker_color=[TIER_COLORS.get(t,COLORS["standard"]) for t in seg_tier["p_frequent_flyer_status"]],
    text=seg_tier["avg_satisfaction"].apply(lambda x:f"{x:.2f}"),textposition="outside"),secondary_y=False)
fig1.add_trace(go.Scatter(
    x=seg_tier["p_frequent_flyer_status"],y=seg_tier["no_show_rate"]*100,
    name="No-Show Rate (%)",mode="lines+markers",
    line=dict(color=COLORS["noshow"],width=3),marker=dict(size=10,symbol="diamond")),secondary_y=True)
fig1.update_layout(
    title=dict(text="✈️ Satisfaction & No-Show Rate by Loyalty Tier",font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["card_bg"],font=dict(color=COLORS["text"]),
    height=420,margin=dict(l=60,r=60,t=50,b=80),bargap=0.35)
fig1.update_yaxes(title_text="Avg Satisfaction",gridcolor="#334155",range=[6.5,7.5],secondary_y=False)
fig1.update_yaxes(title_text="No-Show Rate (%)",ticksuffix="%",showgrid=False,secondary_y=True)
fig1.add_annotation(text="💡 Platinum 7.09 vs Gold 6.94 — loyalty drives satisfaction",
    xref="paper",yref="paper",x=0.01,y=-0.18,font=dict(size=11,color=COLORS["subtext"]),showarrow=False)
fig1.show()

# COMMAND ----------

# Chart 2 — Satisfaction Band Distribution
band_color_map = {"Highly Satisfied":COLORS["satisfied"],"Satisfied":"#34D399",
                  "Neutral":COLORS["neutral"],"Dissatisfied":COLORS["dissatisfied"]}
fig2 = go.Figure(go.Bar(
    x=band_data["satisfaction_band"],y=band_data["count"],
    marker_color=[band_color_map.get(b,COLORS["standard"]) for b in band_data["satisfaction_band"]],
    text=band_data["count"].apply(lambda x:f"{x:,}"),textposition="outside"))
fig2.update_layout(
    title=dict(text="📊 Passenger Satisfaction Distribution",font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["card_bg"],font=dict(color=COLORS["text"]),
    xaxis=dict(gridcolor="#334155",categoryorder="array",categoryarray=band_order),
    yaxis=dict(title="Passengers",gridcolor="#334155"),
    height=400,margin=dict(l=60,r=20,t=50,b=80),bargap=0.3)
fig2.add_annotation(text="💡 9.2% Dissatisfied — fixing this group has the biggest impact on overall satisfaction",
    xref="paper",yref="paper",x=0.01,y=-0.20,font=dict(size=11,color=COLORS["subtext"]),showarrow=False)
fig2.show()

# COMMAND ----------

# Chart 3 — No-Show by Booking Window
fig3 = make_subplots(specs=[[{"secondary_y":True}]])
fig3.add_trace(go.Bar(
    x=noshow_bucket["booking_bucket"],y=noshow_bucket["no_show_rate"],
    name="No-Show Rate (%)",
    marker_color=[COLORS["dissatisfied"] if r>5.5 else COLORS["neutral"] if r>5.0 else COLORS["satisfied"] for r in noshow_bucket["no_show_rate"]],
    text=noshow_bucket["no_show_rate"].apply(lambda x:f"{x:.1f}%"),textposition="outside"),secondary_y=False)
fig3.add_trace(go.Scatter(
    x=noshow_bucket["booking_bucket"],y=noshow_bucket["avg_satisfaction"],
    name="Avg Satisfaction",mode="lines+markers",
    line=dict(color=COLORS["primary"],width=3),marker=dict(size=8)),secondary_y=True)
fig3.update_layout(
    title=dict(text="🗓️ No-Show Rate by Booking Window",font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["card_bg"],font=dict(color=COLORS["text"]),
    xaxis=dict(gridcolor="#334155",categoryorder="array",categoryarray=bucket_order),
    height=420,margin=dict(l=60,r=60,t=50,b=80),bargap=0.3)
fig3.update_yaxes(title_text="No-Show Rate (%)",gridcolor="#334155",ticksuffix="%",secondary_y=False)
fig3.update_yaxes(title_text="Avg Satisfaction",showgrid=False,range=[6.8,7.2],secondary_y=True)
fig3.add_annotation(text="💡 Last-minute bookings (<1 week) have 6.2% no-show — highest friction point",
    xref="paper",yref="paper",x=0.01,y=-0.18,font=dict(size=11,color=COLORS["subtext"]),showarrow=False)
fig3.show()

# COMMAND ----------

# Chart 4 — Check-in Method Friction
fig4 = make_subplots(specs=[[{"secondary_y":True}]])
fig4.add_trace(go.Bar(
    x=checkin_agg["Check_in_Method"],y=checkin_agg["no_show_rate"],
    name="No-Show Rate (%)",
    marker_color=[COLORS["dissatisfied"] if r>5.5 else COLORS["neutral"] if r>4.8 else COLORS["satisfied"] for r in checkin_agg["no_show_rate"]],
    text=checkin_agg["no_show_rate"].apply(lambda x:f"{x:.1f}%"),textposition="outside"),secondary_y=False)
fig4.add_trace(go.Scatter(
    x=checkin_agg["Check_in_Method"],y=checkin_agg["avg_satisfaction"],
    name="Avg Satisfaction",mode="lines+markers",
    line=dict(color=COLORS["satisfied"],width=3),marker=dict(size=10,symbol="diamond")),secondary_y=True)
fig4.update_layout(
    title=dict(text="🎫 Check-in Method: No-Show & Satisfaction",font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["card_bg"],font=dict(color=COLORS["text"]),
    height=420,margin=dict(l=60,r=60,t=50,b=80),bargap=0.35)
fig4.update_yaxes(title_text="No-Show Rate (%)",gridcolor="#334155",ticksuffix="%",secondary_y=False)
fig4.update_yaxes(title_text="Avg Satisfaction",showgrid=False,range=[6.8,7.2],secondary_y=True)
fig4.add_annotation(text="💡 Airport Kiosk highest no-show (5.9%) — Mobile App lowest satisfaction (6.94)",
    xref="paper",yref="paper",x=0.01,y=-0.18,font=dict(size=11,color=COLORS["subtext"]),showarrow=False)
fig4.show()

# COMMAND ----------

# Chart 5 — Delay Impact
delay_order_list = ["No Delay","< 15 min","15-30 min","30-60 min","> 60 min"]
fig5 = make_subplots(specs=[[{"secondary_y":True}]])
fig5.add_trace(go.Bar(
    x=delay_agg["delay_bucket"],y=delay_agg["flights"],
    name="Number of Flights",marker_color="rgba(148,163,184,0.2)"),secondary_y=True)
fig5.add_trace(go.Scatter(
    x=delay_agg["delay_bucket"],y=delay_agg["avg_satisfaction"],
    name="Avg Satisfaction",mode="lines+markers",
    line=dict(color=COLORS["primary"],width=3),marker=dict(size=12)),secondary_y=False)
fig5.update_layout(
    title=dict(text="⏱️ Delay Impact on Satisfaction",font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["card_bg"],font=dict(color=COLORS["text"]),
    xaxis=dict(gridcolor="#334155",categoryorder="array",categoryarray=delay_order_list),
    height=420,margin=dict(l=60,r=60,t=50,b=80),barmode="overlay")
fig5.update_yaxes(title_text="Avg Satisfaction",gridcolor="#334155",range=[6.8,7.2],secondary_y=False)
fig5.update_yaxes(title_text="Number of Flights",showgrid=False,secondary_y=True)
fig5.add_annotation(text="💡 95.1% of flights have some delay — proactive communication reduces friction",
    xref="paper",yref="paper",x=0.01,y=-0.18,font=dict(size=11,color=COLORS["subtext"]),showarrow=False)
fig5.show()

# COMMAND ----------

# Chart 6 — Top vs Bottom Routes
fig6 = make_subplots(rows=1,cols=2,
    subplot_titles=("🏆 Top 8 Routes","⚠️ Bottom 8 Routes"))
fig6.add_trace(go.Bar(
    x=top_routes["avg_satisfaction"],y=top_routes["r_route_id"],
    orientation="h",name="Top",marker_color=COLORS["satisfied"],
    text=top_routes["avg_satisfaction"].apply(lambda x:f"{x:.2f}"),textposition="outside"),row=1,col=1)
fig6.add_trace(go.Bar(
    x=bottom_routes["avg_satisfaction"],y=bottom_routes["r_route_id"],
    orientation="h",name="Bottom",marker_color=COLORS["dissatisfied"],
    text=bottom_routes["avg_satisfaction"].apply(lambda x:f"{x:.2f}"),textposition="outside"),row=1,col=2)
fig6.update_layout(
    title=dict(text="🗺️ Route Satisfaction — Top vs Bottom",font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"],plot_bgcolor=COLORS["card_bg"],font=dict(color=COLORS["text"]),
    showlegend=False,height=450,margin=dict(l=140,r=80,t=80,b=80))
fig6.update_xaxes(gridcolor="#334155",range=[0,10])
fig6.update_yaxes(tickfont=dict(size=9))
fig6.add_annotation(text="💡 Target bottom routes for delay reduction and check-in experience improvements",
    xref="paper",yref="paper",x=0.01,y=-0.12,font=dict(size=11,color=COLORS["subtext"]),showarrow=False)
fig6.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Auto-Update Complete
# MAGIC
# MAGIC | Step | Action | Status |
# MAGIC |------|--------|--------|
# MAGIC | 1 | Detect new data | ✅ |
# MAGIC | 2 | Bronze overwrite (flights) | ✅ |
# MAGIC | 3 | Bronze overwrite (passengers + routes) | ✅ |
# MAGIC | 4 | Silver rebuild | ✅ |
# MAGIC | 5 | Gold rebuild (6 tables) | ✅ |
# MAGIC | 6 | Dashboard refresh | ✅ |
