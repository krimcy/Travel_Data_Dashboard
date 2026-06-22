"""
=============================================================
Step 1 — Flight DB Generation Script
Project: Traveler Satisfaction & Booking Friction (Objective 2)
Source:  Keatonballard, "Synthetic Airline Passenger and Flight Data"
         Kaggle, 2024.
         https://www.kaggle.com/datasets/keatonballard/synthetic-airline-passenger-and-flight-data

Generates two tables derived from real flight data:
  - passengers  : one row per unique passenger with profile + loyalty
  - routes      : one row per route+airline combo with performance KPIs

Output: flight_passengers.db (SQLite)
        flight_passengers.csv / flight_routes.csv (for inspection)
=============================================================
"""

import sqlite3
import pandas as pd
import numpy as np

# ── reproducibility ────────────────────────────────────────
np.random.seed(42)

# ── paths ──────────────────────────────────────────────────
CSV_PATH = "/mnt/user-data/uploads/synthetic_flight_passenger_data.csv"
DB_PATH  = "/home/claude/flight_passengers.db"

# ══════════════════════════════════════════════════════════
#  LOAD SOURCE DATA
# ══════════════════════════════════════════════════════════

print("Loading flight CSV …")
df = pd.read_csv(CSV_PATH)
print(f"  → {len(df):,} rows, {len(df.columns)} columns")

# ══════════════════════════════════════════════════════════
#  TABLE 1 — passengers
#  One row per unique passenger derived from flight history
# ══════════════════════════════════════════════════════════

print("\nBuilding passengers table …")

passengers_df = (
    df.groupby("Passenger_ID")
    .agg(
        age                  = ("Age",                       "first"),
        gender               = ("Gender",                    "first"),
        income_level         = ("Income_Level",              "first"),
        frequent_flyer_status= ("Frequent_Flyer_Status",     "first"),
        total_flights        = ("Flight_ID",                 "count"),
        total_no_shows       = ("No_Show",                   "sum"),
        avg_satisfaction     = ("Flight_Satisfaction_Score", "mean"),
        avg_price_paid       = ("Price_USD",                 "mean"),
        total_delay_minutes  = ("Delay_Minutes",             "sum"),
        preferred_class      = ("Seat_Class",   lambda x: x.mode()[0]),
        preferred_checkin    = ("Check_in_Method", lambda x: x.mode()[0]),
        primary_travel_purpose=("Travel_Purpose", lambda x: x.mode()[0]),
        preferred_airline    = ("Airline",      lambda x: x.mode()[0]),
    )
    .reset_index()
)

# Derived columns
passengers_df["no_show_rate"] = (
    passengers_df["total_no_shows"] / passengers_df["total_flights"]
).round(3)

passengers_df["avg_satisfaction"]  = passengers_df["avg_satisfaction"].round(2)
passengers_df["avg_price_paid"]    = passengers_df["avg_price_paid"].round(2)
passengers_df["total_delay_minutes"]= passengers_df["total_delay_minutes"].round(1)

# Fill nulls in frequent_flyer_status (4,948 nulls = non-members)
passengers_df["frequent_flyer_status"] = (
    passengers_df["frequent_flyer_status"].fillna("Non-Member")
)

# Satisfaction segment
def satisfaction_segment(score):
    if score >= 8.0:
        return "Highly Satisfied"
    elif score >= 6.5:
        return "Satisfied"
    elif score >= 5.0:
        return "Neutral"
    else:
        return "Dissatisfied"

passengers_df["satisfaction_segment"] = (
    passengers_df["avg_satisfaction"].apply(satisfaction_segment)
)

# Final column order
passengers_df = passengers_df[[
    "Passenger_ID",
    "age",
    "gender",
    "income_level",
    "frequent_flyer_status",
    "satisfaction_segment",
    "total_flights",
    "total_no_shows",
    "no_show_rate",
    "avg_satisfaction",
    "avg_price_paid",
    "total_delay_minutes",
    "preferred_class",
    "preferred_checkin",
    "primary_travel_purpose",
    "preferred_airline",
]]

print(f"  → {len(passengers_df):,} unique passengers")
print(f"  Satisfaction segments:")
print(passengers_df["satisfaction_segment"].value_counts().to_string())
print(f"  Frequent flyer status:")
print(passengers_df["frequent_flyer_status"].value_counts().to_string())

# ══════════════════════════════════════════════════════════
#  TABLE 2 — routes
#  One row per Departure + Arrival + Airline combo
#  Performance KPIs derived from flight transactions
# ══════════════════════════════════════════════════════════

print("\nBuilding routes table …")

routes_df = (
    df.groupby(["Departure_Airport", "Arrival_Airport", "Airline"])
    .agg(
        total_flights         = ("Flight_ID",                "count"),
        avg_satisfaction      = ("Flight_Satisfaction_Score","mean"),
        avg_delay_minutes     = ("Delay_Minutes",            "mean"),
        no_show_rate          = ("No_Show",                  "mean"),
        pct_cancelled         = ("Flight_Status", lambda x: (x=="Cancelled").mean()),
        pct_delayed           = ("Flight_Status", lambda x: (x=="Delayed").mean()),
        pct_on_time           = ("Flight_Status", lambda x: (x=="On-time").mean()),
        avg_price_usd         = ("Price_USD",                "mean"),
        avg_duration_minutes  = ("Flight_Duration_Minutes",  "mean"),
        avg_distance_miles    = ("Distance_Miles",           "mean"),
        weather_impact_rate   = ("Weather_Impact",           "mean"),
    )
    .reset_index()
    .round(3)
)

# Route performance tier
def route_tier(row):
    if row["avg_satisfaction"] >= 7.2 and row["pct_on_time"] >= 0.85:
        return "High Performing"
    elif row["avg_satisfaction"] >= 6.5 and row["pct_on_time"] >= 0.75:
        return "Average"
    else:
        return "Needs Improvement"

routes_df["performance_tier"] = routes_df.apply(route_tier, axis=1)

# Create route_id
routes_df["route_id"] = (
    routes_df["Departure_Airport"] + "-" +
    routes_df["Arrival_Airport"]   + "-" +
    routes_df["Airline"].str.replace(" ", "_")
)

# Final column order
routes_df = routes_df[[
    "route_id",
    "Departure_Airport",
    "Arrival_Airport",
    "Airline",
    "total_flights",
    "avg_satisfaction",
    "avg_delay_minutes",
    "no_show_rate",
    "pct_cancelled",
    "pct_delayed",
    "pct_on_time",
    "avg_price_usd",
    "avg_duration_minutes",
    "avg_distance_miles",
    "weather_impact_rate",
    "performance_tier",
]]

print(f"  → {len(routes_df):,} unique routes")
print(f"  Performance tiers:")
print(routes_df["performance_tier"].value_counts().to_string())

# ══════════════════════════════════════════════════════════
#  WRITE TO SQLITE DB
# ══════════════════════════════════════════════════════════

print(f"\nWriting to SQLite DB: {DB_PATH} …")

conn = sqlite3.connect(DB_PATH)

passengers_df.to_sql("passengers", conn, if_exists="replace", index=False)
routes_df.to_sql("routes",         conn, if_exists="replace", index=False)

# Indexes for fast joins
conn.execute("CREATE INDEX IF NOT EXISTS idx_pass_id       ON passengers(Passenger_ID);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_pass_tier     ON passengers(frequent_flyer_status);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_pass_seg      ON passengers(satisfaction_segment);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_route_id      ON routes(route_id);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_route_airports ON routes(Departure_Airport, Arrival_Airport);")
conn.commit()

# ── verify ────────────────────────────────────────────────
print("\nVerification:")
print("  passengers rows:", pd.read_sql("SELECT COUNT(*) AS n FROM passengers", conn).iloc[0,0])
print("  routes rows    :", pd.read_sql("SELECT COUNT(*) AS n FROM routes",     conn).iloc[0,0])

print("\nSample passengers (one per satisfaction segment):")
sample = pd.read_sql("""
    SELECT Passenger_ID, age, gender, frequent_flyer_status,
           satisfaction_segment, avg_satisfaction,
           total_flights, preferred_class, no_show_rate
    FROM passengers
    GROUP BY satisfaction_segment
    ORDER BY avg_satisfaction DESC
    LIMIT 4
""", conn)
print(sample.to_string(index=False))

print("\nSample routes (top 5 by satisfaction):")
print(pd.read_sql("""
    SELECT route_id, avg_satisfaction, avg_delay_minutes,
           no_show_rate, pct_on_time, performance_tier
    FROM routes
    ORDER BY avg_satisfaction DESC
    LIMIT 5
""", conn).to_string(index=False))

conn.close()

# ── export CSVs ───────────────────────────────────────────
passengers_df.to_csv("/home/claude/flight_passengers.csv", index=False)
routes_df.to_csv("/home/claude/flight_routes.csv",         index=False)

print("\n✅ Done!")
print(f"   DB  → {DB_PATH}")
print(f"   CSV → /home/claude/flight_passengers.csv")
print(f"   CSV → /home/claude/flight_routes.csv")
