"""
=============================================================
Step 1 — DB Generation Script
Project: Premium Package Revenue (Objective 1)
Source:  Mojtaba, "Hotel Booking Dataset," Kaggle 2020
         https://www.kaggle.com/datasets/mojtaba142/hotel-booking/data

Generates two tables derived from real booking data:
  - guests        : one row per unique guest with loyalty tier
  - packages      : premium package catalogue for both hotel types

Output: hotel_guests.db (SQLite — drop-in replacement for PostgreSQL)
        guests.csv  / packages.csv  (for object store / inspection)
=============================================================
"""

import sqlite3
import hashlib
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# ── reproducibility ────────────────────────────────────────
random.seed(42)
np.random.seed(42)

# ── paths ──────────────────────────────────────────────────
CSV_PATH = "/mnt/user-data/uploads/hotel_booking__1_.csv"
DB_PATH  = "/home/claude/hotel_guests.db"

# ══════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════

def make_guest_id(email: str) -> str:
    """Stable, reproducible guest ID from email hash."""
    return "G-" + hashlib.md5(email.encode()).hexdigest()[:8].upper()


def assign_loyalty_tier(row) -> str:
    """
    Rule-based loyalty tier derived from real booking behaviour.

    Platinum : lifetime_spend >= 1500  OR  total_stays >= 4
    Gold     : lifetime_spend >= 700   OR  total_stays >= 3
    Silver   : lifetime_spend >= 300   OR  total_stays >= 2
    Bronze   : everyone else
    """
    spend  = row["lifetime_spend"]
    stays  = row["total_stays"]
    reqs   = row["total_special_requests"]

    if spend >= 1500 or stays >= 4:
        return "Platinum"
    elif spend >= 700 or stays >= 3:
        return "Gold"
    elif spend >= 300 or stays >= 2 or reqs >= 3:
        return "Silver"
    else:
        return "Bronze"


def random_date_before(date_str: str, max_days_before: int = 730) -> str:
    """Return a random date up to max_days_before before the given date."""
    try:
        ref = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        ref = datetime(2015, 7, 1)
    delta = timedelta(days=random.randint(30, max_days_before))
    return (ref - delta).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════
#  LOAD & PREPARE BOOKING DATA
# ══════════════════════════════════════════════════════════

print("Loading hotel booking CSV …")
df = pd.read_csv(CSV_PATH)

df["total_nights"]  = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
df["total_revenue"] = df["adr"] * df["total_nights"]

# ══════════════════════════════════════════════════════════
#  TABLE 1 — guests
# ══════════════════════════════════════════════════════════

print("Building guests table …")

guest_agg = (
    df.groupby("email")
    .agg(
        name                   = ("name",                  "first"),
        country                = ("country",               "first"),
        phone                  = ("phone-number",          "first"),
        total_stays            = ("is_canceled",           lambda x: int((x == 0).sum())),
        total_cancellations    = ("is_canceled",           "sum"),
        lifetime_spend         = ("total_revenue",         "sum"),
        avg_adr                = ("adr",                   "mean"),
        preferred_meal         = ("meal",                  lambda x: x.mode()[0] if len(x) else "BB"),
        preferred_room         = ("reserved_room_type",    lambda x: x.mode()[0] if len(x) else "A"),
        total_special_requests = ("total_of_special_requests", "sum"),
        first_booking_date     = ("reservation_status_date",   "min"),
    )
    .reset_index()
)

# Derived columns
guest_agg["guest_id"]      = guest_agg["email"].apply(make_guest_id)
guest_agg["loyalty_tier"]  = guest_agg.apply(assign_loyalty_tier, axis=1)
guest_agg["joined_date"]   = guest_agg["first_booking_date"].apply(
    lambda d: random_date_before(d, max_days_before=365)
)
guest_agg["lifetime_spend"] = guest_agg["lifetime_spend"].round(2)
guest_agg["avg_adr"]        = guest_agg["avg_adr"].round(2)

# Final column order
guests_df = guest_agg[[
    "guest_id",
    "name",
    "email",
    "phone",
    "country",
    "loyalty_tier",
    "total_stays",
    "total_cancellations",
    "lifetime_spend",
    "avg_adr",
    "preferred_meal",
    "preferred_room",
    "total_special_requests",
    "joined_date",
]]

print(f"  → {len(guests_df):,} unique guests")
print(f"  Loyalty tier distribution:")
print(guests_df["loyalty_tier"].value_counts().to_string())

# ══════════════════════════════════════════════════════════
#  TABLE 2 — packages
# ══════════════════════════════════════════════════════════

print("\nBuilding packages table …")

"""
Package catalogue — one row per hotel × meal × room_tier combo.
base_price is set above the observed ADR mean for that room/meal
to reflect the all-inclusive catalogue price.
"""

packages_raw = [
    # ── Resort Hotel ──────────────────────────────────────────────────────────
    # package_id | package_name               | meal | room | base_price | spa   | transfer | hotel        | is_premium
    ("PKG-R-BB-A",  "Resort Starter BB",       "BB",  "A",   85.0,  False, False, "Resort Hotel", False),
    ("PKG-R-BB-D",  "Resort Classic BB",       "BB",  "D",   115.0, False, False, "Resort Hotel", False),
    ("PKG-R-HB-D",  "Resort Comfort HB",       "HB",  "D",   145.0, False, False, "Resort Hotel", False),
    ("PKG-R-HB-F",  "Resort Premium HB",       "HB",  "F",   185.0, True,  False, "Resort Hotel", True),
    ("PKG-R-HB-G",  "Resort Deluxe HB",        "HB",  "G",   210.0, True,  False, "Resort Hotel", True),
    ("PKG-R-FB-F",  "Resort Premium FB",       "FB",  "F",   230.0, True,  True,  "Resort Hotel", True),
    ("PKG-R-FB-G",  "Resort Deluxe FB",        "FB",  "G",   260.0, True,  True,  "Resort Hotel", True),
    ("PKG-R-FB-H",  "Resort Signature FB",     "FB",  "H",   310.0, True,  True,  "Resort Hotel", True),

    # ── City Hotel ────────────────────────────────────────────────────────────
    ("PKG-C-BB-A",  "City Starter BB",         "BB",  "A",   90.0,  False, False, "City Hotel",   False),
    ("PKG-C-BB-D",  "City Classic BB",         "BB",  "D",   120.0, False, False, "City Hotel",   False),
    ("PKG-C-HB-D",  "City Comfort HB",         "HB",  "D",   155.0, False, False, "City Hotel",   False),
    ("PKG-C-HB-E",  "City Business HB",        "HB",  "E",   170.0, False, True,  "City Hotel",   False),
    ("PKG-C-HB-F",  "City Premium HB",         "HB",  "F",   200.0, True,  False, "City Hotel",   True),
    ("PKG-C-HB-G",  "City Deluxe HB",          "HB",  "G",   225.0, True,  False, "City Hotel",   True),
    ("PKG-C-FB-F",  "City Premium FB",         "FB",  "F",   245.0, True,  True,  "City Hotel",   True),
    ("PKG-C-FB-G",  "City Deluxe FB",          "FB",  "G",   275.0, True,  True,  "City Hotel",   True),
    ("PKG-C-FB-H",  "City Signature FB",       "FB",  "H",   320.0, True,  True,  "City Hotel",   True),
]

packages_df = pd.DataFrame(packages_raw, columns=[
    "package_id",
    "package_name",
    "meal_type",
    "room_type",
    "base_price",
    "includes_spa",
    "includes_transfer",
    "hotel_type",
    "is_premium",
])

print(f"  → {len(packages_df)} packages defined")
print(f"  Premium packages: {packages_df['is_premium'].sum()}")
print(f"  Standard packages: {(~packages_df['is_premium']).sum()}")

# ══════════════════════════════════════════════════════════
#  WRITE TO SQLITE DB
# ══════════════════════════════════════════════════════════

print(f"\nWriting to SQLite DB: {DB_PATH} …")

conn = sqlite3.connect(DB_PATH)

guests_df.to_sql("guests", conn, if_exists="replace", index=False)
packages_df.to_sql("packages", conn, if_exists="replace", index=False)

# ── add indexes for fast joins ──────────────────────────
conn.execute("CREATE INDEX IF NOT EXISTS idx_guests_email ON guests(email);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_guests_tier  ON guests(loyalty_tier);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_pkg_meal_room ON packages(meal_type, room_type, hotel_type);")
conn.commit()

# ── verify ─────────────────────────────────────────────
print("\nVerification:")
print("  guests rows   :", pd.read_sql("SELECT COUNT(*) AS n FROM guests",   conn).iloc[0,0])
print("  packages rows :", pd.read_sql("SELECT COUNT(*) AS n FROM packages", conn).iloc[0,0])

print("\nSample guests (one per tier):")
sample = pd.read_sql("""
    SELECT guest_id, name, country, loyalty_tier, total_stays,
           lifetime_spend, preferred_meal, preferred_room
    FROM guests
    GROUP BY loyalty_tier
    ORDER BY lifetime_spend DESC
    LIMIT 4
""", conn)
print(sample.to_string(index=False))

print("\nPackages catalogue:")
print(pd.read_sql("SELECT * FROM packages ORDER BY hotel_type, base_price", conn).to_string(index=False))

conn.close()

# ── also export CSVs for inspection / object store ─────
guests_df.to_csv("/home/claude/guests.csv", index=False)
packages_df.to_csv("/home/claude/packages.csv", index=False)

print("\n✅ Done!")
print(f"   DB  → {DB_PATH}")
print(f"   CSV → /home/claude/guests.csv")
print(f"   CSV → /home/claude/packages.csv")
