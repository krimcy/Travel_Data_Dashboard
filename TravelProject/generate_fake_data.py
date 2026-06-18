"""
generate_fake_data.py
─────────────────────
Generates fake but realistic supplementary hotel data
that enriches the base Kaggle hotel_bookings.csv.

NO LLM-generated data. Pure Python faker + controlled random distributions.
Data sources: base stats derived from the Kaggle dataset's known distributions.

Outputs:
  - data/fake_guest_profiles.csv       : synthetic guest loyalty profiles
  - data/fake_package_catalog.csv      : hotel premium package definitions
  - data/fake_feedback_scores.csv      : simulated post-stay NPS & satisfaction scores
"""

import random
import csv
import os
from datetime import date, timedelta

random.seed(42)  # Reproducible

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Helpers ────────────────────────────────────────────────────────────────────

COUNTRIES = ["PRT", "GBR", "FRA", "ESP", "DEU", "ITA", "BRA", "USA", "CHN", "NLD"]
SEGMENTS  = ["Direct", "Online TA", "Offline TA/TO", "Corporate", "Groups", "Complementary"]
PACKAGES  = ["Standard", "Breakfast Included", "Half Board", "Full Board", "Premium All-Inclusive"]
CHANNELS  = ["Direct", "GDS", "TA/TO"]

def rand_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


# ── 1. Guest Loyalty Profiles ──────────────────────────────────────────────────

def generate_guest_profiles(n: int = 5000) -> None:
    path = os.path.join(OUTPUT_DIR, "fake_guest_profiles.csv")
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    tier_weights = [0.50, 0.30, 0.15, 0.05]

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "guest_id", "country", "loyalty_tier", "total_stays",
            "total_spend_eur", "avg_lead_time_days", "preferred_segment",
            "enrolled_date", "email_opt_in"
        ])
        for i in range(1, n + 1):
            tier = random.choices(tiers, weights=tier_weights)[0]
            stays = {"Bronze": random.randint(1, 3),
                     "Silver": random.randint(4, 10),
                     "Gold":   random.randint(11, 30),
                     "Platinum": random.randint(31, 100)}[tier]
            spend = round(stays * random.uniform(80, 350), 2)
            writer.writerow([
                f"G{i:06d}",
                random.choice(COUNTRIES),
                tier,
                stays,
                spend,
                random.randint(7, 120),
                random.choice(SEGMENTS),
                rand_date(date(2015, 1, 1), date(2023, 12, 31)),
                random.choice(["Y", "N"])
            ])
    print(f"✔ Generated {n} guest profiles → {path}")


# ── 2. Premium Package Catalog ─────────────────────────────────────────────────

def generate_package_catalog() -> None:
    path = os.path.join(OUTPUT_DIR, "fake_package_catalog.csv")
    rows = [
        ["pkg_id", "package_name", "base_price_eur", "margin_pct",
         "includes_breakfast", "includes_spa", "includes_transfer", "min_nights"],
        ["PKG001", "Standard Room",            0,   0.22, "N", "N", "N", 1],
        ["PKG002", "Breakfast Included",       25,  0.35, "Y", "N", "N", 1],
        ["PKG003", "Half Board",               60,  0.40, "Y", "N", "N", 2],
        ["PKG004", "Full Board",               95,  0.42, "Y", "N", "N", 2],
        ["PKG005", "Premium All-Inclusive",   180,  0.55, "Y", "Y", "Y", 3],
        ["PKG006", "Spa & Wellness",           75,  0.50, "N", "Y", "N", 2],
        ["PKG007", "Business Traveler",        45,  0.38, "Y", "N", "Y", 1],
        ["PKG008", "Romantic Getaway",        120,  0.48, "Y", "Y", "N", 2],
    ]
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"✔ Generated package catalog ({len(rows)-1} packages) → {path}")


# ── 3. Post-Stay Feedback Scores ───────────────────────────────────────────────

def generate_feedback_scores(n: int = 8000) -> None:
    path = os.path.join(OUTPUT_DIR, "fake_feedback_scores.csv")

    # NPS distribution: more promoters for premium packages
    def nps_score(pkg: str) -> int:
        if "Premium" in pkg or "Romantic" in pkg or "Spa" in pkg:
            return random.choices(range(0, 11), weights=[1,1,1,1,2,3,5,8,12,18,20])[0]
        elif pkg == "Standard Room":
            return random.choices(range(0, 11), weights=[3,3,4,5,8,10,12,14,15,13,8])[0]
        else:
            return random.choices(range(0, 11), weights=[1,2,3,4,6,8,12,15,18,15,10])[0]

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "feedback_id", "guest_id", "hotel_type", "package_name",
            "stay_date", "nps_score", "checkin_satisfaction",
            "room_satisfaction", "food_satisfaction", "value_for_money",
            "would_return", "booking_channel"
        ])
        guest_pool = [f"G{i:06d}" for i in range(1, 5001)]
        for i in range(1, n + 1):
            pkg = random.choice(PACKAGES)
            writer.writerow([
                f"FB{i:07d}",
                random.choice(guest_pool),
                random.choice(["City Hotel", "Resort Hotel"]),
                pkg,
                rand_date(date(2015, 7, 1), date(2017, 8, 31)),
                nps_score(pkg),
                random.randint(1, 5),
                random.randint(1, 5),
                random.randint(1, 5),
                random.randint(1, 5),
                random.choice(["Y", "N", "Maybe"]),
                random.choice(CHANNELS)
            ])
    print(f"✔ Generated {n} feedback records → {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    generate_guest_profiles()
    generate_package_catalog()
    generate_feedback_scores()
    print("\n✅ All fake data generated. No LLM data used.")
    print("   Data is reproducible (seed=42).")
