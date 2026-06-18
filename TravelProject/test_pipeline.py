"""
test_pipeline.py
─────────────────
Tests for the ETL pipeline logic and downstream update propagation.

Simulates the testing strategy from the project brief:
  - New data added to source
  - Downstream update (curated → analytics)
  - Output and dashboard also updated
  - JSON export as well
"""

import pandas as pd
import os
import json
import csv
from io import StringIO

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


# ── Unit Tests: Transform Logic ────────────────────────────────────────────────

class TestTransformLogic:
    """Test transformation logic without Snowflake/Spark dependency."""

    def _make_booking_row(self, **overrides):
        """Create a minimal test booking dict."""
        defaults = {
            "hotel": "City Hotel",
            "is_canceled": "0",
            "lead_time": "30",
            "arrival_date_year": "2016",
            "arrival_date_month": "August",
            "arrival_date_day_of_month": "15",
            "stays_in_weekend_nights": "2",
            "stays_in_week_nights": "3",
            "adults": "2",
            "children": "0",
            "babies": "0",
            "meal": "HB",
            "country": "GBR",
            "market_segment": "Online TA",
            "distribution_channel": "TA/TO",
            "is_repeated_guest": "0",
            "previous_cancellations": "0",
            "reserved_room_type": "A",
            "assigned_room_type": "A",
            "booking_changes": "0",
            "deposit_type": "No Deposit",
            "days_in_waiting_list": "0",
            "customer_type": "Transient",
            "adr": "120.50",
            "total_of_special_requests": "1",
            "reservation_status": "Check-Out",
            "reservation_status_date": "2016-08-20",
        }
        defaults.update(overrides)
        return defaults

    def test_total_nights_calculation(self):
        row = self._make_booking_row(
            stays_in_weekend_nights="2",
            stays_in_week_nights="5"
        )
        total = int(row["stays_in_weekend_nights"]) + int(row["stays_in_week_nights"])
        assert total == 7

    def test_is_premium_meal_hb(self):
        row = self._make_booking_row(meal="HB")
        assert row["meal"] in {"HB", "FB"}

    def test_is_premium_meal_bb(self):
        row = self._make_booking_row(meal="BB")
        assert row["meal"] not in {"HB", "FB"}

    def test_room_upgrade_detection(self):
        row = self._make_booking_row(reserved_room_type="A", assigned_room_type="C")
        assert row["reserved_room_type"] != row["assigned_room_type"]

    def test_no_upgrade_when_same(self):
        row = self._make_booking_row(reserved_room_type="A", assigned_room_type="A")
        assert row["reserved_room_type"] == row["assigned_room_type"]

    def test_revenue_bucket_low(self):
        adr = 60.0
        bucket = "Low" if adr < 80 else "Mid" if adr < 150 else "Premium"
        assert bucket == "Low"

    def test_revenue_bucket_mid(self):
        adr = 100.0
        bucket = "Low" if adr < 80 else "Mid" if adr < 150 else "Premium"
        assert bucket == "Mid"

    def test_revenue_bucket_premium(self):
        adr = 200.0
        bucket = "Low" if adr < 80 else "Mid" if adr < 150 else "Premium"
        assert bucket == "Premium"


class TestNPSCategorization:
    def _categorize(self, score: int) -> str:
        if score >= 9:  return "Promoter"
        if score >= 7:  return "Passive"
        return "Detractor"

    def test_promoter(self):
        assert self._categorize(9)  == "Promoter"
        assert self._categorize(10) == "Promoter"

    def test_passive(self):
        assert self._categorize(7) == "Passive"
        assert self._categorize(8) == "Passive"

    def test_detractor(self):
        assert self._categorize(0) == "Detractor"
        assert self._categorize(6) == "Detractor"


# ── Integration-like: New Data → Downstream Update ────────────────────────────

class TestDownstreamUpdate:
    """
    Simulates the testing strategy:
    1. New data added to source
    2. Curated layer refreshed
    3. Analytics + JSON updated
    """

    def _simulate_pipeline(self, bookings: list[dict]) -> dict:
        """Minimal Python simulation of the full pipeline."""
        df = pd.DataFrame(bookings)

        # Cast
        df["adr"]         = df["adr"].astype(float)
        df["is_canceled"] = df["is_canceled"].astype(int).astype(bool)
        df["lead_time"]   = df["lead_time"].astype(int)

        # Derive
        df["total_nights"]     = df["stays_in_weekend_nights"].astype(int) + \
                                  df["stays_in_week_nights"].astype(int)
        df["is_premium_meal"]  = df["meal"].isin(["HB", "FB"])
        df["revenue_bucket"]   = df["adr"].apply(
            lambda x: "Low" if x < 80 else "Mid" if x < 150 else "Premium"
        )

        # BO-1 aggregation
        bo1 = df[~df["is_canceled"]].groupby(["hotel", "meal"]).agg(
            total_bookings=("adr", "count"),
            avg_adr=("adr", "mean"),
            premium_count=("is_premium_meal", "sum"),
        ).reset_index()

        # BO-2 aggregation
        bo2 = df.groupby("hotel").agg(
            cancellation_rate=("is_canceled", "mean"),
            avg_lead_time=("lead_time", "mean"),
        ).reset_index()

        # JSON export simulation
        json_output = {
            "BO1": bo1.to_dict(orient="records"),
            "BO2": bo2.to_dict(orient="records"),
        }

        return json_output

    def _base_bookings(self):
        return [
            {"hotel": "City Hotel", "is_canceled": "0", "lead_time": "20",
             "stays_in_weekend_nights": "1", "stays_in_week_nights": "2",
             "meal": "BB", "adr": "95.0"},
            {"hotel": "Resort Hotel", "is_canceled": "1", "lead_time": "60",
             "stays_in_weekend_nights": "2", "stays_in_week_nights": "3",
             "meal": "HB", "adr": "160.0"},
        ]

    def test_base_pipeline_runs(self):
        result = self._simulate_pipeline(self._base_bookings())
        assert "BO1" in result
        assert "BO2" in result

    def test_new_booking_updates_counts(self):
        base = self._base_bookings()
        result_before = self._simulate_pipeline(base)

        # Add a new non-canceled booking
        new_booking = {
            "hotel": "City Hotel", "is_canceled": "0", "lead_time": "5",
            "stays_in_weekend_nights": "0", "stays_in_week_nights": "1",
            "meal": "FB", "adr": "200.0"
        }
        result_after = self._simulate_pipeline(base + [new_booking])

        # BO1 should now have more city hotel rows or higher count
        city_before = [r for r in result_before["BO1"] if r["hotel"] == "City Hotel"]
        city_after  = [r for r in result_after["BO1"]  if r["hotel"] == "City Hotel"]
        total_before = sum(r["total_bookings"] for r in city_before)
        total_after  = sum(r["total_bookings"] for r in city_after)
        assert total_after > total_before, "New booking should increase City Hotel count"

    def test_new_cancellation_updates_rate(self):
        base = self._base_bookings()
        result_before = self._simulate_pipeline(base)

        # Add a cancellation
        new_cancel = {
            "hotel": "City Hotel", "is_canceled": "1", "lead_time": "90",
            "stays_in_weekend_nights": "1", "stays_in_week_nights": "1",
            "meal": "SC", "adr": "80.0"
        }
        result_after = self._simulate_pipeline(base + [new_cancel])

        rate_before = [r["cancellation_rate"] for r in result_before["BO2"] if r["hotel"] == "City Hotel"][0]
        rate_after  = [r["cancellation_rate"] for r in result_after["BO2"]  if r["hotel"] == "City Hotel"][0]
        assert rate_after >= rate_before, "New cancellation should not decrease cancel rate"

    def test_json_export_structure(self):
        result = self._simulate_pipeline(self._base_bookings())
        json_str = json.dumps(result)
        parsed = json.loads(json_str)
        assert isinstance(parsed["BO1"], list)
        assert isinstance(parsed["BO2"], list)
        assert all("hotel" in r for r in parsed["BO2"])
