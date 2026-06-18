

"""
test_data_quality.py
─────────────────────
Data quality checks for the hotel booking pipeline.
Run: pytest tests/ -v

Tests cover:
  - Schema validation
  - Null checks on critical fields
  - Business logic assertions
  - Downstream update propagation (simulated)
"""

import os

import pytest  # type: ignore[import]  # pylint: disable=import-error

pd = pytest.importorskip("pandas")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


# ── Fixtures 
@pytest.fixture(scope="module")
def bookings_df():
    path = os.path.join(DATA_DIR, "hotel_bookings.csv")
    if not os.path.exists(path):
        pytest.skip("hotel_bookings.csv not present — download from Kaggle first")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


@pytest.fixture(scope="module")
def profiles_df():
    path = os.path.join(DATA_DIR, "fake_guest_profiles.csv")
    if not os.path.exists(path):
        pytest.skip("fake_guest_profiles.csv not present — run generate_fake_data.py")
    return pd.read_csv(path)


@pytest.fixture(scope="module")
def catalog_df():
    path = os.path.join(DATA_DIR, "fake_package_catalog.csv")
    if not os.path.exists(path):
        pytest.skip("fake_package_catalog.csv not present — run generate_fake_data.py")
    return pd.read_csv(path)


@pytest.fixture(scope="module")
def feedback_df():
    path = os.path.join(DATA_DIR, "fake_feedback_scores.csv")
    if not os.path.exists(path):
        pytest.skip("fake_feedback_scores.csv not present — run generate_fake_data.py")
    return pd.read_csv(path)


# ── hotel_bookings.csv 
class TestBookingsSchema:
    REQUIRED_COLS = [
        "hotel", "is_canceled", "lead_time", "arrival_date_year",
        "arrival_date_month", "arrival_date_day_of_month",
        "stays_in_weekend_nights", "stays_in_week_nights",
        "adults", "meal", "country", "market_segment",
        "distribution_channel", "adr", "reservation_status"
    ]

    def test_required_columns_present(self, bookings_df):
        missing = [c for c in self.REQUIRED_COLS if c not in bookings_df.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_row_count_reasonable(self, bookings_df):
        assert len(bookings_df) > 100_000, "Expected ~119K rows in Kaggle dataset"

    def test_hotel_values(self, bookings_df):
        valid = {"City Hotel", "Resort Hotel"}
        actual = set(bookings_df["hotel"].unique())
        assert actual.issubset(valid), f"Unexpected hotel values: {actual - valid}"

    def test_is_canceled_binary(self, bookings_df):
        vals = set(bookings_df["is_canceled"].astype(str).unique())
        assert vals.issubset({"0", "1"}), f"Non-binary cancellation values: {vals}"

    def test_adr_no_negatives(self, bookings_df):
        adr = pd.to_numeric(bookings_df["adr"], errors="coerce")
        neg_count = (adr < 0).sum()
        assert neg_count == 0, f"Found {neg_count} negative ADR values"

    def test_arrival_year_range(self, bookings_df):
        years = pd.to_numeric(bookings_df["arrival_date_year"], errors="coerce")
        assert years.min() >= 2015, "Arrival years too early"
        assert years.max() <= 2020, "Arrival years unexpectedly far in future"

    def test_meal_values(self, bookings_df):
        valid_meals = {"BB", "HB", "FB", "SC", "Undefined", ""}
        actual = set(bookings_df["meal"].fillna("").unique())
        unexpected = actual - valid_meals
        assert not unexpected, f"Unexpected meal codes: {unexpected}"

    def test_no_duplicate_rows(self, bookings_df):
        dupes = bookings_df.duplicated().sum()
        # Small number of dupes acceptable in real dataset
        assert dupes < len(bookings_df) * 0.01, f"Too many duplicate rows: {dupes}"


# ── fake_guest_profiles.csv

class TestGuestProfiles:
    def test_row_count(self, profiles_df):
        assert len(profiles_df) == 5000

    def test_guest_id_unique(self, profiles_df):
        assert profiles_df["guest_id"].is_unique, "Guest IDs must be unique"

    def test_loyalty_tiers(self, profiles_df):
        valid = {"Bronze", "Silver", "Gold", "Platinum"}
        actual = set(profiles_df["loyalty_tier"].unique())
        assert actual.issubset(valid)

    def test_total_spend_positive(self, profiles_df):
        assert (profiles_df["total_spend_eur"] > 0).all()

    def test_platinum_rarity(self, profiles_df):
        platinum_pct = (profiles_df["loyalty_tier"] == "Platinum").mean()
        assert platinum_pct < 0.10, "Platinum guests should be <10% of base"

    def test_email_opt_in_values(self, profiles_df):
        assert set(profiles_df["email_opt_in"].unique()).issubset({"Y", "N"})


# ── fake_package_catalog.csv 

class TestPackageCatalog:
    def test_row_count(self, catalog_df):
        assert len(catalog_df) == 8, "Expected 8 package definitions"

    def test_pkg_id_unique(self, catalog_df):
        assert catalog_df["pkg_id"].is_unique

    def test_margin_range(self, catalog_df):
        assert (catalog_df["margin_pct"] >= 0.0).all()
        assert (catalog_df["margin_pct"] <= 1.0).all()

    def test_premium_has_highest_margin(self, catalog_df):
        premium_margin = catalog_df.loc[
            catalog_df["package_name"] == "Premium All-Inclusive", "margin_pct"
        ].values[0]
        std_margin = catalog_df.loc[
            catalog_df["package_name"] == "Standard Room", "margin_pct"
        ].values[0]
        assert premium_margin > std_margin, "Premium should have higher margin than Standard"


# ── fake_feedback_scores.csv ───────────────────────────────────────────────────

class TestFeedbackScores:
    def test_row_count(self, feedback_df):
        assert len(feedback_df) == 8000

    def test_nps_range(self, feedback_df):
        assert feedback_df["nps_score"].between(0, 10).all(), "NPS must be 0–10"

    def test_satisfaction_range(self, feedback_df):
        for col in ["checkin_satisfaction", "room_satisfaction",
                    "food_satisfaction", "value_for_money"]:
            assert feedback_df[col].between(1, 5).all(), f"{col} must be 1–5"

    def test_premium_nps_higher_than_standard(self, feedback_df):
        premium_nps = feedback_df[
            feedback_df["package_name"] == "Premium All-Inclusive"
        ]["nps_score"].mean()
        standard_nps = feedback_df[
            feedback_df["package_name"].str.contains("Standard", na=False)
        ]["nps_score"].mean()
        assert premium_nps > standard_nps, "Premium packages should have higher NPS"

    def test_hotel_type_values(self, feedback_df):
        valid = {"City Hotel", "Resort Hotel"}
        assert set(feedback_df["hotel_type"].unique()).issubset(valid)