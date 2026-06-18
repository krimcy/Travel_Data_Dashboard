"""
spark_transform.py
──────────────────
PySpark transformations: RAW → CURATED layer.
Compatible with Databricks and local PySpark.

Pipeline:
  1. Read raw hotel_bookings from Snowflake RAW schema
  2. Cast + clean columns
  3. Add derived fields (total_nights, is_premium_meal, revenue_bucket, etc.)
  4. Join with guest profiles on country + segment
  5. Write to Snowflake CURATED schema
  6. Process feedback scores (NPS categorization)

Run on Databricks:
  - Upload this file to DBFS or a Databricks Repo
  - Create a job with this as the entry point
  - Set Snowflake connector JAR in cluster libraries

Run locally:
  pip install pyspark snowflake-connector-python
  python etl/spark_transform.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, FloatType, BooleanType, DateType, StringType
)

# ── Config ─────────────────────────────────────────────────────────────────────

SNOWFLAKE_OPTIONS = {
    "sfURL":       os.getenv("SNOWFLAKE_ACCOUNT", "your_account.snowflakecomputing.com"),
    "sfUser":      os.getenv("SNOWFLAKE_USER",     "your_user"),
    "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD", "your_password"),
    "sfDatabase":  "HOTEL_DB",
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "HOTEL_WH"),
    "sfRole":      "SYSADMIN",
}

MONTH_MAP = {
    "January": 1,  "February": 2,  "March": 3,    "April": 4,
    "May": 5,      "June": 6,      "July": 7,     "August": 8,
    "September": 9,"October": 10,  "November": 11,"December": 12
}

PREMIUM_MEALS = {"HB", "FB"}  # Half Board, Full Board


# ── Spark Session ───────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HotelBookingETL")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ── Read from Snowflake ─────────────────────────────────────────────────────────

def read_snowflake(spark: SparkSession, schema: str, table: str):
    opts = {**SNOWFLAKE_OPTIONS, "sfSchema": schema, "dbtable": table}
    return spark.read.format("snowflake").options(**opts).load()


def write_snowflake(df, schema: str, table: str, mode: str = "overwrite"):
    opts = {**SNOWFLAKE_OPTIONS, "sfSchema": schema, "dbtable": table}
    df.write.format("snowflake").options(**opts).mode(mode).save()


# ── Transform: BOOKINGS ────────────────────────────────────────────────────────

def transform_bookings(spark: SparkSession):
    print("📥 Reading RAW.HOTEL_BOOKINGS ...")
    raw = read_snowflake(spark, "RAW", "HOTEL_BOOKINGS")

    # Month name → number UDF
    month_udf = F.udf(lambda m: MONTH_MAP.get(m, 1), IntegerType())

    df = (
        raw
        # Cast numeric columns
        .withColumn("is_canceled",     F.col("is_canceled").cast(IntegerType()).cast(BooleanType()))
        .withColumn("lead_time",       F.col("lead_time").cast(IntegerType()))
        .withColumn("adults",          F.col("adults").cast(IntegerType()))
        .withColumn("children",        F.coalesce(F.col("children").cast(IntegerType()), F.lit(0)))
        .withColumn("babies",          F.col("babies").cast(IntegerType()))
        .withColumn("adr",             F.col("adr").cast(FloatType()))
        .withColumn("stays_in_weekend_nights", F.col("stays_in_weekend_nights").cast(IntegerType()))
        .withColumn("stays_in_week_nights",    F.col("stays_in_week_nights").cast(IntegerType()))
        .withColumn("booking_changes", F.col("booking_changes").cast(IntegerType()))
        .withColumn("days_in_waiting_list", F.col("days_in_waiting_list").cast(IntegerType()))
        .withColumn("previous_cancellations", F.col("previous_cancellations").cast(IntegerType()))
        .withColumn("is_repeated_guest",  F.col("is_repeated_guest").cast(IntegerType()).cast(BooleanType()))
        .withColumn("total_of_special_requests", F.col("total_of_special_requests").cast(IntegerType()))

        # Build arrival_date from parts
        .withColumn("month_num",  month_udf(F.col("arrival_date_month")))
        .withColumn("arrival_date", F.to_date(
            F.concat_ws("-",
                F.col("arrival_date_year"),
                F.lpad(F.col("month_num").cast(StringType()), 2, "0"),
                F.lpad(F.col("arrival_date_day_of_month"), 2, "0")
            ), "yyyy-MM-dd"
        ))

        # Derived fields
        .withColumn("total_nights",
            F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights"))
        .withColumn("room_upgrade",
            F.col("reserved_room_type") != F.col("assigned_room_type"))
        .withColumn("is_premium_meal",
            F.col("meal").isin(list(PREMIUM_MEALS)))
        .withColumn("revenue_bucket", F.when(F.col("adr") < 80,  "Low")
                                       .when(F.col("adr") < 150, "Mid")
                                       .otherwise("Premium"))

        # Surrogate key
        .withColumn("booking_id", F.concat(F.lit("BK"), F.monotonically_increasing_id().cast(StringType())))

        # Rename special requests
        .withColumnRenamed("total_of_special_requests", "special_requests")

        # Drop unnecessary raw columns
        .drop("arrival_date_year", "arrival_date_month", "arrival_date_week_number",
              "arrival_date_day_of_month", "month_num", "_ingested_at",
              "agent", "company", "required_car_parking_spaces")

        # Select final columns in order
        .select(
            "booking_id", "hotel", "is_canceled", "lead_time", "arrival_date",
            "total_nights", "adults", "children", "babies",
            F.col("meal").alias("meal_plan"),
            "country", "market_segment", "distribution_channel",
            "is_repeated_guest", "previous_cancellations",
            "reserved_room_type", "assigned_room_type", "room_upgrade",
            "booking_changes", "deposit_type", "days_in_waiting_list",
            "customer_type", "adr", "special_requests",
            "reservation_status", F.col("reservation_status_date").cast(DateType()),
            "is_premium_meal", "revenue_bucket"
        )

        # Drop nulls in critical columns
        .dropna(subset=["adr", "arrival_date"])

        # Filter out clearly bad rows
        .filter(F.col("adr") >= 0)
        .filter(F.col("total_nights") > 0)
    )

    row_count = df.count()
    print(f"✔ Transformed {row_count:,} booking rows")

    print("📤 Writing to CURATED.BOOKINGS ...")
    write_snowflake(df, "CURATED", "BOOKINGS")
    print("✅ CURATED.BOOKINGS updated")
    return df


# ── Transform: FEEDBACK ────────────────────────────────────────────────────────

def transform_feedback(spark: SparkSession):
    print("📥 Reading RAW.FEEDBACK_SCORES ...")
    raw = read_snowflake(spark, "RAW", "FEEDBACK_SCORES")

    df = (
        raw
        .withColumn("nps_score",            F.col("nps_score").cast(IntegerType()))
        .withColumn("checkin_satisfaction", F.col("checkin_satisfaction").cast(IntegerType()))
        .withColumn("room_satisfaction",    F.col("room_satisfaction").cast(IntegerType()))
        .withColumn("food_satisfaction",    F.col("food_satisfaction").cast(IntegerType()))
        .withColumn("value_for_money",      F.col("value_for_money").cast(IntegerType()))
        .withColumn("stay_date",            F.to_date(F.col("stay_date")))

        # NPS category
        .withColumn("nps_category",
            F.when(F.col("nps_score") >= 9, "Promoter")
            .when(F.col("nps_score") >= 7, "Passive")
            .otherwise("Detractor")
        )

        # Average satisfaction score
        .withColumn("avg_satisfaction", F.round(
            (F.col("checkin_satisfaction") + F.col("room_satisfaction") +
             F.col("food_satisfaction")    + F.col("value_for_money")) / 4.0,
            2
        ))

        .drop("checkin_satisfaction", "room_satisfaction",
              "food_satisfaction", "value_for_money", "_ingested_at")
    )

    print(f"✔ Transformed {df.count():,} feedback rows")
    print("📤 Writing to CURATED.FEEDBACK ...")
    write_snowflake(df, "CURATED", "FEEDBACK")
    print("✅ CURATED.FEEDBACK updated")
    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Hotel Booking Spark ETL  |  RAW → CURATED")
    print("=" * 60)
    spark = get_spark()

    try:
        transform_bookings(spark)
        transform_feedback(spark)
        print("\n🎉 ETL complete. CURATED layer ready for Analytics queries.")
    except Exception as e:
        print(f"\n❌ ETL failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
