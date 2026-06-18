-- ============================================================
-- 03_bo1_premium_revenue.sql
-- Business Objective 1: How can we increase premium package revenue?
-- Snowflake Analytics Layer Population + Key Queries
-- ============================================================

USE DATABASE HOTEL_DB;
USE WAREHOUSE HOTEL_WH;

-- ── STEP 1: Populate Analytics Table ──────────────────────────────────────────

INSERT INTO ANALYTICS.BO1_PREMIUM_REVENUE
SELECT
    CURRENT_DATE()                                              AS snapshot_date,
    hotel,
    market_segment,
    meal_plan,
    revenue_bucket,
    COUNT(*)                                                    AS total_bookings,
    SUM(adr)                                                    AS total_adr,
    ROUND(AVG(adr), 2)                                         AS avg_adr,
    ROUND(AVG(CASE WHEN is_canceled THEN 1.0 ELSE 0.0 END), 4) AS cancellation_rate,
    ROUND(AVG(CASE WHEN is_premium_meal THEN 1.0 ELSE 0.0 END), 4) AS premium_conversion_rate
FROM CURATED.BOOKINGS
WHERE NOT is_canceled   -- completed stays only
GROUP BY 1,2,3,4,5;


-- ── QUERY 1: Premium Revenue Share by Meal Plan ────────────────────────────────
-- Business Question: Which meal plan drives the most revenue?

SELECT
    meal_plan,
    hotel,
    COUNT(*)                            AS bookings,
    ROUND(AVG(adr), 2)                 AS avg_daily_rate,
    ROUND(SUM(adr), 0)                 AS total_revenue,
    ROUND(
        100.0 * SUM(adr) / SUM(SUM(adr)) OVER (PARTITION BY hotel),
        1
    )                                   AS pct_of_hotel_revenue
FROM CURATED.BOOKINGS
WHERE NOT is_canceled
GROUP BY 1,2
ORDER BY hotel, total_revenue DESC;


-- ── QUERY 2: Market Segment Premium Conversion Funnel ──────────────────────────
-- Business Question: Which segments are most likely to upgrade to premium?

WITH segment_stats AS (
    SELECT
        market_segment,
        COUNT(*)                                            AS total_bookings,
        SUM(CASE WHEN is_premium_meal THEN 1 ELSE 0 END)  AS premium_bookings,
        ROUND(AVG(adr), 2)                                 AS avg_adr,
        ROUND(AVG(CASE WHEN is_canceled THEN 1.0 ELSE 0.0 END), 4) AS cancel_rate
    FROM CURATED.BOOKINGS
    GROUP BY 1
)
SELECT
    market_segment,
    total_bookings,
    premium_bookings,
    ROUND(100.0 * premium_bookings / NULLIF(total_bookings, 0), 1) AS premium_pct,
    avg_adr,
    cancel_rate,
    RANK() OVER (ORDER BY premium_pct DESC)                         AS premium_rank
FROM segment_stats
ORDER BY premium_pct DESC;


-- ── QUERY 3: Seasonal Premium Revenue Opportunity ──────────────────────────────
-- Business Question: When are guests most willing to pay for premium?

SELECT
    arrival_date_year,
    CASE arrival_date_month
        WHEN 'January'   THEN 1  WHEN 'February'  THEN 2
        WHEN 'March'     THEN 3  WHEN 'April'     THEN 4
        WHEN 'May'       THEN 5  WHEN 'June'      THEN 6
        WHEN 'July'      THEN 7  WHEN 'August'    THEN 8
        WHEN 'September' THEN 9  WHEN 'October'   THEN 10
        WHEN 'November'  THEN 11 WHEN 'December'  THEN 12
    END                                                 AS month_num,
    arrival_date_month,
    hotel,
    COUNT(*)                                            AS bookings,
    ROUND(AVG(adr), 2)                                 AS avg_adr,
    ROUND(AVG(CASE WHEN is_premium_meal THEN 1.0 ELSE 0.0 END) * 100, 1) AS premium_pct
FROM CURATED.BOOKINGS
    JOIN HOTEL_DB.RAW.HOTEL_BOOKINGS
        ON CURATED.BOOKINGS.booking_id = 'raw'   -- adjust join key to your ETL
WHERE NOT is_canceled
GROUP BY 1,2,3,4
ORDER BY arrival_date_year, month_num, hotel;


-- ── QUERY 4: Room Upgrade Effect on Revenue ────────────────────────────────────
-- Business Question: Do room upgrades correlate with higher ADR or satisfaction?

SELECT
    hotel,
    room_upgrade,
    COUNT(*)            AS bookings,
    ROUND(AVG(adr), 2) AS avg_adr,
    ROUND(AVG(special_requests), 2) AS avg_special_requests,
    ROUND(AVG(CASE WHEN is_canceled THEN 1.0 ELSE 0.0 END) * 100, 1) AS cancel_pct
FROM CURATED.BOOKINGS
GROUP BY 1,2
ORDER BY hotel, room_upgrade;


-- ── QUERY 5: Loyalty Tier × Premium Package Cross-Sell Opportunity ─────────────

SELECT
    gp.loyalty_tier,
    b.meal_plan,
    b.hotel,
    COUNT(DISTINCT gp.guest_id)    AS guests,
    ROUND(AVG(b.adr), 2)          AS avg_adr,
    ROUND(AVG(gp.total_spend_eur), 2) AS avg_lifetime_spend
FROM CURATED.BOOKINGS b
JOIN RAW.GUEST_PROFILES gp
    ON b.country = gp.country
    AND b.market_segment = gp.preferred_segment
WHERE NOT b.is_canceled
GROUP BY 1,2,3
ORDER BY loyalty_tier, avg_adr DESC;


-- ── STEP 2: Export BO1 Results as JSON ────────────────────────────────────────

INSERT INTO ANALYTICS.JSON_EXPORT (export_type, payload)
SELECT
    'BO1',
    OBJECT_CONSTRUCT(
        'generated_at',     TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS'),
        'hotel',            hotel,
        'market_segment',   market_segment,
        'meal_plan',        meal_plan,
        'revenue_bucket',   revenue_bucket,
        'total_bookings',   total_bookings,
        'avg_adr',          avg_adr,
        'cancellation_rate', cancellation_rate,
        'premium_conversion_rate', premium_conversion_rate
    )
FROM ANALYTICS.BO1_PREMIUM_REVENUE;

-- Verify
SELECT COUNT(*) AS bo1_rows FROM ANALYTICS.BO1_PREMIUM_REVENUE;
SELECT export_type, COUNT(*) FROM ANALYTICS.JSON_EXPORT GROUP BY 1;
