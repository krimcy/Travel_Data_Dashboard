-- ============================================================
-- 04_bo2_satisfaction.sql
-- Business Objective 2: How can we improve traveler satisfaction
--                        and reduce booking friction?
-- ============================================================

USE DATABASE HOTEL_DB;
USE WAREHOUSE HOTEL_WH;

-- ── STEP 1: Populate Analytics Table ──────────────────────────────────────────

INSERT INTO ANALYTICS.BO2_SATISFACTION
WITH nps_agg AS (
    SELECT
        hotel_type,
        'All'                                           AS market_segment,
        ROUND(AVG(nps_score), 2)                       AS avg_nps,
        ROUND(AVG(CASE WHEN nps_category = 'Promoter'  THEN 1.0 ELSE 0.0 END) * 100, 1) AS promoter_pct,
        ROUND(AVG(CASE WHEN nps_category = 'Detractor' THEN 1.0 ELSE 0.0 END) * 100, 1) AS detractor_pct,
        ROUND(AVG(avg_satisfaction), 2)                AS avg_sat
    FROM CURATED.FEEDBACK
    GROUP BY 1,2
)
SELECT
    CURRENT_DATE()                                              AS snapshot_date,
    b.hotel,
    b.market_segment,
    ROUND(AVG(b.lead_time), 1)                                AS avg_lead_time,
    ROUND(AVG(CASE WHEN b.is_canceled THEN 1.0 ELSE 0.0 END), 4) AS cancellation_rate,
    ROUND(AVG(b.special_requests), 2)                         AS avg_special_requests,
    ROUND(AVG(CASE WHEN b.is_repeated_guest THEN 1.0 ELSE 0.0 END), 4) AS repeat_guest_rate,
    n.avg_nps                                                 AS avg_nps_score,
    n.promoter_pct,
    n.detractor_pct,
    n.avg_sat                                                 AS avg_satisfaction
FROM CURATED.BOOKINGS b
LEFT JOIN nps_agg n ON b.hotel = n.hotel_type AND n.market_segment = 'All'
GROUP BY 1,2,3, n.avg_nps, n.promoter_pct, n.detractor_pct, n.avg_sat;


-- ── QUERY 1: Cancellation Rate by Lead Time Bucket ─────────────────────────────
-- Business Question: At what lead time does cancellation spike?

SELECT
    hotel,
    CASE
        WHEN lead_time BETWEEN 0  AND 7   THEN '0–7 days'
        WHEN lead_time BETWEEN 8  AND 30  THEN '8–30 days'
        WHEN lead_time BETWEEN 31 AND 90  THEN '31–90 days'
        WHEN lead_time BETWEEN 91 AND 180 THEN '91–180 days'
        ELSE '180+ days'
    END                                             AS lead_time_bucket,
    COUNT(*)                                        AS bookings,
    SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END)   AS canceled,
    ROUND(
        100.0 * SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                               AS cancellation_pct,
    ROUND(AVG(adr), 2)                             AS avg_adr
FROM CURATED.BOOKINGS
GROUP BY 1,2
ORDER BY hotel,
    CASE lead_time_bucket
        WHEN '0–7 days'    THEN 1 WHEN '8–30 days'   THEN 2
        WHEN '31–90 days'  THEN 3 WHEN '91–180 days' THEN 4
        ELSE 5
    END;


-- ── QUERY 2: Special Requests as Friction Indicator ────────────────────────────
-- Business Question: Do more special requests predict cancellation?

SELECT
    hotel,
    total_of_special_requests,
    COUNT(*)                                                    AS bookings,
    ROUND(
        100.0 * SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                                           AS cancel_pct,
    ROUND(AVG(adr), 2)                                        AS avg_adr
FROM CURATED.BOOKINGS
GROUP BY 1,2
ORDER BY hotel, total_of_special_requests;


-- ── QUERY 3: Deposit Type Effect on Cancellation & Satisfaction ────────────────

SELECT
    hotel,
    deposit_type,
    COUNT(*)                                                    AS bookings,
    ROUND(
        100.0 * SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                                           AS cancel_pct,
    ROUND(AVG(lead_time), 1)                                   AS avg_lead_time,
    ROUND(AVG(adr), 2)                                        AS avg_adr,
    ROUND(AVG(special_requests), 2)                           AS avg_special_requests
FROM CURATED.BOOKINGS
GROUP BY 1,2
ORDER BY hotel, cancel_pct DESC;


-- ── QUERY 4: NPS Breakdown by Package & Channel ────────────────────────────────

SELECT
    hotel_type,
    package_name,
    booking_channel,
    COUNT(*)                                                    AS responses,
    ROUND(AVG(nps_score), 1)                                  AS avg_nps,
    ROUND(
        100.0 * SUM(CASE WHEN nps_category = 'Promoter'  THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                                           AS promoter_pct,
    ROUND(
        100.0 * SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                                           AS detractor_pct,
    ROUND(AVG(avg_satisfaction), 2)                           AS avg_satisfaction
FROM CURATED.FEEDBACK
GROUP BY 1,2,3
ORDER BY hotel_type, avg_nps DESC;


-- ── QUERY 5: Repeat Guest Behavior ─────────────────────────────────────────────
-- Business Question: What do repeat guests have in common?

SELECT
    hotel,
    is_repeated_guest,
    ROUND(AVG(lead_time), 1)                    AS avg_lead_time,
    ROUND(AVG(adr), 2)                         AS avg_adr,
    ROUND(AVG(special_requests), 2)            AS avg_special_requests,
    ROUND(AVG(booking_changes), 2)             AS avg_changes,
    ROUND(AVG(CASE WHEN is_canceled THEN 1.0 ELSE 0.0 END) * 100, 1) AS cancel_pct,
    COUNT(*)                                    AS bookings
FROM CURATED.BOOKINGS
GROUP BY 1,2
ORDER BY hotel, is_repeated_guest;


-- ── QUERY 6: Waiting List = Hidden Friction ────────────────────────────────────

SELECT
    hotel,
    CASE
        WHEN days_in_waiting_list = 0 THEN 'No wait'
        WHEN days_in_waiting_list BETWEEN 1 AND 7 THEN '1–7 days wait'
        ELSE '8+ days wait'
    END                                                     AS wait_bucket,
    COUNT(*)                                                AS bookings,
    ROUND(AVG(adr), 2)                                    AS avg_adr,
    ROUND(
        100.0 * SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                                       AS cancel_pct
FROM CURATED.BOOKINGS
GROUP BY 1,2
ORDER BY hotel, wait_bucket;


-- ── STEP 2: Export BO2 Results as JSON ────────────────────────────────────────

INSERT INTO ANALYTICS.JSON_EXPORT (export_type, payload)
SELECT
    'BO2',
    OBJECT_CONSTRUCT(
        'generated_at',         TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS'),
        'hotel',                hotel,
        'market_segment',       market_segment,
        'avg_lead_time',        avg_lead_time,
        'cancellation_rate',    cancellation_rate,
        'avg_special_requests', avg_special_requests,
        'repeat_guest_rate',    repeat_guest_rate,
        'avg_nps_score',        avg_nps_score,
        'promoter_pct',         promoter_pct,
        'detractor_pct',        detractor_pct,
        'avg_satisfaction',     avg_satisfaction
    )
FROM ANALYTICS.BO2_SATISFACTION;

-- Verify
SELECT COUNT(*) AS bo2_rows FROM ANALYTICS.BO2_SATISFACTION;
