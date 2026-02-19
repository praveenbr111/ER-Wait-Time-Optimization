-- ============================================================
-- 04_GOLD_LAYER.SQL
-- Project : ER Wait Time Optimization
-- Source  : ER_ANALYTICS.SILVER.PATIENT_VISITS_CLEAN
-- Target  : ER_ANALYTICS.GOLD.PATIENT_VISITS_ANALYTICS
-- Purpose : Feature Engineering for Business Intelligence. 
--           Transforming timestamps into operational metrics (Wait Times)
--           and financial metrics (Revenue Leakage).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS ER_ANALYTICS.GOLD;

-- Creating the Final Analytics Table (Single Source of Truth)
CREATE OR REPLACE TABLE ER_ANALYTICS.GOLD.PATIENT_VISITS_ANALYTICS AS
SELECT
    -- 1. BASIC INFO: Core patient and staff identifiers
    visit_id,
    patient_id,
    doctor_id,
    nurse_id,
    arrival_time,
    triage_time,
    doctor_assigned_time,
    discharge_time,
    complaint_category,
    severity_level,
    age,
    insurance_status,

    -- 2. STOPWATCH CALCULATIONS (Operational KPIs)
    -- Measure clinical efficiency at every touchpoint
    DATEDIFF('minute', arrival_time, triage_time)            AS wait_to_triage_min,
    DATEDIFF('minute', triage_time, doctor_assigned_time)    AS wait_to_doctor_min,
    DATEDIFF('minute', doctor_assigned_time, discharge_time) AS treatment_duration_min,
    DATEDIFF('minute', arrival_time, discharge_time)         AS total_visit_duration_min,

    -- 3. TIME DIMENSIONS (Temporal Analysis)
    -- Extracted for Heatmap visualizations and Peak-Hour staffing models
    EXTRACT(HOUR FROM arrival_time)   AS arrival_hour,
    DAYNAME(arrival_time)             AS arrival_day_of_week,
    EXTRACT(MONTH FROM arrival_time)  AS arrival_month,
    DATE(arrival_time)                AS arrival_date,

    -- 4. VISIT SEGMENTATION (The "Leaky Bucket" Logic)
    -- Categorizing where the patient journey was interrupted (LWBS - Left Without Being Seen)
    CASE
        WHEN triage_time IS NULL THEN 'Left Before Triage'
        WHEN triage_time IS NOT NULL AND doctor_assigned_time IS NULL THEN 'Left Before Doctor'
        WHEN doctor_assigned_time IS NOT NULL AND discharge_time IS NULL THEN 'Left Before Discharge'
        ELSE 'Completed Visit'
    END AS visit_status,

    -- 5. REVENUE LEAKAGE (Financial Impact)
    -- Estimated ₹5,000 loss per patient who walks out before seeing a physician
    CASE
        WHEN triage_time IS NULL THEN 5000
        WHEN triage_time IS NOT NULL AND doctor_assigned_time IS NULL THEN 5000
        ELSE 0
    END AS revenue_lost,

    -- 6. DEMOGRAPHIC BINS
    -- Pediatric, Adult, and Senior segments for healthcare policy reporting
    CASE
        WHEN age < 18 THEN 'Pediatric'
        WHEN age BETWEEN 18 AND 59 THEN 'Adult'
        WHEN age >= 60 THEN 'Senior'
        ELSE 'Unknown'
    END AS age_group

FROM ER_ANALYTICS.SILVER.PATIENT_VISITS_CLEAN;

---
-- VALIDATION & INSIGHT QUERIES
---

-- [A] FINANCIAL SUMMARY: Total Revenue Opportunity Loss
SELECT 
    SUM(revenue_lost) AS total_revenue_lost_inr,
    ROUND(SUM(revenue_lost) / 10000000, 2) AS crores_lost
FROM ER_ANALYTICS.GOLD.PATIENT_VISITS_ANALYTICS;

-- [B] BOTTLENECK DISCOVERY: Mapping the "Drop-off" points
-- Uses Window Functions to calculate the percentage of total walkouts
SELECT 
    visit_status, 
    COUNT(*) AS patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    ROUND(AVG(wait_to_triage_min), 2) AS avg_triage_wait,
    ROUND(AVG(wait_to_doctor_min), 2) AS avg_doctor_wait
FROM ER_ANALYTICS.GOLD.PATIENT_VISITS_ANALYTICS
GROUP BY 1
ORDER BY patient_count DESC;

-- [C] SAFETY & TRIAGE AUDIT: Checking Patient Safety
-- High-severity patients should have the lowest walkout rates (Goal: 0%)
SELECT 
    severity_level,
    COUNT(*) AS total_patients,
    ROUND(AVG(wait_to_triage_min), 2) AS avg_triage_wait,
    ROUND(COUNT(CASE WHEN visit_status != 'Completed Visit' THEN 1 END) * 100.0 / COUNT(*), 2) AS walkout_pct
FROM ER_ANALYTICS.GOLD.PATIENT_VISITS_ANALYTICS
GROUP BY 1
ORDER BY 
    CASE severity_level 
        WHEN 'Critical' THEN 1 
        WHEN 'High'     THEN 2 
        WHEN 'Medium'   THEN 3 
        WHEN 'Low'      THEN 4 
    END;

-- [D] STAFFING HEATMAP DATA (For Export to Power BI/Python)
-- Full 168-hour temporal map (7 days * 24 hours)
SELECT
    arrival_day_of_week,
    arrival_hour,
    COUNT(*) AS total_visits,
    SUM(CASE WHEN visit_status IN ('Left Before Triage', 'Left Before Doctor') THEN 1 ELSE 0 END) AS lwbs_count,
    ROUND(SUM(CASE WHEN visit_status IN ('Left Before Triage', 'Left Before Doctor') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS lwbs_rate_pct
FROM ER_ANALYTICS.GOLD.PATIENT_VISITS_ANALYTICS
GROUP BY 1, 2
ORDER BY 
    CASE arrival_day_of_week
        WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2 WHEN 'Wed' THEN 3 WHEN 'Thu' THEN 4
        WHEN 'Fri' THEN 5 WHEN 'Sat' THEN 6 WHEN 'Sun' THEN 7
    END,
    arrival_hour;