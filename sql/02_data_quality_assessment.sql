/********************************************************************************
  PROJECT: Emergency Department Wait Time Optimization
  LAYER: Data Quality Assessment
  DATE: Day 2
  PURPOSE: Identify data quality issues before cleaning
*********************************************************************************/

-- QUERY 1: NULL COUNT PER COLUMN
-- Purpose: Find missing data in each column to categorize as
-- either data quality issues (need fixing) or business insights (keep as NULL).

SELECT 
    COUNT(*)                      AS total_rows,
    COUNT(visit_id)               AS visit_id_count,
    COUNT(patient_id)             AS patient_id_count,
    COUNT(arrival_time)           AS arrival_time_count,
    COUNT(triage_time)            AS triage_time_count,
    COUNT(doctor_assigned_time)   AS doctor_assigned_time_count,
    COUNT(discharge_time)         AS discharge_time_count,
    COUNT(complaint_category)     AS complaint_category_count,
    COUNT(severity_level)         AS severity_level_count,
    COUNT(age)                    AS age_count,
    COUNT(insurance_status)       AS insurance_status_count,
    COUNT(doctor_id)              AS doctor_id_count,
    COUNT(nurse_id)               AS nurse_id_count
FROM ER_ANALYTICS.RAW_DATA.PATIENT_VISITS;

-- ACTUAL RESULTS:
-- total_rows: 29,400

-- DATA QUALITY ISSUES (must fix in Silver):
-- patient_id NULL:        882 rows (3.0%)  → COALESCE to 'UNKNOWN_PATIENT' + flag
-- age NULL:             1,470 rows (5.0%)  → Flag as 'Missing'
-- insurance_status NULL: 9,932 rows (33.8%) → COALESCE to 'Unknown'

-- VALID NULLs (business insights, keep as NULL):
-- triage_time NULL:          2,174 rows (7.4%)  → Left Before Triage (LWBS)
-- doctor_assigned_time NULL: 4,198 rows (14.3%) → Left Before Doctor (LWBS)
-- discharge_time NULL:       4,198 rows (14.3%) → Left Before Discharge
-- doctor_id NULL:            4,198 rows (14.3%) → No doctor assigned (LWBS)
-- nurse_id NULL:             4,198 rows (14.3%) → No nurse assigned (LWBS)

-- PERFECT COLUMNS (0 NULLs):
-- visit_id, arrival_time, complaint_category, severity_level: 29,400 (100% complete)


-- QUERY 2: DATE FORMAT IDENTIFICATION
-- Purpose: Understand how many different date string formats exist
-- so we can write the correct TRY_TO_TIMESTAMP conversions in Silver.
-- REGEXP replaces all digits with 'X' to reveal the pattern skeleton.

SELECT DISTINCT 
    REGEXP_REPLACE(arrival_time, '[0-9]', 'X') AS date_pattern,
    COUNT(*) AS frequency
FROM ER_ANALYTICS.RAW_DATA.PATIENT_VISITS
GROUP BY 1
ORDER BY 2 DESC;

-- ACTUAL RESULTS — 4 FORMAT TYPES FOUND:
-- Format 1: YYYY-MM-DD HH:MM:SS  → 26,431 rows (89.9%) — Standard Snowflake format with seconds
-- Format 2: YYYY/MM/DD HH:MM     →  1,030 rows  (3.5%) — Slash delimiter, no seconds
-- Format 3: DD-Mon-YYYY HH:MM    →    847 rows  (2.9%) — Day-first with text month (e.g. 15-Apr-2024 14:30)
-- Format 4: Mon DD YYYY HH:MM    →    972 rows  (3.3%) — Month-first with text month (e.g. Apr 15 2024 14:30)
-- ~120 rows return NULL from REGEXP (NULLs in source) — not a concern.

-- Silver fix: COALESCE(TRY_TO_TIMESTAMP format1, format2, format3, format4)
-- Snowflake's 'MON' token handles all month abbreviations automatically.


-- QUERY 3: COMPLAINT CATEGORY INCONSISTENCIES
-- Purpose: Find text standardization issues before cleaning.

SELECT DISTINCT complaint_category,
    COUNT(*) AS frequency
FROM ER_ANALYTICS.RAW_DATA.PATIENT_VISITS
GROUP BY 1
ORDER BY 1;

-- ACTUAL RESULTS — 3 TYPES OF INCONSISTENCY FOUND:
-- Issue 1: Lowercase versions of all categories (~15% of rows)
--          e.g. 'chest pain' instead of 'Chest Pain'
-- Issue 2: Spaced slash variants for 2 categories (~1.7% of rows)
--          e.g. 'Injury / Trauma' instead of 'Injury/Trauma'
--          e.g. 'Nausea / Vomiting' instead of 'Nausea/Vomiting'
-- Issue 3: Combination of both (lowercase + spaced slash)
--
-- Total raw variants: 26 dirty values → 12 clean after Silver fix
--
-- NOTE: INITCAP() alone is NOT sufficient because it produces
-- 'Shortness Of Breath' (capitalizes 'Of') instead of correct
-- 'Shortness of Breath'. CASE statement used instead.
--
-- Silver fix: CASE UPPER(TRIM(complaint_category)) WHEN ... END
-- maps all 26 variants explicitly to the 12 correct values.

-- CORRECT 12 CATEGORIES:
-- Abdominal Pain, Allergic Reaction, Back Pain, Chest Pain,
-- Dizziness, Fever, Headache, Injury/Trauma, Minor Cuts,
-- Nausea/Vomiting, Respiratory Issues, Shortness of Breath


-- QUERY 4: DUPLICATE DETECTION
-- Purpose: Find patients with identical arrival timestamps.
-- These are ghost records injected by the Python script (~5% of rows).
-- Composite key: patient_id + arrival_time
-- A real patient cannot physically arrive twice at the exact same second.

SELECT 
    patient_id, 
    arrival_time, 
    COUNT(*) AS visit_occurrence
FROM ER_ANALYTICS.RAW_DATA.PATIENT_VISITS
GROUP BY 1, 2
HAVING COUNT(*) > 1;

-- ACTUAL RESULTS:
-- ~1,386 duplicate pairs found (expected ~1,400 from Python script).
-- Difference explained: NULL patient_ids share same partition key,
-- causing slight over-collapse during dedup — acceptable variance.
-- One NULL patient_id row visible in output — handled by UNKNOWN_PATIENT fix.
-- Mixed date formats also appear in duplicates — confirms dedup must run
-- AFTER date standardization in Silver, not before.
-- Silver fix: ROW_NUMBER() PARTITION BY patient_id, arrival_time ORDER BY visit_id


-- QUERY 5: AGE DATA QUALITY
-- Purpose: Categorize age values as valid, missing, or impossible.
-- TRY_CAST prevents query failure on non-numeric values.

SELECT 
    CASE 
        WHEN age IS NULL                          THEN 'Missing Age'
        WHEN TRY_CAST(age AS INTEGER) IS NULL     THEN 'Not A Number'
        WHEN TRY_CAST(age AS INTEGER) < 1         THEN 'Negative/Zero Age'
        WHEN TRY_CAST(age AS INTEGER) > 120       THEN 'Too Old (>120)'
        ELSE 'Valid Age'
    END AS age_category,
    COUNT(*) AS record_count,
    MIN(TRY_CAST(age AS INTEGER)) AS youngest_in_group,
    MAX(TRY_CAST(age AS INTEGER)) AS oldest_in_group
FROM ER_ANALYTICS.RAW_DATA.PATIENT_VISITS
GROUP BY 1
ORDER BY record_count DESC;

-- ACTUAL RESULTS:
-- Valid Age:    27,920 rows — age 1 to 85 (matches Python script max of 85)
-- Missing Age:   1,470 rows — NULL values
-- Too Old >120:     10 rows — all have age=999 (random seed landed all on 999)
-- Not A Number:      0 rows — no text values in age column
--
-- Silver fix: Set age to NULL if outside 1-120 range, flag with age_quality_flag