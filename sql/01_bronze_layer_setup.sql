/********************************************************************************
  PROJECT: Emergency Department Wait Time Optimization
  LAYER: Bronze (Raw Data Layer)
  DATE: Day 1
  PURPOSE: Initial Snowflake setup and data loading
*********************************************************************************/

CREATE DATABASE IF NOT EXISTS ER_ANALYTICS;
USE DATABASE ER_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

CREATE OR REPLACE TABLE PATIENT_VISITS (
    visit_id             VARCHAR(10),
    patient_id           VARCHAR(10),
    arrival_time         VARCHAR(50),
    triage_time          VARCHAR(50),
    doctor_assigned_time VARCHAR(50),
    discharge_time       VARCHAR(50),
    complaint_category   VARCHAR(100),
    severity_level       VARCHAR(20),
    age                  VARCHAR(10),
    insurance_status     VARCHAR(20),
    doctor_id            VARCHAR(10),
    nurse_id             VARCHAR(10)
);

-- All columns loaded as VARCHAR intentionally.
-- Reason: Mixed date formats and dirty age values would cause
-- type errors on load if we used DATE or INTEGER columns.
-- We cast to correct types in the Silver layer after cleaning.

DESCRIBE TABLE PATIENT_VISITS;

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE my_csv_stage
    FILE_FORMAT = csv_format;

LIST @my_csv_stage;

COPY INTO PATIENT_VISITS
FROM @my_csv_stage/er_wait_times_data.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS total_rows
FROM PATIENT_VISITS;
-- Result: 29,400 rows loaded successfully