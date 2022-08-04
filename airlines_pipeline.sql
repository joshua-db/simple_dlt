-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze AS
SELECT * FROM cloud_files("/databricks-datasets/asa/airlines", "csv", map("header", "true"))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_delays (
CONSTRAINT arr_delay_is_numeric EXPECT (try_cast(ArrDelay AS DOUBLE) IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT dep_delay_is_numeric EXPECT (try_cast(DepDelay AS DOUBLE) IS NOT NULL) ON VIOLATION DROP ROW)
AS
SELECT Year, Month, DayofMonth, UniqueCarrier, ArrDelay, DepDelay
FROM STREAM(LIVE.bronze)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_UA_delays AS
SELECT *
FROM STREAM(LIVE.silver_delays)
WHERE UniqueCarrier = "UA"

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_NW_delays AS
SELECT *
FROM STREAM(LIVE.silver_delays)
WHERE UniqueCarrier = "NW"
