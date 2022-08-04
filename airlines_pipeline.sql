-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze AS
SELECT * FROM cloud_files("/databricks-datasets/asa/airlines", "csv", map("header", "true"))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_delays AS
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
