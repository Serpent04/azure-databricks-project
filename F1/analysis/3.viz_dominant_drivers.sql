-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Report on dominant drivers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers AS
SELECT driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points,
       RANK () OVER(ORDER BY ROUND(AVG(calculated_points), 2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Line plot of drivers dominance throughout the time

-- COMMAND ----------

SELECT race_year,
       driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Bar chart of total finished races against total points

-- COMMAND ----------

SELECT race_year,
       driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Area plot of drivers dominance throughout the time

-- COMMAND ----------

SELECT race_year,
       driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

