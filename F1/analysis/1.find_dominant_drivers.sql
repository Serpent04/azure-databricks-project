-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Define the most dominant drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######The average of points gained for each race was chosen as the main metric to rank drivers. The reason for that is that there are multiple cases where a driver has gained significantly more points for significantly less number of races finished. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The most dominant driver of all time

-- COMMAND ----------

SELECT driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The most dominant driver of 2011-2020 period

-- COMMAND ----------

SELECT driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The most dominant driver of 2001-2010 period

-- COMMAND ----------

SELECT driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC