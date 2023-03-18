-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Define the most dominant teams

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######The average of points gained for each race was chosen as the main metric to rank drivers. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The most dominant team of all time

-- COMMAND ----------

SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The most dominant team of 2011-2020 period

-- COMMAND ----------

SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The most dominant team of 2001-2010 period

-- COMMAND ----------

SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

