-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Report on Dominant Teams

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams AS
SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points,
       RANK () OVER(ORDER BY ROUND(AVG(calculated_points), 2) DESC) AS team_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Line plot of teams dominance throughout the time

-- COMMAND ----------

SELECT race_year,
       team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
  GROUP BY race_year, team_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Bar chart of total finished races against total points

-- COMMAND ----------

SELECT race_year,
       team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
  GROUP BY race_year, team_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Area plot of teams dominance throughout the time

-- COMMAND ----------

SELECT race_year,
       team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points), 2) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
  GROUP BY race_year, team_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

