-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING)
USING csv
OPTIONS (path '/mnt/serpent04dl/raw/circuits.csv', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date STRING,
time STRING,
url STRING)
USING csv
OPTIONS (path '/mnt/serpent04dl/raw/races.csv', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create constructors table (JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT, 
constructorRef STRING, 
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS (path '/mnt/serpent04dl/raw/constructors.json', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create drivers table(nested JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT, 
driverRef STRING, 
number INT,
code STRING,
name STRUCT<forename:STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path '/mnt/serpent04dl/raw/drivers.json', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create results table(JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
result INT, raceId INT, driverId INT,contructorId INT,
number INT, grid INT, position INT, positionText STRING,
positionOrder INT, points DOUBLE, laps INT, time STRING,
milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING,
fastestLapSpeed STRING, statusId INT)
USING json
OPTIONS (path '/mnt/serpent04dl/raw/results.json', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create pit_stops table(multiline JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT, driverId INT, stop STRING ,lap INT,
time STRING, duration STRING, milliseconds INT)
USING json
OPTIONS (path '/mnt/serpent04dl/raw/pit_stops.json', header true, multiLine true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create lap_times table(CSV Folder)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT, 
driverId INT, 
lap INT,
position INT,
time STRING,
milliseconds INT)
USING csv
OPTIONS (path '/mnt/serpent04dl/raw/lap_times', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create qualifying table(Multiline JSON Folder)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyingId INT, raceId INT, driverId INT, constructorId INT,
number INT, position INT, q1 STRING, q2 STRING, q3 STRING)
USING json
OPTIONS (path '/mnt/serpent04dl/raw/qualifying', multiLine true);