# Databricks notebook source
# MAGIC %md
# MAGIC ####Create a full-detailed race results table

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 0 - import external tools and initialize parameters

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - import all necessary tables

# COMMAND ----------

circuits_df = spark.read.format('delta') \
.load(f'{processed_folder_path}/circuits') \
.withColumnRenamed('name', 'circuit_name') \
.withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

drivers_df = spark.read.format('delta') \
.load(f'{processed_folder_path}/drivers') \
.withColumnRenamed('name', 'driver_name') \
.withColumnRenamed('number', 'driver_number') \
.withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format('delta') \
.load(f'{processed_folder_path}/constructors') \
.withColumnRenamed('name', 'team')

# COMMAND ----------

races_df = spark.read.format('delta') \
.load(f'{processed_folder_path}/races') \
.withColumnRenamed('name', 'race_name') \
.withColumn('race_date', to_date(col('race_timestamp')))

# COMMAND ----------

results_df = spark.read.format('delta') \
.load(f'{processed_folder_path}/results') \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed('time', 'race_time') \
.withColumnRenamed('race_id', 'results_race_id') \
.withColumnRenamed('file_date', 'results_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - join all the tables into one

# COMMAND ----------

# MAGIC %md
# MAGIC ######Since the F1 organization has changed the counting points policy throught the years, we should normalize the results for races in all years. Thus, new column "calculated_points" containing a number from 1 to 10 (we include only top 10 drivers to the list) will replace the original "points" column for further calculations.

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, on='circuit_id') \
.select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location')

# COMMAND ----------

final_df = results_df \
.join(races_circuits_df, results_df.results_race_id==races_circuits_df.race_id) \
.join(drivers_df, on='driver_id') \
.join(constructors_df, on='constructor_id') \
.select('race_year', 'team', 'driver_id', 'driver_name', 
        'race_id', 'position', 'points') \
.withColumn('calculated_points', 11 - col('position')) \
.withColumn('created_date', current_timestamp()) \
.filter(col('position') <= 10)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - write the resulting table to ADSL in Delta format

# COMMAND ----------

condition = 'tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id'
delta_incremental_load(final_df, 'f1_presentation', 'calculated_race_results', presentation_folder_path, condition, 'race_id')