# Databricks notebook source
# MAGIC %md 
# MAGIC ###Produce contructor standings

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

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
# MAGIC #####Step 1 - Import Race Results data

# COMMAND ----------

race_results_list = spark.read.format('delta') \
.load(f'{presentation_folder_path}/race_results/') \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Insure imported data contains all expected years

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format('delta') \
.load(f'{presentation_folder_path}/race_results/') \
.filter(col('race_year').isin(race_year_list))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Make a drivers rating list for each year by counting the amount of points and wins for each driver 

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy('race_year', 'team') \
.agg(sum('points').alias('total_points'),
    count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

# COMMAND ----------

final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write final data to gold tier directory in ADSL

# COMMAND ----------

condition = 'tgt.team = src.team AND tgt.race_year = src.race_year'
delta_incremental_load(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, condition, 'race_year')