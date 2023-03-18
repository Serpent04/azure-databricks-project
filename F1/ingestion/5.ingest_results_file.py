# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 0 - import external tools and initialize parameters

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

results_schema = StructType(fields=[StructField('resultId', IntegerType(), False),
                                   StructField('raceId', IntegerType(), False),
                                   StructField('driverId', IntegerType(), False),                                
                                   StructField('constructorId', IntegerType(), False),
                                   StructField('number', IntegerType(), True),
                                   StructField('grid', IntegerType(), False),
                                   StructField('position', IntegerType(), True),
                                   StructField('positionText', StringType(), False),
                                   StructField('positionOrder', IntegerType(), False),
                                   StructField('points', FloatType(), False),
                                   StructField('laps', IntegerType(), False),
                                   StructField('time', StringType(), True),
                                   StructField('milliseconds', IntegerType(), True),
                                   StructField('fastestLap', IntegerType(), True),
                                   StructField('rank', IntegerType(), True),
                                   StructField('fastestLapTime', StringType(), True),
                                   StructField('fastestLapSpeed', StringType(), True),
                                   StructField('statusId', IntegerType(), False)])

# COMMAND ----------

results_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/results.json', schema=results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename columns and add new columns containing ingestion date, source of data, date of data creation

# COMMAND ----------

results_renamed_df = results_df \
.withColumnRenamed('resultId', 'result_id') \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumnRenamed('positionText', 'position_text') \
.withColumnRenamed('positionOrder', 'position_order') \
.withColumnRenamed('fastestLap', 'fastest_lap') \
.withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Drop the unwanted columns

# COMMAND ----------

results_final_df = results_renamed_df.drop(col('statusId')).dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ######Drop duplicates in the final df

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - partition by race id and write the resulting dataframe to ADSL

# COMMAND ----------

condition = 'tgt.result_id = src.result_id AND tgt.race_id = src.race_id'
delta_incremental_load(results_final_df, 'f1_processed', 'results', processed_folder_path, condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Good')