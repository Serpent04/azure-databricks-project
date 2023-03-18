# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest lap_times.csv file

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
# MAGIC #####Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

laptimes_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('lap', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

laptimes_df = spark.read \
.schema(laptimes_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/lap_times') 


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename columns and add new columns

# COMMAND ----------

laptimes_final_df = laptimes_df \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Write the resulting DF into ADSL

# COMMAND ----------

condition = 'tgt.race_id = src.race_id AND tgt.driver_id=src.driver_id AND tgt.lap = src.lap'
delta_incremental_load(laptimes_final_df, 'f1_processed', 'lap_times', processed_folder_path, condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Good')