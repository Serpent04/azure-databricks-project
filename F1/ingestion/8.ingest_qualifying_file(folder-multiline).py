# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest qualifying.json file

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
# MAGIC ####Ingest qualifying.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), False),
                                    StructField('constructorId', IntegerType(), False),
                                    StructField('number', IntegerType(), False),
                                    StructField('position', IntegerType(), True),
                                    StructField('q1', StringType(), True),
                                    StructField('q2', StringType(), True),
                                    StructField('q3', StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option('multiline', True) \
.json(f'{raw_folder_path}/{v_file_date}/qualifying') 


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename columns and add new columns containing ingestion date, source of data, date of data creation

# COMMAND ----------

qualifying_final_df = qualifying_df \
.withColumnRenamed('qualifyId', 'qualify_id') \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Write the resulting DF into ADSL

# COMMAND ----------

condition = 'tgt.qualify_ID = src.qualify_id'
delta_incremental_load(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Good')