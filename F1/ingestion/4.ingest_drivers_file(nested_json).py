# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 0 - import external tools and initialize parameters

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename', StringType(), True),
                                StructField('surname', StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), True),
                                   StructField('driverRef', StringType(), True),
                                   StructField('number', IntegerType(), True),
                                   StructField('code', StringType(), True),
                                   StructField('name', name_schema),
                                   StructField('dob', DateType(), True),
                                   StructField('nationality', StringType(), True),
                                   StructField('url', StringType(), True)])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename columns and add new columns containing ingestion date, source of data, date of data creation

# COMMAND ----------

drivers_with_columns_df = drivers_df \
.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('driverRef', 'driver_ref') \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write the resulting dataframe to ADSL

# COMMAND ----------

drivers_final_df.write \
.mode('overwrite') \
.format('delta') \
.saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Good')