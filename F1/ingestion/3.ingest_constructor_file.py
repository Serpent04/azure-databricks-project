# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 0 - import external tools and initialize parameters

# COMMAND ----------

from pyspark.sql.functions import *

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

# MAGIC %md
# MAGIC #####DDL way of defining schema

# COMMAND ----------

constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

contructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Rename columns and add ingestion date

# COMMAND ----------

contructors_final_df = contructors_dropped_df \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumnRenamed('constructorRef', 'constructor_ref') \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write the resulting DF into ADSL

# COMMAND ----------

contructors_final_df.write \
.mode('overwrite') \
.format('delta') \
.saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Good')