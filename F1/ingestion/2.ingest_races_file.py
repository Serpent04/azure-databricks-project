# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest races.csv

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField, TimestampType
from pyspark.sql.functions import *

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
# MAGIC #####Step 1 - read the CSV file using the spark dataframe reader

# COMMAND ----------

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                 StructField('year', IntegerType(), True),
                                 StructField('round', IntegerType(), True),
                                 StructField('circuitId', IntegerType(), False),
                                 StructField('name', StringType(), True),
                                 StructField('date', StringType(), True),
                                 StructField('time', StringType(), True),
                                 StructField('url', StringType(), True)])

# COMMAND ----------

races_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/races.csv', header=True, schema=races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - transform dataframe

# COMMAND ----------

races_renamed_df = races_df \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('year', 'race_year') \
.withColumnRenamed('circuitId', 'circuit_id') \
.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - finalizing resulting dataframe

# COMMAND ----------

races_final_df = races_renamed_df \
.select('race_id', 'race_year', 'round', 'circuit_id', 'name', 'race_timestamp', 'ingestion_date', 'file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write the resulting DF into ADSL

# COMMAND ----------

races_final_df.write \
.mode('overwrite') \
.format('delta') \
.saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Good')