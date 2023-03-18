# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def replace_partition_column(df, col_name):
    columns = df.columns
    columns.pop(columns.index(col_name))
    columns.append(col_name)
    final_df = df.select(columns)
    return final_df
        

# COMMAND ----------

#Incremental load function
def incremental_load(df, partition_column, db, table):
    output_df = replace_partition_column(df, partition_column)
    #Setup configuration to prevent job from overwriting the entire table
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    #Dynamically overwrite data, if the table exists
    if (spark._jsparkSession.catalog().tableExists(f'{db}.{table}')):
        output_df \
        .write \
        .mode('overwrite') \
        .insertInto(f'{db}.{table}')
    #Create new Parquet Table in case it doesnt already exist
    else:
        output_df \
        .write \
        .partitionBy(partition_column) \
        .mode('overwrite') \
        .format('parquet') \
        .saveAsTable(f'{db}.{table}')

# COMMAND ----------

 def delta_incremental_load(df, db_name, table_name, folder_path, merge_condition, partition_column):
        #Setup configuration to optimize condition check
        spark.conf.set('spark.databricks.optimizer.dynamicPatitionPruning', 'true')
        #Using Merge transaction to update data in case the table already exists
        if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
            deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
            deltaTable.alias('tgt') \
            .merge(df.alias('src'), 
            merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        #Create new Delta Table in case it doesnt already exist
        else:
            df \
            .write \
            .partitionBy(partition_column) \
            .mode('overwrite') \
            .format('delta') \
            .saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

#Saves columns names as a list. Used
def df_column_to_list(df, col_name):
    df_rows = df.select(col_name) \
    .distinct() \
    .collect()
    
    col_val_list = [row[col_name] for row in df_rows]
    return col_val_list