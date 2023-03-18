# Databricks notebook source
res = dbutils.notebook.run('1.ingest_circuit_file', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('2.ingest_races_file', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('3.ingest_constructor_file', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('4.ingest_drivers_file(nested_json)', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('5.ingest_results_file', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('6.ingest_pitstops_file(multiline_json)', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('7.ingest_laptimes_file(folder)', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res

# COMMAND ----------

res = dbutils.notebook.run('8.ingest_qualifying_file(folder-multiline)', 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})

# COMMAND ----------

res