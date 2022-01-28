# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Dataframe using SQL
# MAGIC ##### 1.Temp view

# COMMAND ----------

race_results_df = spark.read.parquet('/mnt/formula1dl199/presentation/race_results/')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

#race_results_df.createTempView('v_race_results')
race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

p_year = 2020

# COMMAND ----------

count_2020 = spark.sql(f"SELECT COUNT(*) FROM v_race_results WHERE race_year = {p_year}")

# COMMAND ----------

display(count_2020)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.Global Temp View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN GLOBAL_TEMP;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GV_RACE_RESULTS 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.GV_RACE_RESULTS 

# COMMAND ----------

