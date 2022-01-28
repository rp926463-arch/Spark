# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Introdunction
# MAGIC * UI intro
# MAGIC * Magic commands

# COMMAND ----------

msg = "Hello"

# COMMAND ----------

print(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/COVID/USAFacts/

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/databricks-datasets/COVID/USAFacts/covid_deaths_usafacts.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

