# Databricks notebook source
# MAGIC %md
# MAGIC ## FileSystem Utility

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for folder_name in dbutils.fs.ls('/'):
    print(folder_name)

# COMMAND ----------

#list all fileSystem Utility commands available
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('mount')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Workflow Utility

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run('./Child-Notebook',10, {"input" : "Called from main notebook"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library Utility

# COMMAND ----------

# MAGIC %pip install pandas

# COMMAND ----------

