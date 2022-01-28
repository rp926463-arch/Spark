# Databricks notebook source
df = spark.read.option("header", True).option("InferSchema", True).csv('/mnt/formula1dl199/raw/races.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.filter("year = 2019 and round <= 5"))

# COMMAND ----------

display(df.filter((df.year == 2019) & (df.round <= 5)))

# COMMAND ----------

display(df.filter((df["year"] == 2019) & (df["round"] <= 5)))

# COMMAND ----------

