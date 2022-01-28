# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/formula1dl199/processed/

# COMMAND ----------

circuits_df = spark.read.parquet('/mnt/formula1dl199/processed/circuits/').filter("circuit_id < 70").withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_df = spark.read.parquet('/mnt/formula1dl199/processed/races/').filter("race_year = 2019").withColumnRenamed('name', 'race_name')

# COMMAND ----------

display(race_df)

# COMMAND ----------

inner_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, race_df.race_name, race_df.round)

# COMMAND ----------

display(inner_df)

# COMMAND ----------

left_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, race_df.race_name, race_df.round)

# COMMAND ----------

display(left_df)

# COMMAND ----------

right_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, race_df.race_name, race_df.round)

# COMMAND ----------

display(right_df)

# COMMAND ----------

full_outer_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "outer") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, race_df.race_name, race_df.round)

# COMMAND ----------

display(full_outer_df)

# COMMAND ----------

exist_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "semi")

# COMMAND ----------

display(exist_df)

# COMMAND ----------

not_exist_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "anti")

# COMMAND ----------

display(not_exist_df)

# COMMAND ----------

cross_df = circuits_df.crossJoin(race_df)

# COMMAND ----------

display(cross_df)

# COMMAND ----------

