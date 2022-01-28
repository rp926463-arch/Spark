# Databricks notebook source
df = spark.read.parquet('/mnt/formula1dl199/presentation/race_results/')

# COMMAND ----------

display(df)

# COMMAND ----------

demo_df = df.filter("race_year==2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count('race_name').alias('races')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name').alias('total_races')).show()

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum('points').alias('Total points scored by Lewis in 2020'),countDistinct('race_name').alias('number_of_races')).show()

# COMMAND ----------

demo_df \
.groupBy('driver_name') \
.agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races')) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window function

# COMMAND ----------

demo_df = df.filter('race_year in (2019,2020)')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df \
.groupBy('race_year', 'driver_name') \
.agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races'))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('total_points'))
demo_grouped_df.withColumn('rank', rank().over(driverRankSpec)).show()

# COMMAND ----------

 