-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESC database demo;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1.create managed tables using python
-- MAGIC ##### 2.create managed tables using SQL
-- MAGIC ##### 3.Effect of dropping managed tables
-- MAGIC ##### 4.describe table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet('/mnt/formula1dl199/presentation/race_results/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_sql;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

DROP table demo.race_results_sql;

-- COMMAND ----------

SHOW Tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External table
-- MAGIC ##### 1. Create External table using python
-- MAGIC ##### 2. Create External table using sql
-- MAGIC ##### 3. Effect of dropping external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format('parquet').option('path', '/mnt/formula1dl199/presentation/race_results_ext_py/').saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

DESC EXTENDED race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION '/mnt/formula1dl199/presentation/race_results_ext_sql/'

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_sql

-- COMMAND ----------

show tables

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABleS IN demo;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on table
-- MAGIC   ##### 1.Create Temp View
-- MAGIC   ##### 2.Create Global Temp View
-- MAGIC   ##### 3.Create Permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
  SELECT * FROM demo.race_results_python 
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * from v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
  SELECT * FROM demo.race_results_python
  WHERE race_year = 2012

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

SELECT * from global_temp.gv_race_results;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
  SELECT * FROM demo.race_results_python
  WHERE race_year = 2000

-- COMMAND ----------

show databases;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

