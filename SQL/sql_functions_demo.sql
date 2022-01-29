-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
  FROM drivers LIMIT 10

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] AS forename, SPLIT(name, ' ')[1] AS surname 
  FROM drivers LIMIT 10

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy') AS new_dob_format
  FROM drivers LIMIT 10

-- COMMAND ----------

SELECT *, date_add(dob, 1) AS date_moved
  FROM drivers LIMIT 10

-- COMMAND ----------

SELECT nationality, COUNT(*) as no_of_drivers
  FROM drivers 
  GROUP BY nationality 
  HAVING no_of_drivers > 100 
  ORDER BY no_of_drivers DESC

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank 
  FROM drivers
  ORDER BY nationality, age_rank

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
    FROM f1_presentation.driver_standings
  WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
    FROM f1_presentation.driver_standings
  WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  JOIN v_driver_standings_2020 d_2020
  ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  LEFT JOIN v_driver_standings_2020 d_2020
  ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  RIGHT JOIN v_driver_standings_2020 d_2020
  ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  FULL JOIN v_driver_standings_2020 d_2020
  ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  SEMI JOIN v_driver_standings_2020 d_2020
  ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  ANTI JOIN v_driver_standings_2020 d_2020
  ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------


