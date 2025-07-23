-- Databricks notebook source
create schema demo

-- COMMAND ----------

show schemas

-- COMMAND ----------

create table demo.emp(empID int, empName string)

-- COMMAND ----------

insert into demo.emp values (101, "Shyam")

-- COMMAND ----------

select * from demo.emp

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdir("dbfs:/FileStore/ecomm")
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/Ecommerce
