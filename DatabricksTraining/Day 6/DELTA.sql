-- Databricks notebook source
create table emp(id integer, name string, salary int)

-- COMMAND ----------

desc extended emp

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

-- COMMAND ----------

desc history emp

-- COMMAND ----------

insert into emp values(101, "Shyam", 150000)

-- COMMAND ----------

select * from emp

-- COMMAND ----------

desc detail emp
