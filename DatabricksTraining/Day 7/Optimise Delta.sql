-- Databricks notebook source
create table demo(id int, name string)

-- COMMAND ----------

desc extended demo

-- COMMAND ----------

insert into demo values (1, "Shyam");
insert into demo values (1, "Ram");
insert into demo values (1, "Mohan");
insert into demo values (1, "Sohan");
insert into demo values (1, "Devanshu")

-- COMMAND ----------

select * from demo

-- COMMAND ----------

optimize demo
zorder by (id)

-- COMMAND ----------

desc history demo

-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo

-- COMMAND ----------

select * from demo

-- COMMAND ----------

desc history demo

-- COMMAND ----------

restore table demo to version as of 6

-- COMMAND ----------

select * from demo

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

create table demo_vendors (id int, name string) location '/FileStore/eclerx_metadata/vendors'

-- COMMAND ----------

insert into demo_vendors values (1,'a')

-- COMMAND ----------

drop table demo

-- COMMAND ----------

drop table demo_vendors
