-- Databricks notebook source
create database databricks

-- COMMAND ----------

show databases;
use databricks;


-- COMMAND ----------

create table emp(name string, emp_id int)

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe emp

-- COMMAND ----------

insert into table emp values ("Mohan", 102),( "Manish", 103)

-- COMMAND ----------

select * from emp
