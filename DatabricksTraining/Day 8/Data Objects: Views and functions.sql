-- Databricks notebook source
-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/day.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv", header=True, inferSchema=True)
-- MAGIC df.write.saveAsTable("bike_day")
-- MAGIC

-- COMMAND ----------

create or replace view  max_month as
select mnth, round(max(temp),2) as max from bike_day group by mnth order by max desc

-- COMMAND ----------

show views

-- COMMAND ----------

select * from max_month

-- COMMAND ----------



-- COMMAND ----------

select * from hive_metastore.default.bike_day

-- COMMAND ----------

create or replace temp view holiday_count_temp as
select mnth, count(holiday) as holiday from bike_day where holiday=1 group by mnth order by holiday desc

-- COMMAND ----------

desc extended max_month

-- COMMAND ----------

show views

-- COMMAND ----------

create or replace function fullname(first_name string, last_name string)
returns string
return concat(first_name, " ", last_name)

-- COMMAND ----------

select fullname("Shyam", "Mishra") as name

-- COMMAND ----------

create or replace function age_group(age int)
returns string
return
case
  when age >= 60 then 'senior'
  when age >= 20 then 'adult'
  else  'teenager'
end;

-- COMMAND ----------

create table if not exists person_details(id int, first_name string, last_name string, age int);
INSERT INTO person_details VALUES (101, 'Shyam', 'Mishra', 30);
INSERT INTO person_details VALUES (102, 'Aarav', 'Sharma', 15);
INSERT INTO person_details VALUES (103, 'Riya', 'Verma', 22);
INSERT INTO person_details VALUES (104, 'Anjali', 'Patel', 65);
INSERT INTO person_details VALUES (105, 'Dev', 'Reddy', 12);
INSERT INTO person_details VALUES (106, 'Karan', 'Singh', 45);
INSERT INTO person_details VALUES (107, 'Meera', 'Desai', 18);
INSERT INTO person_details VALUES (108, 'Vikram', 'Joshi', 72);
INSERT INTO person_details VALUES (109, 'Pooja', 'Mehta', 16);
INSERT INTO person_details VALUES (110, 'Rahul', 'Kapoor', 38);

-- COMMAND ----------

select hive_metastore.default.age_group(age) as agegroup, age from person_details

-- COMMAND ----------

select * from person_details
