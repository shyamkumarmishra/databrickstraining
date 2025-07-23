# Databricks notebook source
df =  spark.read.csv("dbfs:/FileStore/ecomm/circuits.csv", inferSchema=True, header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date
df1=df.withColumnRenamed("CircuitId", "Circuit_Id").withColumn("CurrentDate", current_date()).drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists eclerx

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("eclerx.circuit")

# COMMAND ----------

desc extended
