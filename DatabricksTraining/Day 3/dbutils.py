# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/Ecommerce/

# COMMAND ----------

dbutils.fs.rmd("dbfs:/FileStore/Ecommerce/sales-1.csv", recurse=True)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/ecomm")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Ecommerce", True)
