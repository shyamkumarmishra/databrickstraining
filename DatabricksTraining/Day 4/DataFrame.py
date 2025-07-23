# Databricks notebook source
spark

# COMMAND ----------

user_data = [(101, "Shyam"), (102, "Anil")]
user_schema = "usrId int, userName string"
df = spark.createDataFrame(data=user_data, schema=user_schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1 = df.select(col("usrId").alias("UserId"))

# COMMAND ----------

df1.show()

# COMMAND ----------

df.withColumnRenamed('UserId', "ID").withColumnRenamed("userName", "Name").show()

# COMMAND ----------

df.show()
