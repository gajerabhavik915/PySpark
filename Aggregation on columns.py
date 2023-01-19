# Databricks notebook source
spark

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/c0851337@mylambton.ca/sample_data.csv")

# COMMAND ----------

df1.show()

# COMMAND ----------


