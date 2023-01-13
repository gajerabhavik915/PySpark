# Databricks notebook source
spark

# COMMAND ----------

###Creating DataFrame in Spark

# COMMAND ----------

age = [10,11,14,13,24,16,45]

# COMMAND ----------

type(age)

# COMMAND ----------

df=spark.createDataFrame(age, 'int')

# COMMAND ----------

df.show()

# COMMAND ----------

name = ['hari', 'Ghanshyam', 'Maharaj', 'Krishna']

# COMMAND ----------

type(name)

# COMMAND ----------

df = spark.createDataFrame(name, 'string')

# COMMAND ----------

df.show()

# COMMAND ----------

ListOfTuple = [('hari', 1), ('Ghanshyam', 2), ('Maharaj', 3), ('Krishna',4)]

# COMMAND ----------

df = spark.createDataFrame(ListOfTuple, 'name string, id int')

# COMMAND ----------


