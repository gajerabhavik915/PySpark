# Databricks notebook source
spark

# COMMAND ----------

import datetime

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

users = [{'id' : 1,
          'f_name':'Hari',
          'l_name': 'Ghanu',
          'email': 'ghanu@hari.com',
          'is_customer': True,
          'amount_paid': 4000,
          'Phone Number' : Row(home_number='234567891', office_number='2345136789'),
          'customer_from': 'Akshardham',
          'start_date': datetime.date(2020,1,1),
          'last_update': datetime.datetime(2021, 1,1,15,0)
         },
         {
          'id' : 2,
          'f_name':'Ghanashyam',
          'l_name': 'Ghanuji',
          'email': 'ghanuji@ghanashyam.com',
          'is_customer': False,
          'amount_paid': 8000,
          'Phone Number' : Row(home_number='234867891', office_number='2346656789'),
          'customer_from': 'Bramhand',
          'start_date': datetime.date(2020,2,1),
          'last_update': datetime.datetime(2021, 2,1,15,0)
         },
         {
          'id' : 3,
          'f_name':'shreeHari',
          'l_name': 'Ghanshyamji',
          'email': 'ghanu@shareehari.com',
          'is_customer': True,
          'amount_paid': 7000,
          'Phone Number' : Row(home_number = None, office_number = None),
          'customer_from': 'Purushotam',
          'start_date': datetime.date(2020,4,1),
          'last_update': datetime.datetime(2021, 5,1,15,0)
         },
         {
             'id' : 4,
          'f_name':'Lalaji',
          'l_name': 'Lalacharan',
          'email': 'ghanu@lalsharan.com',
          'is_customer': False,
          'amount_paid': 3000,
          'Phone Number' : Row(home_number='345678915', office_number='2345136789'),
          'customer_from': 'AksharOradi',
          'start_date': datetime.date(2020,8,1),
          'last_update': datetime.datetime(2021,9,1,15,0)
         }
    
]

# COMMAND ----------

import pandas as pd

# COMMAND ----------

user = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

user.select('id', 'f_name', 'l_name').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.select('id', col('f_name').alias('Firt_name'), col('l_name').alias('Last_name')).show()

# COMMAND ----------

user.alias('Usr').select('Usr.id', 'Usr.f_name', 'Usr.l_name').show()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

user.select('id', col('f_name'), col('l_name'), concat(col('f_name'), lit(' '), col('l_name')).alias('Full_name')).show()

# COMMAND ----------

user.selectExpr('id', 'f_name', 'l_name', "concat(f_name, ',', l_name) as full_name").show()

# COMMAND ----------

user.createOrReplaceTempView('usr')

# COMMAND ----------

spark.sql("""
   select id, f_name, l_name, concat(f_name, ' ' ,l_name) as Full_name from usr """).show()

# COMMAND ----------


