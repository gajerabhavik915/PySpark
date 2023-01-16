# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

import datetime

# COMMAND ----------

users = [{'id' : 1,
          'f_name':'Hari',
          'l_name': 'Ghanu',
          'email': 'ghanu@hari.com',
          'is_customer': True,
          'courses' : [1,2],
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
          'courses' : [1],
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
          'courses' : [1,2],
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
          'courses' : [1],
          'amount_paid': 3000,
          'Phone Number' : Row(home_number='345678915', office_number='2345136789'),
          'customer_from': 'AksharOradi',
          'start_date': datetime.date(2020,8,1),
          'last_update': datetime.datetime(2021,9,1,15,0)
         }
    
]

# COMMAND ----------

user = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

user.drop('id').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.drop(col('id')).show()

# COMMAND ----------

user.drop(col('id'), col('f_name')).show()

# COMMAND ----------

user.drop('id', 'f_name').show()

# COMMAND ----------

## another thing needs to remember

# COMMAND ----------

dropping_list = ['id', 'f_name', 'l_name']

# COMMAND ----------

user.drop(dropping_list).show()

# COMMAND ----------

user.drop(*dropping_list).show()

# COMMAND ----------


