# Databricks notebook source
spark

# COMMAND ----------

age = [10,11,1,12,14]

# COMMAND ----------

type(age)

# COMMAND ----------

df = spark.createDataFrame(age, 'int')

# COMMAND ----------

df.show()

# COMMAND ----------

name = ['hari', 'Ghanshyam', 'Maharaj', 'Krishna']

# COMMAND ----------

df = spark.createDataFrame(name, 'string')

# COMMAND ----------

df.show()

# COMMAND ----------

ListOfTuple = [('hari', 1), ('Ghanshyam', 2), ('Maharaj', 3), ('Krishna',4)]

# COMMAND ----------

df = spark.createDataFrame(ListOfTuple, 'name string, id int')

# COMMAND ----------

df.show()


# COMMAND ----------

df.collect()

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

Row("Bhavik", 3)

# COMMAND ----------

Ro = Row(name='Bhavik', id=2)

# COMMAND ----------

Ro.name

# COMMAND ----------

ListOfTuple = [['hari', 1], ['Ghanshyam', 2], ['Maharaj', 3], ['Krishna',4]]

# COMMAND ----------

df = spark.createDataFrame(ListOfTuple)

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.createDataFrame(ListOfTuple, 'name string, id int')

# COMMAND ----------

df.show()

# COMMAND ----------

ListOfTuple = [['hari', 1], ['Ghanshyam', 2], ['Maharaj', 3], ['Krishna',4]]

# COMMAND ----------

rows = [Row(*list1) for list1 in ListOfTuple]

# COMMAND ----------

rows

# COMMAND ----------

ListOfTuple = [{'name':'hari', 'id':1},
               {'name':'hadri', 'id':2},
               {'name':'hasri', 'id':3},
               {'name':'haari', 'id':4}]

# COMMAND ----------

spark.createDataFrame(ListOfTuple).show()

# COMMAND ----------

ListOfTuple1 = ListOfTuple[1]

# COMMAND ----------

Row(*ListOfTuple1.values())

# COMMAND ----------

r = [Row(*user1.values()) for user1 in ListOfTuple]

# COMMAND ----------

r

# COMMAND ----------

spark.createDataFrame(r).show()

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
          'customer_from': 'Brahmhand',
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
          'customer_from': 'AksharOradi',
          'start_date': datetime.date(2020,8,1),
          'last_update': datetime.datetime(2021,9,1,15,0)
         }
    
]

# COMMAND ----------

u = spark.createDataFrame([Row(*user.values()) for user in users])

# COMMAND ----------

u.show()

# COMMAND ----------

spark

# COMMAND ----------

import datetime 

# COMMAND ----------

users = [{'id' : 1,
          'f_name':'Hari',
          'l_name': 'Ghanu',
          'email': 'ghanu@hari.com',
          'is_customer': True,
          'amount_paid': 4000,
          'Phone Number' : [234567891, 2345136789],
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
          'Phone Number' : [234567891, 2345136789],
          'customer_from': 'Brahmhand',
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
          'Phone Number' : None,
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
          'Phone Number' : [234567891, 2345136789],
          'customer_from': 'AksharOradi',
          'start_date': datetime.date(2020,8,1),
          'last_update': datetime.datetime(2021,9,1,15,0)
         }
    
]

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

import pandas as pd

# COMMAND ----------

users1 = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users1.printSchema()

# COMMAND ----------

users1.show()

# COMMAND ----------

users1.columns

# COMMAND ----------

users1.dtypes

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

users1. \
    withColumn('Phone Number', explode('Phone Number')). \
    show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users1.select( 'id', col('Phone Number')[0].alias('Home Number'), col('Phone Number')[1].alias('Office Number')).show()

# COMMAND ----------

from pyspark.sql.functions import explode_outer 

# COMMAND ----------

users1. \
    withColumn('Phone Number', explode_outer('Phone Number')). \
    show()

# COMMAND ----------

users = [{'id' : 1,
          'f_name':'Hari',
          'l_name': 'Ghanu',
          'email': 'ghanu@hari.com',
          'is_customer': True,
          'amount_paid': 4000,
          'Phone Number' : {'home_number': '234567891', 'office_number':'2345136789'},
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
          'Phone Number' : {'home_number':'234867891', 'office_number':'2346656789'},
          'customer_from': 'Brahmhand',
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
          'Phone Number' : None,
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
          'Phone Number' : {'home_number':'345678915', 'office_number':'2345136789'},
          'customer_from': 'AksharOradi',
          'start_date': datetime.date(2020,8,1),
          'last_update': datetime.datetime(2021,9,1,15,0)
         }
    
]

# COMMAND ----------

user = spark.createDataFrame([Row(**user1) for user1 in users])

# COMMAND ----------

user.columns

# COMMAND ----------

user.dtypes

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.select( 'id', ('Phone Number')).show(truncate = False)

# COMMAND ----------

user.select('id', col('Phone Number')['home_number'].alias('Home_NUM'), col('Phone Number')['office_number'].alias('Office_NUM')).show()

# COMMAND ----------

