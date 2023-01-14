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

user.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.alias('usr').select('usr.id', 'usr.f_name', 'usr.l_name').show()

# COMMAND ----------

user.select(col('id'), 'f_name', 'l_name').show()

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

user.select(col('id'), 'f_name', 'l_name', concat(col('f_name'), lit(' '), col('l_name')).alias('Full_name')).show()

# COMMAND ----------

user.selectExpr('id', 'f_name', 'l_name', "concat(f_name, ' ', l_name) as Full_name").show()

# COMMAND ----------

user.createOrReplaceTempView('usr')

# COMMAND ----------

spark.sql("""
   select id, f_name, l_name, concat(f_name, ' ' ,l_name) as Full_name from usr
   """).show()

# COMMAND ----------

##Below two commad will not work as alias is already given even though it has used prior name

# COMMAND ----------

user.alias('usr').select(user['id'], 'usr.f_name', 'usr.l_name').show()

# COMMAND ----------

user.alias('usr').selectExpr(user['id'], 'usr.f_name', 'usr.l_name').show()

# COMMAND ----------

## below command will not work, as selectExpr only take string operation, here col is given 

# COMMAND ----------

user.selectExpr(col('id'), 'f_name', 'l_name').show()

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

user.alias('usr').selectExpr('id', "concat(usr.f_name, ' ', usr.l_name) as full_name").show()

# COMMAND ----------

user.createOrReplaceTempView('usr')

# COMMAND ----------

spark.sql("""
   select u.id, u.l_name, u.f_name from usr as u
   """). show()

# COMMAND ----------

list_of_columns = ['id', 'f_name', 'l_name']
user.select(*list_of_columns).show()

# COMMAND ----------

## Let's understand about changing datatype

# COMMAND ----------

calling = col('id')

# COMMAND ----------

user.select(calling).show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

user.select('id', 'start_date', date_format('start_date', 'yyyyMMdd').alias('new_date')).show()

# COMMAND ----------

user.select('id', 'start_date', date_format('start_date', 'yyyyMMdd').alias('new_date')).dtypes

# COMMAND ----------

user.select('id', 'start_date', date_format('start_date', 'yyyyMMdd').cast('int').alias('new_date')).dtypes

# COMMAND ----------

user.select('id' , 'last_update', date_format('last_update', 'yyyyMMdd').cast('int').alias('Last Upadted Date')).show()

# COMMAND ----------

user.select('id' , 'last_update', date_format('last_update', 'yyyyMMdd').cast('int').alias('Last Upadted Date')).dtypes

# COMMAND ----------

user.show()

# COMMAND ----------

## below command will not work as sum can be done between two integer value, here one is string and second one is integer.

# COMMAND ----------

user.select('id', 'amount_paid', ('amount_paid'+25)).show()

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

user.select('id', (col('amount_paid') + lit(25)).alias('New_Amt')).show()

# COMMAND ----------

user.selectExpr('id', 'amount_paid', '(amount_paid + 25) AS New_Amt').show()

# COMMAND ----------

type(col('f_name'))

# COMMAND ----------

from pyspark.sql.functions import concat 

# COMMAND ----------

##Let's learn how to rename column by using 'withcolumn' 

# COMMAND ----------

user.select('id', concat(col('f_name'), lit(' '), col('l_name')).alias('Full_name')).show()

# COMMAND ----------

user.select('id', 'f_name', 'l_name').withColumn('Full_name', concat(col('f_name'), lit(' '), col('l_name')).alias('Fu_name')).show()

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

user.select('id', 'courses').show()

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

user.select('id', 'courses').withColumn('CountOfCourses', size('courses')).show()

# COMMAND ----------

## we can renaming the column by using withColumnRenamed

# COMMAND ----------

user.select('id', 'f_name', 'l_name'). \
   withColumnRenamed('id', 'usr_id'). \
   withColumnRenamed('f_name', 'Fisrt_name').show()

# COMMAND ----------

user.select(user['id'].alias('user_id'), user['f_name'].alias('first_name'), user['l_name'].alias('last_name')). \
   withColumn('Full_name', concat(col('first_name'), lit(' '), col('last_name'))). \
   show()




