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

from pyspark.sql.functions import col

# COMMAND ----------

user.filter(col('id') == 1).show()

# COMMAND ----------

user.filter(user['id'] == 1).show()

# COMMAND ----------

user.filter('id == 1').show()

# COMMAND ----------

user.where('id == 1').show()

# COMMAND ----------

user.createOrReplaceTempView('usr')

# COMMAND ----------

spark.sql("""
   select * from usr
   where id == 1
   """).show()

# COMMAND ----------

## For Boolean value.

# COMMAND ----------

user.filter(col('is_customer') == True).show()

# COMMAND ----------

user.where(col('is_customer') == 'true').show()

# COMMAND ----------

user.filter('is_customer == true').show()

# COMMAND ----------

user.filter('is_customer = true').show()

# COMMAND ----------

user.filter(col('customer_from') == 'Akshardham').show()

# COMMAND ----------

user.where('customer_from = "Akshardham"').show()

# COMMAND ----------

from pyspark.sql.functions import cast

# COMMAND ----------

new_user = user.select('id', 'f_name', 'l_name', col('amount_paid').cast('int')).show(truncate = False)

# COMMAND ----------

## how to get None value from table

# COMMAND ----------

from pyspark.sql.functions import isnan

# COMMAND ----------

user.filter(isnan('amount_paid') == False).show()

# COMMAND ----------

user.filter(isnan('amount_paid') == True).show()

# COMMAND ----------

user.select('id', 'f_name', 'l_name', 'customer_from'). \
   filter(col('customer_from') != 'Akshardham').show()

# COMMAND ----------

user.select('id', 'l_name', 'f_name'). \
   filter("customer_from == 'Akshardham' or customer_from == 'Bramhand'").show() 

# COMMAND ----------

user.select('id', 'f_name', 'l_name').  
   filter(col('last_update').between('2021-01-01','2021-05-01')). \
   show()

# COMMAND ----------

user.select('id','f_name', 'l_name'). \
   filter(col('amount_paid').between(4000, 7000)).show()

# COMMAND ----------

user.filter(col('amount_paid').between(4000 and 7000)).show()

# COMMAND ----------

user.filter('amount_paid BETWEEN 4000 AND 7000').show()

# COMMAND ----------

user.filter(col('customer_from') != ' ').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.where(col('customer_from').isNotNull()).show()

# COMMAND ----------

##Try to find out user's city is null or with empty space.

# COMMAND ----------

user.filter((col('customer_from').isNull()) | (col('customer_from') == '')).show()

# COMMAND ----------

user.where("customer_from == 'Akshardham' OR customer_from == 'Bramhand'").show()

# COMMAND ----------

user.select('id', 'f_name', 'l_name'). \
   filter(col('customer_from').isin('Akshardham','Bramhand')).show()

# COMMAND ----------

user.select('id', 'f_name', 'l_name'). \
   filter("customer_from IN ('Akshardham','Bramhand')").show()

# COMMAND ----------

## Let's learn how to use greater or less than and equal to  

# COMMAND ----------

user.filter(col('amount_paid') > 5000).show()

# COMMAND ----------

user.where("amount_paid > 5000").show()

# COMMAND ----------

user.where("amount_paid Between 2000 and 7000").show()

# COMMAND ----------

user.filter((col('amount_paid') > 2000) & ((col('amount_paid') < 7000) | (col('amount_paid') == 7000))).show()

# COMMAND ----------

user.where(col('amount_paid').isin(2000, 5000)).show()

# COMMAND ----------

## get the users who became customer between 20th Jan 2020 and Feb 2020 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.select('*'). \
   filter((col('start_date').between('2020-01-01', '2020-03-01')) & (col('is_customer') == 'True')).show()

# COMMAND ----------

user.select('id', 'email'). \
   filter((col('is_customer') == False) | (col('last_update')<'2021-07-01')).show()

# COMMAND ----------

user.select('id', 'email'). \
   filter("is_customer = False OR last_update < '2021-07-01'").show() 

# COMMAND ----------


