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
          'courses' : [1,2],
          'amount_paid': 4000,
          'Phone Number' : Row(home_number='234567891', office_number='2345136789'),
          'customer_from': 'Akshardham',
          'start_date': datetime.date(2020,1,1),
          'last_update': datetime.datetime(2021, 1,1,15,0)
         },
         {'id' : 1,
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
          'id' : 3,
          'f_name':'shreeHari',
          'l_name': 'Ghanshyamji',
          'email': 'ghanu@shareehari.com',
          'is_customer': True,
          'courses' : [1,2],
          'amount_paid': 3000,
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

user.show()
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('*').sort('f_name').show()
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('*').orderBy('f_name').show()
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('*').orderBy(user['f_name']).show()
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user.select('*').orderBy(col('f_name')).show()
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('*').sort('f_name', ascending = False).show()

# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('*').sort(user['f_name'].desc()).show()

# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

user.select('*').orderBy(desc('f_name')).show()

# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

from pyspark.sql.functions import col

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
         {'id' : None,
          'f_name':'Hari',
          'l_name': 'Ghanu',
          'email':None,
          'is_customer': True,
          'courses' : [1,2],
          'amount_paid': None,
          'Phone Number' : Row(home_number='234567891', office_number='2345136789'),
          'customer_from': 'Akshardham',
          'start_date': None,
          'last_update': datetime.datetime(2021, 1,1,15,0)
         },
         {'id' : 1,
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
          'id' : None,
          'f_name':None,
          'l_name': None,
          'email': None,
          'is_customer': None,
          'courses' : None,
          'amount_paid': None,
          'Phone Number' : None,
          'customer_from': None,
          'start_date': None,
          'last_update': None
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
          'id' : 3,
          'f_name':'shreeHari',
          'l_name': 'Ghanshyamji',
          'email': 'ghanu@shareehari.com',
          'is_customer': True,
          'courses' : [1,2],
          'amount_paid': 3000,
          'Phone Number' : Row(home_number = None, office_number = None),
          'customer_from': 'Purushotam',
          'start_date': datetime.date(2020,4,1),
          'last_update': datetime.datetime(2021, 5,1,15,0)
         }, 
         {
          'id' : None,
          'f_name':'Ghanashyamay',
          'l_name': None,
          'email': 'ghanuji@ghanashyam.com',
          'is_customer': False,
          'courses' : [1],
          'amount_paid': 8000,
          'Phone Number' : Row(home_number='234867891', office_number='2346656789'),
          'customer_from': None,
          'start_date': datetime.date(2020,2,1),
          'last_update': None
         }, 
         {
          'id' : 3,
          'f_name':'Ghanashyamay',
          'l_name': None,
          'email': 'ghanuji@ghanashyam.com',
          'is_customer': False,
          'courses' : [1],
          'amount_paid': 8000,
          'Phone Number' : Row(home_number='234867891', office_number='2346656789'),
          'customer_from': None,
          'start_date': datetime.date(2020,2,1),
          'last_update': None
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
         }, 
         {
          'id' : None,
          'f_name':'Ghanashyamay',
          'l_name': 'Pandeyji',
          'email': None,
          'is_customer': False,
          'courses' : [1],
          'amount_paid': 8000,
          'Phone Number' : Row(home_number='234867891', office_number='2346656789'),
          'customer_from': None,
          'start_date': datetime.date(2020,2,1),
          'last_update': None
         }
    
]

# COMMAND ----------

user = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

user.show()
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |null|        Hari|      Ghanu|                null|       true| [1, 2]|       null|{234567891, 23451...|   Akshardham|      null|2021-01-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |null|        null|       null|                null|       null|   null|       null|                null|         null|      null|               null|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |null|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   3|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |null|Ghanashyamay|   Pandeyji|                null|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

## How to sort column for getting null value at bottom 

# COMMAND ----------

user.select('*').orderBy(col('id').asc_nulls_last()).show()

# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |   3|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |null|        Hari|      Ghanu|                null|       true| [1, 2]|       null|{234567891, 23451...|   Akshardham|      null|2021-01-01 15:00:00|
# |null|Ghanashyamay|   Pandeyji|                null|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |null|        null|       null|                null|       null|   null|       null|                null|         null|      null|               null|
# |null|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('id').sort(user['id'].desc()).show()

# +----+
# |  id|
# +----+
# |   4|
# |   3|
# |   3|
# |   3|
# |   2|
# |   1|
# |   1|
# |null|
# |null|
# |null|
# |null|
# +----+

# COMMAND ----------

## how to get null values at top.

# COMMAND ----------

user.select('*').sort(col('id').desc_nulls_first()).show()

# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |null|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |null|        null|       null|                null|       null|   null|       null|                null|         null|      null|               null|
# |null|        Hari|      Ghanu|                null|       true| [1, 2]|       null|{234567891, 23451...|   Akshardham|      null|2021-01-01 15:00:00|
# |null|Ghanashyamay|   Pandeyji|                null|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.select('id').sort(user['id'].asc()).show()

# +----+
# |  id|
# +----+
# |null|
# |null|
# |null|
# |null|
# |   1|
# |   1|
# |   2|
# |   3|
# |   3|
# |   3|
# |   4|
# +----+

# COMMAND ----------

## how to sort two column at a time.

# COMMAND ----------

user.select('id', 'f_name').sort('id', 'f_name').show()

# +----+------------+
# |  id|      f_name|
# +----+------------+
# |null|        null|
# |null|Ghanashyamay|
# |null|Ghanashyamay|
# |null|        Hari|
# |   1|        Hari|
# |   1|        Hari|
# |   2|  Ghanashyam|
# |   3|Ghanashyamay|
# |   3|   shreeHari|
# |   3|   shreeHari|
# |   4|      Lalaji|
# +----+------------+

# COMMAND ----------

user.select('id', 'f_name').sort(user['id'].desc(), user['f_name'].asc()).show()

# +----+------------+
# |  id|      f_name|
# +----+------------+
# |   4|      Lalaji|
# |   3|Ghanashyamay|
# |   3|   shreeHari|
# |   3|   shreeHari|
# |   2|  Ghanashyam|
# |   1|        Hari|
# |   1|        Hari|
# |null|        null|
# |null|Ghanashyamay|
# |null|Ghanashyamay|
# |null|        Hari|
# +----+------------+

# COMMAND ----------

user.select('id', 'f_name').sort(col('id').desc_nulls_first(), user['f_name'].asc()).show()

# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

from pyspark.sql.functions import asc

# COMMAND ----------

user.select('id', 'f_name').sort(asc(user['id']), desc(user['f_name'])).show()

# +----+------------+
# |  id|      f_name|
# +----+------------+
# |null|        Hari|
# |null|Ghanashyamay|
# |null|Ghanashyamay|
# |null|        null|
# |   1|        Hari|
# |   1|        Hari|
# |   2|  Ghanashyam|
# |   3|   shreeHari|
# |   3|   shreeHari|
# |   3|Ghanashyamay|
# |   4|      Lalaji|
# +----+------------+

# COMMAND ----------

user.select('id', 'f_name').sort('id', asc('f_name')).show()

# +----+------------+
# |  id|      f_name|
# +----+------------+
# |null|        null|
# |null|Ghanashyamay|
# |null|Ghanashyamay|
# |null|        Hari|
# |   1|        Hari|
# |   1|        Hari|
# |   2|  Ghanashyam|
# |   3|Ghanashyamay|
# |   3|   shreeHari|
# |   3|   shreeHari|
# |   4|      Lalaji|
# +----+------------+

# COMMAND ----------

user.select('f_name','id', 'l_name').sort(['f_name', 'id'], asc = [0,1]).show()

# this one is for [1,0]
# +------------+----+-----------+
# |      f_name|  id|     l_name|
# +------------+----+-----------+
# |        null|null|       null|
# |  Ghanashyam|   2|    Ghanuji|
# |Ghanashyamay|null|   Pandeyji|
# |Ghanashyamay|null|       null|
# |Ghanashyamay|   3|       null|
# |        Hari|null|      Ghanu|
# |        Hari|   1|      Ghanu|
# |        Hari|   1|      Ghanu|
# |      Lalaji|   4| Lalacharan|
# |   shreeHari|   3|Ghanshyamji|
# |   shreeHari|   3|Ghanshyamji|
# +------------+----+-----------+

# COMMAND ----------


