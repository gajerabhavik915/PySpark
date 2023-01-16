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

## how to drop duplicate value from dataframe

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

user1 = spark.createDataFrame([Row(**user) for user in users]).show()

# COMMAND ----------

user.distinct().show()

# COMMAND ----------

user.distinct().count()

# COMMAND ----------

user.dropDuplicates(['id']).show()
## This will dropp all the rows where only Id's are same.

# COMMAND ----------

user.dropDuplicates(['id','amount_paid']).show()
#this will drop rows which have same id and amount(e.x. id 1 has both same)

# COMMAND ----------

help(user.dropDuplicates)
# Help on method dropDuplicates in module pyspark.sql.dataframe:

# dropDuplicates(subset: Optional[List[str]] = None) -> 'DataFrame' method of pyspark.sql.dataframe.DataFrame instance
#     Return a new :class:`DataFrame` with duplicate rows removed,
#     optionally only considering certain columns.
    
#     For a static batch :class:`DataFrame`, it just drops duplicate rows. For a streaming
#     :class:`DataFrame`, it will keep all data across triggers as intermediate state to drop
#     duplicates rows. You can use :func:`withWatermark` to limit how late the duplicate data can
#     be and system will accordingly limit the state. In addition, too late data older than
#     watermark will be dropped to avoid any possibility of duplicates.
    
#     :func:`drop_duplicates` is an alias for :func:`dropDuplicates`.
    
#     .. versionadded:: 1.4.0
    
#     Examples
#     --------
#     >>> from pyspark.sql import Row
#     >>> df = sc.parallelize([ \
#     ...     Row(name='Alice', age=5, height=80), \
#     ...     Row(name='Alice', age=5, height=80), \
#     ...     Row(name='Alice', age=10, height=80)]).toDF()
#     >>> df.dropDuplicates().show()
#     +-----+---+------+
#     | name|age|height|
#     +-----+---+------+
#     |Alice|  5|    80|
#     |Alice| 10|    80|
#     +-----+---+------+
    
#     >>> df.dropDuplicates(['name', 'height']).show()
#     +-----+---+------+
#     | name|age|height|
#     +-----+---+------+
#     |Alice|  5|    80|
#     +-----+---+------+

# Command took 0.13 seconds -- by c0851337@mylambton.ca at 1/16/2023, 11:26:27 AM on Bhavikkumar Gajera's Cluster
# 1
 

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
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+

# COMMAND ----------

user.na.drop().show()
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

user.dropna().show()
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

user.na.drop('any').show()
# this will drop all the rows which contain even a single 'NULL', 

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

user.dropna('all').show()
# this will drop rows which contain only null value  
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |null|        Hari|      Ghanu|                null|       true| [1, 2]|       null|{234567891, 23451...|   Akshardham|      null|2021-01-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |null|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.dropna(subset = 'id').show()
#it will only delete that row where mentioned column contain null value

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

user.dropna(thresh=2).show()
# This will only drop those rows which will have less than 2 non-null values.
# e.x. - here, in second row, we have more than 2 non-null values. so, it didn't drop. 
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |null|        Hari|      Ghanu|                null|       true| [1, 2]|       null|{234567891, 23451...|   Akshardham|      null|2021-01-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |null|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+

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

user.dropna(how = 'all', subset = ['id','email']).show()
# it will drop rows which contains all nulls and also drop rows which have null values on id and email column.
# e.x. here row no.6 didn't get drop as it doesn't contain null on email column.

# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |   2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |   3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |null|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   3|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |   4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +----+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

user.dropna(how = 'any', subset = ['id', 'email']).show()
# here, in any mentioned column, if single column contains null values, then it will delete that row.
# +---+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# | id|      f_name|     l_name|               email|is_customer|courses|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+
# |  1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  1|        Hari|      Ghanu|      ghanu@hari.com|       true| [1, 2]|       4000|{234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  2|  Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|     Bramhand|2020-02-01|2021-02-01 15:00:00|
# |  3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       7000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3|   shreeHari|Ghanshyamji|ghanu@shareehari.com|       true| [1, 2]|       3000|        {null, null}|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  3|Ghanashyamay|       null|ghanuji@ghanashya...|      false|    [1]|       8000|{234867891, 23466...|         null|2020-02-01|               null|
# |  4|      Lalaji| Lalacharan| ghanu@lalsharan.com|      false|    [1]|       3000|{345678915, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +---+------------+-----------+--------------------+-----------+-------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------


