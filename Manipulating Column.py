# Databricks notebook source
spark

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/c0851337@mylambton.ca/sample_data.csv")

# COMMAND ----------

df1.show()

# COMMAND ----------

from pyspark.sql.functions import to_date

# COMMAND ----------

# how to change column type from string to INT.
df1 = df1.withColumn('new_date', to_date('DATE ', 'dd/MM/yyyy'))

# how to change column type from string to INT or from string to INT.
# df1.withcolumn('hsg', col('hsfg').cast())

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

df1.select('*', date_format('new_date', 'yyyyMM').alias('Year_Month')).drop('DATE ').show()

# +---+--------+------+---+-------------+----------+----------+
# | ID|    NAME|GENDER|AGE|      COUNTRY|  new_date|Year_Month|
# +---+--------+------+---+-------------+----------+----------+
# |  1|    Dett|  Male| 18|Great Britain|2015-05-21|    201505|
# |  2|   Nern |Female| 19|       France|2017-10-15|    201710|
# |  3| Kallsie|  Male| 20|       France|2016-08-16|    201608|
# |  4|   Siuau|Female| 21|Great Britain|2015-05-21|    201505|
# |  5|Shennice|  Male| 22|       France|2016-05-21|    201605|
# |  6|  Chasse|Female| 23|       France|2018-10-15|    201810|
# |  7|  Tommye|  Male| 24|United States|2017-08-16|    201708|
# |  8| Dorcast|Female| 25|United States|2016-05-21|    201605|
# |  9| Angelee|  Male| 26|Great Britain|2017-05-21|    201705|
# | 10| Willoom|Female| 27|       France|2019-10-15|    201910|
# | 11| Waeston|  Male| 28|Great Britain|2018-08-16|    201808|
# | 12|   Rosma|Female| 29|       France|2017-05-21|    201705|
# | 13|Felisaas|  Male| 30|       France|2018-05-21|    201805|
# | 14| Demetas|Female| 31|Great Britain|2020-10-15|    202010|
# | 15| Jeromyw|Female| 32|       France|2019-08-16|    201908|
# | 16|  Rashid|Female| 33|       France|2018-05-21|    201805|
# | 17|    Dett|Female| 34|United States|2019-05-21|    201905|
# | 18|   Nern |Female| 35|United States|2021-10-15|    202110|
# | 19| Kallsie|Female| 36|Great Britain|2020-08-16|    202008|
# | 20|   Siuau|Female| 37|       France|2019-05-21|    201905|
# +---+--------+------+---+-------------+----------+----------+

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

df1.groupBy(date_format('new_date', 'yyyyMM').alias('Year_month')).agg(count('new_date')).orderBy('Year_month').show()


# +----------+---------------+
# |Year_month|count(new_date)|
# +----------+---------------+
# |    201505|             66|
# |    201605|             66|
# |    201608|             33|
# |    201705|             66|
# |    201708|             33|
# |    201710|             33|
# |    201805|             66|
# |    201808|             33|
# |    201810|             33|
# |    201905|             98|
# |    201908|             33|
# |    201910|             33|
# |    202005|            130|
# |    202008|             65|
# |    202010|             33|
# |    202105|            130|
# |    202108|             65|
# |    202110|             65|
# |    202205|            130|
# |    202208|             65|
# +----------+---------------+

# COMMAND ----------

## How to create Dummy dataframe

# COMMAND ----------

l = [('l', )]

# COMMAND ----------

df = spark.createDataFrame(l)

# COMMAND ----------

df.show()

# +---+
# | _1|
# +---+
# |  l|
# +---+

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

df.select(current_date()).show()

# +--------------+
# |current_date()|
# +--------------+
# |    2023-01-21|
# +--------------+

# COMMAND ----------

df.select(current_date().alias('Ajani_Tarikh')).show()

# +------------+
# |Ajani_Tarikh|
# +------------+
# |  2023-01-21|
# +------------+

# COMMAND ----------


df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/c0851337@mylambton.ca/sample_data.csv")

# COMMAND ----------

df1.show()

# +---+--------+------+---+----------+-------------+
# | ID|    NAME|GENDER|AGE|     DATE |      COUNTRY|
# +---+--------+------+---+----------+-------------+
# |  1|    Dett|  Male| 18|21/05/2015|Great Britain|
# |  2|   Nern |Female| 19|15/10/2017|       France|
# |  3| Kallsie|  Male| 20|16/08/2016|       France|
# |  4|   Siuau|Female| 21|21/05/2015|Great Britain|
# |  5|Shennice|  Male| 22|21/05/2016|       France|
# |  6|  Chasse|Female| 23|15/10/2018|       France|
# |  7|  Tommye|  Male| 24|16/08/2017|United States|
# |  8| Dorcast|Female| 25|21/05/2016|United States|
# |  9| Angelee|  Male| 26|21/05/2017|Great Britain|
# | 10| Willoom|Female| 27|15/10/2019|       France|
# | 11| Waeston|  Male| 28|16/08/2018|Great Britain|
# | 12|   Rosma|Female| 29|21/05/2017|       France|
# | 13|Felisaas|  Male| 30|21/05/2018|       France|
# | 14| Demetas|Female| 31|15/10/2020|Great Britain|
# | 15| Jeromyw|Female| 32|16/08/2019|       France|
# | 16|  Rashid|Female| 33|21/05/2018|       France|
# | 17|    Dett|Female| 34|21/05/2019|United States|
# | 18|   Nern |Female| 35|15/10/2021|United States|
# | 19| Kallsie|Female| 36|16/08/2020|Great Britain|
# | 20|   Siuau|Female| 37|21/05/2019|       France|
# +---+--------+------+---+----------+-------------+

# COMMAND ----------

from pyspark.sql.functions import lower

# COMMAND ----------

df1.select('id', lower('name')).show()

# +---+-----------+
# | id|lower(name)|
# +---+-----------+
# |  1|       dett|
# |  2|      nern |
# |  3|    kallsie|
# |  4|      siuau|
# |  5|   shennice|
# |  6|     chasse|
# |  7|     tommye|
# |  8|    dorcast|
# |  9|    angelee|
# | 10|    willoom|
# | 11|    waeston|
# | 12|      rosma|
# | 13|   felisaas|
# | 14|    demetas|
# | 15|    jeromyw|
# | 16|     rashid|
# | 17|       dett|
# | 18|      nern |
# | 19|    kallsie|
# | 20|      siuau|
# +---+-----------+
# only showing top 20 rows

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, upper

# COMMAND ----------

df1.select(upper('NAME').alias('name')).show()

# +--------+
# |    name|
# +--------+
# |    DETT|
# |   NERN |
# | KALLSIE|
# |   SIUAU|
# |SHENNICE|
# |  CHASSE|
# |  TOMMYE|
# | DORCAST|
# | ANGELEE|
# | WILLOOM|
# | WAESTON|
# |   ROSMA|
# |FELISAAS|
# | DEMETAS|
# | JEROMYW|
# |  RASHID|
# |    DETT|
# |   NERN |
# | KALLSIE|
# |   SIUAU|
# +--------+
# only showing top 20 rows

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

df1.groupBy(upper('NAME')).agg(count('NAME')).show()

# +-----------+-----------+
# |upper(NAME)|count(NAME)|
# +-----------+-----------+
# |     CHASSE|        162|
# |    KALLSIE|        162|
# |      ROSMA|        162|
# |    DEMETAS|        162|
# |   SHENNICE|        162|
# |    JEROMYW|        162|
# |     RASHID|        162|
# |      SIUAU|        162|
# |    WILLOOM|        162|
# |    WAESTON|        162|
# |       DETT|        131|
# |    DORCAST|        162|
# |     TOMMYE|        162|
# |   FELISAAS|        162|
# |    ANGELEE|        162|
# |      NERN |        162|
# +-----------+-----------+

# COMMAND ----------



# COMMAND ----------

df1.orderBy(upper('NAME').desc()).show()

# COMMAND ----------

df1.select('id', concat(col('name'), lit(' '), col('age'))).show()

# +---+--------------------+
# | id|concat(name,  , age)|
# +---+--------------------+
# |  1|             Dett 18|
# |  2|            Nern  19|
# |  3|          Kallsie 20|
# |  4|            Siuau 21|
# |  5|         Shennice 22|
# |  6|           Chasse 23|
# |  7|           Tommye 24|
# |  8|          Dorcast 25|
# |  9|          Angelee 26|
# | 10|          Willoom 27|
# | 11|          Waeston 28|
# | 12|            Rosma 29|
# | 13|         Felisaas 30|
# | 14|          Demetas 31|
# | 15|          Jeromyw 32|
# | 16|           Rashid 33|
# | 17|             Dett 34|
# | 18|            Nern  35|
# | 19|          Kallsie 36|
# | 20|            Siuau 37|
# +---+--------------------+
# only showing top 20 rows

# COMMAND ----------

df1.select('id', col('age')*lit('2')).show()

# +---+---------+
# | id|(age * 2)|
# +---+---------+
# |  1|     36.0|
# |  2|     38.0|
# |  3|     40.0|
# |  4|     42.0|
# |  5|     44.0|
# |  6|     46.0|
# |  7|     48.0|
# |  8|     50.0|
# |  9|     52.0|
# | 10|     54.0|
# | 11|     56.0|
# | 12|     58.0|
# | 13|     60.0|
# | 14|     62.0|
# | 15|     64.0|
# | 16|     66.0|
# | 17|     68.0|
# | 18|     70.0|
# | 19|     72.0|
# | 20|     74.0|
# +---+---------+
# only showing top 20 rows

# COMMAND ----------

from pyspark.sql.functions import substring, col, lit

# COMMAND ----------

l = [('l', )]

# COMMAND ----------

df = spark.createDataFrame(l)

# COMMAND ----------

df.select(substring(lit('Hello World'), -2, 6)).show()

# +-----------------------------+
# |substring(Hello World, -2, 6)|
# +-----------------------------+
# |                           ld|
# +-----------------------------+

# COMMAND ----------

df.select(substring(lit('9057812974'), 6, 7)).show()

# +---------------------------+
# |substring(9057812974, 6, 7)|
# +---------------------------+
# |                      12974|
# +---------------------------+

# COMMAND ----------


df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/c0851337@mylambton.ca/sample_data.csv")

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.select('id', 'name'). \
   withColumn('name', substring(col('NAME'), 1, 3)).show()

# +---+----+
# | id|name|
# +---+----+
# |  1| Det|
# |  2| Ner|
# |  3| Kal|
# |  4| Siu|
# |  5| She|
# |  6| Cha|
# |  7| Tom|
# |  8| Dor|
# |  9| Ang|
# | 10| Wil|
# | 11| Wae|
# | 12| Ros|
# | 13| Fel|
# | 14| Dem|
# | 15| Jer|
# | 16| Ras|
# | 17| Det|
# | 18| Ner|
# | 19| Kal|
# | 20| Siu|
# +---+----+
# only showing top 20 rows

# COMMAND ----------

from pyspark.sql.functions import split, lit, explode, col

# COMMAND ----------

l = [('l', )]

# COMMAND ----------

df = spark.createDataFrame(l)

# COMMAND ----------

df.show()

# +---+
# | _1|
# +---+
# |  l|
# +---+

# COMMAND ----------

df.select(split(lit("how are you today"), " ")).show()

# +-------------------------------+
# |split(how are you today,  , -1)|
# +-------------------------------+
# |           [how, are, you, t...|
# +-------------------------------+

# COMMAND ----------

df.select(explode(split(lit('How are you today'), ' '))).show()

# +-----+
# |  col|
# +-----+
# |  How|
# |  are|
# |  you|
# |today|
# +-----+

# COMMAND ----------

df1.show()

# +---+--------+------+---+----------+-------------+
# | ID|    NAME|GENDER|AGE|     DATE |      COUNTRY|
# +---+--------+------+---+----------+-------------+
# |  1|    Dett|  Male| 18|21/05/2015|Great Britain|
# |  2|   Nern |Female| 19|15/10/2017|       France|
# |  3| Kallsie|  Male| 20|16/08/2016|       France|
# |  4|   Siuau|Female| 21|21/05/2015|Great Britain|
# |  5|Shennice|  Male| 22|21/05/2016|       France|
# |  6|  Chasse|Female| 23|15/10/2018|       France|
# |  7|  Tommye|  Male| 24|16/08/2017|United States|
# |  8| Dorcast|Female| 25|21/05/2016|United States|
# |  9| Angelee|  Male| 26|21/05/2017|Great Britain|
# | 10| Willoom|Female| 27|15/10/2019|       France|

# COMMAND ----------

df1.select('id', explode(split(col('date '), '/'))).show()

# +---+----+
# | id| col|
# +---+----+
# |  1|  21|
# |  1|  05|
# |  1|2015|
# |  2|  15|
# |  2|  10|
# |  2|2017|
# |  3|  16|
# |  3|  08|
# |  3|2016|
# |  4|  21|
# |  4|  05|
# |  4|2015|
# |  5|  21|
# |  5|  05|
# |  5|2016|
# |  6|  15|
# |  6|  10|
# |  6|2018|
# |  7|  16|
# |  7|  08|
# +---+----+
# only showing top 20 rows

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

user = spark.createDataFrame([Row(**user) for user in users])

# +---+----------+-----------+--------------------+-----------+-----------+--------------------+-------------+----------+-------------------+
# | id|    f_name|     l_name|               email|is_customer|amount_paid|        Phone Number|customer_from|start_date|        last_update|
# +---+----------+-----------+--------------------+-----------+-----------+--------------------+-------------+----------+-------------------+
# |  1|      Hari|      Ghanu|      ghanu@hari.com|       true|       4000|[234567891, 23451...|   Akshardham|2020-01-01|2021-01-01 15:00:00|
# |  2|Ghanashyam|    Ghanuji|ghanuji@ghanashya...|      false|       8000|[234567891, 23451...|    Brahmhand|2020-02-01|2021-02-01 15:00:00|
# |  3| shreeHari|Ghanshyamji|ghanu@shareehari.com|       true|       7000|                null|   Purushotam|2020-04-01|2021-05-01 15:00:00|
# |  4|    Lalaji| Lalacharan| ghanu@lalsharan.com|      false|       3000|[234567891, 23451...|  AksharOradi|2020-08-01|2021-09-01 15:00:00|
# +---+----------+-----------+--------------------+-----------+-----------+--------------------+-------------+----------+-------------------+


# COMMAND ----------

from pyspark.sql.functions import explode, split, col

# COMMAND ----------

user.select('id', 'phone number').show()

# COMMAND ----------

users = user.select('id', 'phone number'). \
   withColumn('splitnumber', explode('phone number')). \
   drop('phone number')

# +---+-----------+
# | id|splitnumber|
# +---+-----------+
# |  1|  234567891|
# |  1| 2345136789|
# |  2|  234567891|
# |  2| 2345136789|
# |  4|  234567891|
# |  4| 2345136789|
# +---+-----------+

# COMMAND ----------

from pyspark.sql.functions import substring

# COMMAND ----------

users.select('id', 'splitnumber'). \
   withColumn('first3NUM', substring('splitnumber', 1,3)). \
   withColumn('last4NUM', substring('splitnumber', -4, 4)).show()

# +---+-----------+---------+--------+
# | id|splitnumber|first3NUM|last4NUM|
# +---+-----------+---------+--------+
# |  1|  234567891|      234|    7891|
# |  1| 2345136789|      234|    6789|
# |  2|  234567891|      234|    7891|
# |  2| 2345136789|      234|    6789|
# |  4|  234567891|      234|    7891|
# |  4| 2345136789|      234|    6789|
# +---+-----------+---------+--------+


# COMMAND ----------

user.show()

# COMMAND ----------

from pyspark.sql.functions import concat, lpad, rpad, lit

# COMMAND ----------

users = user.select(
           concat(rpad('f_name', 5, '0'), 
                  lit(' '),
                  rpad('l_name', 5, '-'),
                  rpad('email', 5, '-')).alias('new_column'))

# +--------------------------------------------------------------------+
# |concat(rpad(f_name, 5, 0),  , rpad(l_name, 5, -), rpad(email, 5, -))|
# +--------------------------------------------------------------------+
# |                                                    Hari0 Ghanughanu|
# |                                                    Ghana Ghanughanu|
# |                                                    shree Ghansghanu|
# |                                                    Lalaj Lalacghanu|
# +--------------------------------------------------------------------+

# COMMAND ----------

from pyspark.sql.functions import ltrim, rtrim, trim

# COMMAND ----------

users.select(trim('new_column'), rtrim('new_column')).show()

# +----------------+-----------------+
# |trim(new_column)|rtrim(new_column)|
# +----------------+-----------------+
# |Hari0 Ghanughanu| Hari0 Ghanughanu|
# |Ghana Ghanughanu| Ghana Ghanughanu|
# |shree Ghansghanu| shree Ghansghanu|
# |Lalaj Lalacghanu| Lalaj Lalacghanu|
# +----------------+-----------------+


# COMMAND ----------

l = [('    hello    ', )]

# COMMAND ----------

l=spark.createDataFrame(l).toDF('l')

# COMMAND ----------

l.withColumn('allTrimmed', trim(col('l'))).withColumn('leftTrimmed', ltrim(col('l'))).withColumn('rightTrimmed', rtrim(col('l'))).show()

# +-------------+----------+-----------+------------+
# |            l|allTrimmed|leftTrimmed|rightTrimmed|
# +-------------+----------+-----------+------------+
# |    hello    |     hello|  hello    |       hello|
# +-------------+----------+-----------+------------+


# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

# COMMAND ----------

l = [('l', )]

# COMMAND ----------

l = spark.createDataFrame(l).toDF('dummy')

# COMMAND ----------

l.select(to_date(lit('20201004'), 'yyyyMMdd')).show()

# COMMAND ----------

l.select(current_date()).show()

# COMMAND ----------

l.select(current_timestamp()).show()

# COMMAND ----------

import datetime

# COMMAND ----------

users = [{'id' : 1,
          'f_name':'Hari',
          'l_name': 'Ghanu',
          'email': 'ghanu@hari.com',
          'is_customer': True,
          'amount_paid': 4000,
          'Phone Number' : '234567891',
          'customer_from': 'Akshardham',
          'start_date': '20200101',
          'last_update': datetime.datetime(2021, 1,1,15,0)
         },
         {
          'id' : 2,
          'f_name':'Ghanashyam',
          'l_name': 'Ghanuji',
          'email': 'ghanuji@ghanashyam.com',
          'is_customer': False,
          'amount_paid': 8000,
          'Phone Number' : '23456432591',
          'customer_from': 'Brahmhand',
          'start_date': '20200201',
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
          'start_date': '20200301',
          'last_update': datetime.datetime(2021, 5,1,15,0)
         },
         {
             'id' : 4,
          'f_name':'Lalaji',
          'l_name': 'Lalacharan',
          'email': 'ghanu@lalsharan.com',
          'is_customer': False,
          'amount_paid': 3000,
          'Phone Number' : '23456762491',
          'customer_from': 'AksharOradi',
          'start_date': '20200501',
          'last_update': datetime.datetime(2021,9,1,15,0)
         }
    
]

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

user = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

## how to convert string to date time formate

# COMMAND ----------

from pyspark.sql.functions import to_date, col


# COMMAND ----------

date = user.select('id', 'start_date').withColumn('new_date', to_date(col('start_date'), 'yyyyMMdd'))

# +---+----------+----------+
# | id|start_date|  new_date|
# +---+----------+----------+
# |  1|  20200101|2020-01-01|
# |  2|  20200201|2020-02-01|
# |  3|  20200301|2020-03-01|
# |  4|  20200501|2020-05-01|
# +---+----------+----------+

# COMMAND ----------


from pyspark.sql.functions import datediff, date_add, date_sub

# COMMAND ----------

date.select('id', 'new_date'). \
   withColumn('add_date', date_add('new_date', 10)). \
   withColumn('add1_date', date_sub('new_date', 10)).show()

# +---+----------+----------+----------+
# | id|  new_date|  add_date| add1_date|
# +---+----------+----------+----------+
# |  1|2020-01-01|2020-01-11|2019-12-22|
# |  2|2020-02-01|2020-02-11|2020-01-22|
# |  3|2020-03-01|2020-03-11|2020-02-20|
# |  4|2020-05-01|2020-05-11|2020-04-21|
# +---+----------+----------+----------+

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

date.select('id', 'new_date'). \
   withColumn('date_difference', datediff(current_date(), 'new_date')).show()

# +---+----------+---------------+
# | id|  new_date|date_difference|
# +---+----------+---------------+
# |  1|2020-01-01|           1118|
# |  2|2020-02-01|           1087|
# |  3|2020-03-01|           1058|
# |  4|2020-05-01|            997|
# +---+----------+---------------+

# COMMAND ----------

from pyspark.sql.functions import months_between, add_months,round, cast

# COMMAND ----------

date.show()

# COMMAND ----------

date.select('id', 'new_date'). \
   withColumn('monthsDiff', round(months_between(current_date(), 'new_date'), 2)). \
   withColumn('monthsAdd', (add_months('new_date', 1))).show()

# +---+----------+----------+----------+
# | id|  new_date|monthsDiff| monthsAdd|
# +---+----------+----------+----------+
# |  1|2020-01-01|     36.71|2020-02-01|
# |  2|2020-02-01|     35.71|2020-03-01|
# |  3|2020-03-01|     34.71|2020-04-01|
# |  4|2020-05-01|     32.71|2020-06-01|
# +---+----------+----------+----------+

# COMMAND ----------

from pyspark.sql.functions import trunc

# COMMAND ----------

date.show()

# COMMAND ----------

date.select('id', 'new_date'). \
   withColumn('first_MonthDate', trunc('new_date', 'MM')). \
   withColumn('first_YearDate', trunc('new_date', 'yyyy')).show()
# +---+----------+---------------+--------------+
# | id|  new_date|first_MonthDate|first_YearDate|
# +---+----------+---------------+--------------+
# |  1|2020-01-01|     2020-01-01|    2020-01-01|
# |  2|2020-02-01|     2020-02-01|    2020-01-01|
# |  3|2020-03-01|     2020-03-01|    2020-01-01|
# |  4|2020-05-01|     2020-05-01|    2020-01-01|
# +---+----------+---------------+--------------+

# COMMAND ----------

date.select('id', 'new_date', trunc('new_date', 'MM').alias('first_MonthDate')).show()

# +---+----------+---------------+
# | id|  new_date|first_MonthDate|
# +---+----------+---------------+
# |  1|2020-01-01|     2020-01-01|
# |  2|2020-02-01|     2020-02-01|
# |  3|2020-03-01|     2020-03-01|
# |  4|2020-05-01|     2020-05-01|
# +---+----------+---------------+

# COMMAND ----------

from pyspark.sql.functions import date_trunc

# COMMAND ----------

date.select('id', 'new_date'). \
   withColumn('willReturnWIth_TimeStamp_month', date_trunc('MM', 'new_date')). \
   withColumn('willReturnWIth_TimeStamp_year', date_trunc('yyyy', 'new_date')).show()

# COMMAND ----------

from pyspark.sql.functions import weekofyear, dayofyear, dayofmonth, dayofweek

# COMMAND ----------

date.select('id', 'new_date', month('new_date').alias('Abstrected_month'), year('new_date'), weekofyear('new_date'), dayofyear('new_date'), dayofmonth('new_date'), dayofweek('new_date')).show()

# +---+----------+----------------+--------------+--------------------+-------------------+--------------------+-------------------+
# | id|  new_date|Abstrected_month|year(new_date)|weekofyear(new_date)|dayofyear(new_date)|dayofmonth(new_date)|dayofweek(new_date)|
# +---+----------+----------------+--------------+--------------------+-------------------+--------------------+-------------------+
# |  1|2020-01-01|               1|          2020|                   1|                  1|                   1|                  4|
# |  2|2020-02-01|               2|          2020|                   5|                 32|                   1|                  7|
# |  3|2020-03-01|               3|          2020|                   9|                 61|                   1|                  1|
# |  4|2020-05-01|               5|          2020|                  18|                122|                   1|                  6|
# +---+----------+----------------+--------------+--------------------+-------------------+--------------------+-------------------+


# COMMAND ----------

date.show()

# COMMAND ----------

from pyspark.sql.functions import to_date, to_timestamp, lit

# COMMAND ----------

date.select('id', to_date('start_date', 'yyyyMMdd').alias('new_date1')).show()

# COMMAND ----------

l = [('l', )]

# COMMAND ----------

l = spark.createDataFrame(l).toDF('dummy')

# COMMAND ----------

l.select(to_date(lit('02-March-2014'), 'dd-MMMM-yyyy')).show()

# +------------------------------------+
# |to_date(02-March-2014, dd-MMMM-yyyy)|
# +------------------------------------+
# |                          2014-03-02|
# +------------------------------------+

# COMMAND ----------

l.select(to_timestamp(lit('02-March-2014 10:12:45'), 'dd-MMMM-yyyy HH:mm:ss')).show()

# +-----------------------------------------------------------+
# |to_timestamp(02-March-2014 10:12:45, dd-MMMM-yyyy HH:mm:ss)|
# +-----------------------------------------------------------+
# |                                        2014-03-02 10:12:45|
# +-----------------------------------------------------------+

# COMMAND ----------

date.show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

date.select('id', 'new_date'). \
   withColumn('onlyYear&Month', date_format('new_date', 'yyyyMM')).show()

# +---+----------+--------------+
# | id|  new_date|onlyYear&Month|
# +---+----------+--------------+
# |  1|2020-01-01|        202001|
# |  2|2020-02-01|        202002|
# |  3|2020-03-01|        202003|
# |  4|2020-05-01|        202005|
# +---+----------+--------------+

# COMMAND ----------

from pyspark.sql.functions import cast

# COMMAND ----------

date1 = date.select('id', date_format('new_date', 'yyyyD').cast('int').alias('YearAndDayofYear')).show()

# +---+----------------+
# | id|YearAndDayofYear|
# +---+----------------+
# |  1|           20201|
# |  2|          202032|
# |  3|          202061|
# |  4|         2020122|
# +---+----------------+


# COMMAND ----------

from pyspark.sql.functions import substring 

# COMMAND ----------


