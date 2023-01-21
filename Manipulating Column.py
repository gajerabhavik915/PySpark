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


