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

from pyspark.sql.functions import concat_ws

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1.withColumn('name & Age', concat_ws(' ', col('Name'), col('age'))).show()

# COMMAND ----------

from pyspark.sql.functions import upper, lower,initcap, length

# COMMAND ----------

df1.select('id', 'name').\
   withColumn('UpperName', upper(col('name'))). \
   withColumn('lowerName', lower(col('name'))). \
   withColumn('initCapName', initcap(col('name'))). \
   withColumn('legthName', length(col('name'))).show()

# +---+--------+---------+---------+-----------+---------+
# | id|    name|UpperName|lowerName|initCapName|legthName|
# +---+--------+---------+---------+-----------+---------+
# |  1|    Dett|     DETT|     dett|       Dett|        4|
# |  2|   Nern |    NERN |    nern |      Nern |        5|
# |  3| Kallsie|  KALLSIE|  kallsie|    Kallsie|        7|
# |  4|   Siuau|    SIUAU|    siuau|      Siuau|        5|
# |  5|Shennice| SHENNICE| shennice|   Shennice|        8|
# |  6|  Chasse|   CHASSE|   chasse|     Chasse|        6|
# |  7|  Tommye|   TOMMYE|   tommye|     Tommye|        6|
# |  8| Dorcast|  DORCAST|  dorcast|    Dorcast|        7|
# |  9| Angelee|  ANGELEE|  angelee|    Angelee|        7|
# | 10| Willoom|  WILLOOM|  willoom|    Willoom|        7|
# | 11| Waeston|  WAESTON|  waeston|    Waeston|        7|
# | 12|   Rosma|    ROSMA|    rosma|      Rosma|        5|
# | 13|Felisaas| FELISAAS| felisaas|   Felisaas|        8|
# | 14| Demetas|  DEMETAS|  demetas|    Demetas|        7|
# | 15| Jeromyw|  JEROMYW|  jeromyw|    Jeromyw|        7|
# | 16|  Rashid|   RASHID|   rashid|     Rashid|        6|
# | 17|    Dett|     DETT|     dett|       Dett|        4|
# | 18|   Nern |    NERN |    nern |      Nern |        5|
# | 19| Kallsie|  KALLSIE|  kallsie|    Kallsie|        7|
# | 20|   Siuau|    SIUAU|    siuau|      Siuau|        5|
# +---+--------+---------+---------+-----------+---------+
# only showing top 20 rows

# COMMAND ----------

from pyspark.sql.functions import substring, lit

# COMMAND ----------

l = [('l', )]

# COMMAND ----------

df = spark.createDataFrame(l)

# COMMAND ----------

df.select(substring(lit('Hello World'), 1, 5)).show()

# COMMAND ----------


