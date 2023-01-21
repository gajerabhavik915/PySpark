# Databricks notebook source
spark

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

df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

df1.select(count('*')).show()

# +--------+
# |count(1)|
# +--------+
# |    2561|
# +--------+

# COMMAND ----------

# how to know the number of male and female in table

# COMMAND ----------

df1.groupBy('GENDER').agg(count('NAME')).show()

# +------+-----------+
# |GENDER|count(NAME)|
# +------+-----------+
# |Female|       2330|
# |  Male|        231|
# +------+-----------+

# COMMAND ----------

## How to know number of country with number of male and female

# COMMAND ----------

df1.groupBy('country', 'gender').agg(count('gender')).show()

# +-------------+------+-------------+
# |      country|gender|count(gender)|
# +-------------+------+-------------+
# |       France|  Male|           99|
# |       France|Female|         1166|
# |Great Britain|Female|          647|
# |Great Britain|  Male|           99|
# |United States|  Male|           33|
# |United States|Female|          517|
# +-------------+------+-------------+

# COMMAND ----------

new_df = df1.groupBy('country', 'gender').agg(count('gender'))

# COMMAND ----------

from pyspark.sql.functions import max

# COMMAND ----------

new_df.groupBy('gender').agg(max('count(gender)')).alias('max_value').show()

# +------+------------------+
# |gender|max(count(gender))|
# +------+------------------+
# |Female|              1166|
# |  Male|                99|
# +------+------------------+


# COMMAND ----------

## how to count perticular country's participants ( mean like 'where' with 'Group by')

# COMMAND ----------

df1.filter("country = 'Great Britain'").groupBy('gender').agg(count('gender').alias('Total_For_Great_Britain')).show()

# +------+-----------------------+
# |gender|Total_For_Great_Britain|
# +------+-----------------------+
# |Female|                    647|
# |  Male|                     99|
# +------+-----------------------+

# COMMAND ----------

## Now, noramlly perform few Aggregations 

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.filter("name = 'Dett'").select(count('name').alias('Having Dett name')).show()

# +----------------+
# |Having Dett name|
# +----------------+
# |             131|
# +----------------+

# COMMAND ----------

## how many country are having "Dett" and which one has maximum "dett" name people

# COMMAND ----------

df1.filter("name = 'Dett'").groupBy('country', 'name').agg(count('name')).show()

# +-------------+----+-----------+
# |      country|name|count(name)|
# +-------------+----+-----------+
# |Great Britain|Dett|         33|
# |United States|Dett|         33|
# |       France|Dett|         65|
# +-------------+----+-----------+

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

 df1.count()

# COMMAND ----------

df1.select(count('*')).show()

# COMMAND ----------

## how to lower all alphabets 

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


