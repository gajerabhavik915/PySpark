# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

import datetime
courses=[
    {
        'course_id':1,
        'course_title':'Mastering Python',
        'course_published_git':datetime.date(2021,1,14),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':2,
        'course_title':'Data Engineering Essentials',
        'course_published_git':datetime.date(2021,2,10),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':3,
        'course_title':'Basic Python',
        'course_published_git':datetime.date(2021,1,7),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':4,
        'course_title':'AWS Essentials',
        'course_published_git':datetime.date(2021,3,19),
        'is_active':False,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':5,
        'course_title':'Docker 101',
        'course_published_git':datetime.date(2021,2,28),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    }
]

courses_df=spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

users=[
    {
        'user_id':1,
        'user_first_name':'Jaydip',
        'user_last_name':'Dobariya',
        'user_email':'dobariyajaydip@gmail.com'
    },
    {
        'user_id':2,
        'user_first_name':'Vishal',
        'user_last_name':'Barvaliya',
        'user_email':'barvaliyavishal@gmail.com'
    },
    {
        'user_id':3,
        'user_first_name':'Bhavik',
        'user_last_name':'Gajera',
        'user_email':'gajerabhavik@gmail.com'
    },
    {
        'user_id':4,
        'user_first_name':'Dhaval',
        'user_last_name':'Kathiriya',
        'user_email':'kathiriyadhaval@gmail.com'
    },
    {
        'user_id':5,
        'user_first_name':'Meet',
        'user_last_name':'Ambaliya',
        'user_email':'ambaliyameet@gmail.com'
    },
    {
        'user_id':6,
        'user_first_name':'Shyam',
        'user_last_name':'Kaveri',
        'user_email':'kaverishyam@gmail.com'
    },
    {
        'user_id':7,
        'user_first_name':'Krutik',
        'user_last_name':'Shiroya',
        'user_email':'shiroyakrutik@gmail.com'
    },
    {
        'user_id':8,
        'user_first_name':'Jenish',
        'user_last_name':'Thummar',
        'user_email':'thummarjenish@gmail.com'
    },
    {
        'user_id':9,
        'user_first_name':'Sanket',
        'user_last_name':'Bhimani',
        'user_email':'bhimanisanket@gmail.com'
    },
    {
        'user_id':10,
        'user_first_name':'Jay',
        'user_last_name':'Chothani',
        'user_email':'jaychothani@gmail.com'
    }
]
users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

course_enrolments=[
    {
        'course_enrolment_id':1,
        'user_id':10,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':2,
        'user_id':5,
        'course_id':1,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':3,
        'user_id':7,
        'course_id':3,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':4,
        'user_id':9,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':5,
        'user_id':8,
        'course_id':4,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':6,
        'user_id':5,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':7,
        'user_id':4,
        'course_id':1,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':8,
        'user_id':7,
        'course_id':3,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':9,
        'user_id':8,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':10,
        'user_id':3,
        'course_id':4,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':11,
        'user_id':7,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':12,
        'user_id':3,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':13,
        'user_id':5,
        'course_id':1,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':14,
        'user_id':4,
        'course_id':4,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':15,
        'user_id':8,
        'course_id':1,
        'price_paid':9.99
    }
]
course_enrolments_df=spark.createDataFrame([Row(**user) for user in course_enrolments])

# COMMAND ----------

courses_df.show()

# +---------+--------------------+--------------------+---------+--------------------+
# |course_id|        course_title|course_published_git|is_active|     last_updated_ts|
# +---------+--------------------+--------------------+---------+--------------------+
# |        1|    Mastering Python|          2021-01-14|     true|2021-02-02 18:16:...|
# |        2|Data Engineering ...|          2021-02-10|     true|2021-02-02 18:16:...|
# |        3|        Basic Python|          2021-01-07|     true|2021-02-02 18:16:...|
# |        4|      AWS Essentials|          2021-03-19|    false|2021-02-02 18:16:...|
# |        5|          Docker 101|          2021-02-28|     true|2021-02-02 18:16:...|
# +---------+--------------------+--------------------+---------+--------------------+

# COMMAND ----------

course_enrolments_df.show()

# +-------------------+-------+---------+----------+
# |course_enrolment_id|user_id|course_id|price_paid|
# +-------------------+-------+---------+----------+
# |                  1|     10|        2|      9.99|
# |                  2|      5|        1|      9.99|
# |                  3|      7|        3|     10.99|
# |                  4|      9|        2|      9.99|
# |                  5|      8|        4|      9.99|
# |                  6|      5|        5|     10.99|
# |                  7|      4|        1|     10.99|
# |                  8|      7|        3|     10.99|
# |                  9|      8|        5|     10.99|
# |                 10|      3|        4|     10.99|
# |                 11|      7|        5|     10.99|
# |                 12|      3|        2|      9.99|
# |                 13|      5|        1|      9.99|
# |                 14|      4|        4|     10.99|
# |                 15|      8|        1|      9.99|
# +-------------------+-------+---------+----------+

# COMMAND ----------

users_df.show()

# +-------+---------------+--------------+--------------------+
# |user_id|user_first_name|user_last_name|          user_email|
# +-------+---------------+--------------+--------------------+
# |      1|         Jaydip|      Dobariya|dobariyajaydip@gm...|
# |      2|         Vishal|     Barvaliya|barvaliyavishal@g...|
# |      3|         Bhavik|        Gajera|gajerabhavik@gmai...|
# |      4|         Dhaval|     Kathiriya|kathiriyadhaval@g...|
# |      5|           Meet|      Ambaliya|ambaliyameet@gmai...|
# |      6|          Shyam|        Kaveri|kaverishyam@gmail...|
# |      7|         Krutik|       Shiroya|shiroyakrutik@gma...|
# |      8|         Jenish|       Thummar|thummarjenish@gma...|
# |      9|         Sanket|       Bhimani|bhimanisanket@gma...|
# |     10|            Jay|      Chothani|jaychothani@gmail...|
# +-------+---------------+--------------+--------------------+


# COMMAND ----------

## How to know who studying which subjectsa?

# COMMAND ----------

new_table = users_df.join(course_enrolments_df, course_enrolments_df.user_id == users_df.user_id)
# just for checking 
# +-------+---------------+--------------+--------------------+-------------------+-------+---------+----------+
# |user_id|user_first_name|user_last_name|          user_email|course_enrolment_id|user_id|course_id|price_paid|
# +-------+---------------+--------------+--------------------+-------------------+-------+---------+----------+
# |      3|         Bhavik|        Gajera|gajerabhavik@gmai...|                 10|      3|        4|     10.99|
# |      3|         Bhavik|        Gajera|gajerabhavik@gmai...|                 12|      3|        2|      9.99|
# |      4|         Dhaval|     Kathiriya|kathiriyadhaval@g...|                  7|      4|        1|     10.99|
# |      4|         Dhaval|     Kathiriya|kathiriyadhaval@g...|                 14|      4|        4|     10.99|
# |      5|           Meet|      Ambaliya|ambaliyameet@gmai...|                  2|      5|        1|      9.99|
# |      5|           Meet|      Ambaliya|ambaliyameet@gmai...|                  6|      5|        5|     10.99|
# |      5|           Meet|      Ambaliya|ambaliyameet@gmai...|                 13|      5|        1|      9.99|
# |      7|         Krutik|       Shiroya|shiroyakrutik@gma...|                  3|      7|        3|     10.99|
# |      7|         Krutik|       Shiroya|shiroyakrutik@gma...|                  8|      7|        3|     10.99|
# |      7|         Krutik|       Shiroya|shiroyakrutik@gma...|                 11|      7|        5|     10.99|
# |      8|         Jenish|       Thummar|thummarjenish@gma...|                  5|      8|        4|      9.99|
# |      8|         Jenish|       Thummar|thummarjenish@gma...|                  9|      8|        5|     10.99|
# |      8|         Jenish|       Thummar|thummarjenish@gma...|                 15|      8|        1|      9.99|
# |      9|         Sanket|       Bhimani|bhimanisanket@gma...|                  4|      9|        2|      9.99|
# |     10|            Jay|      Chothani|jaychothani@gmail...|                  1|     10|        2|      9.99|
# +-------+---------------+--------------+--------------------+-------------------+-------+---------+----------+


# COMMAND ----------

n_table = courses_df.join(new_table, courses_df.course_id == new_table.course_id).select(courses_df['course_title'], courses_df['is_active'], new_table['user_first_name'])

# +--------------------+---------+---------------+
# |        course_title|is_active|user_first_name|
# +--------------------+---------+---------------+
# |    Mastering Python|     true|         Jenish|
# |    Mastering Python|     true|           Meet|
# |    Mastering Python|     true|           Meet|
# |    Mastering Python|     true|         Dhaval|
# |Data Engineering ...|     true|            Jay|
# |Data Engineering ...|     true|         Sanket|
# |Data Engineering ...|     true|         Bhavik|
# |        Basic Python|     true|         Krutik|
# |        Basic Python|     true|         Krutik|
# |          Docker 101|     true|         Jenish|
# |          Docker 101|     true|         Krutik|
# |          Docker 101|     true|           Meet|
# |      AWS Essentials|    false|         Jenish|
# |      AWS Essentials|    false|         Dhaval|
# |      AWS Essentials|    false|         Bhavik|
# +--------------------+---------+---------------+

# COMMAND ----------

## how many students are in each course?

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

n_table.groupBy('course_title', 'is_active').agg(count('user_first_name').alias('Total_Students')).show()

# +--------------------+---------+--------------+
# |        course_title|is_active|Total_Students|
# +--------------------+---------+--------------+
# |    Mastering Python|     true|             4|
# |Data Engineering ...|     true|             3|
# |        Basic Python|     true|             2|
# |          Docker 101|     true|             3|
# |      AWS Essentials|    false|             3|
# +--------------------+---------+--------------+

# COMMAND ----------


