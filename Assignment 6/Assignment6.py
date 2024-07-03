from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create SparkSession
spark = SparkSession.builder \
            .getOrCreate()


wordsPath = '/FileStore/tables'
df = spark.read.option('header','True').option('inferSchema','true').csv(wordsPath)
df.printSchema()

display(df)

df.createOrReplaceTempView('v_Fire_df')

%sql
# 1. How many distinct types of calls were made to the fire department?
SELECT COUNT(DISTINCT `Call Type`) FROM v_Fire_df WHERE `Call Type` IS NOT NULL;

%sql
# 2. What are distinct types of calls made to the fire department?
SELECT DISTINCT `Call Type` FROM v_fire_df WHERE `Call Type` IS NOT NULL limit 10;

+-----------------------------+
|CallType                     |
+-----------------------------+
|Alarms                       |
|Odor (Strange / Unknown)     |
|Citizen Assist / Service Call|
|Vehicle Fire                 |
|Other                        |
|Outside Fire                 |
|Electrical Hazard            |
|Structure Fire               |
|Medical Incident             |
|Fuel Spill                   |
+-----------------------------+
only showing top 10 rows


# 3. Find out all responses for delayed times greater than 5 mins?

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
new_fire_df.select("ResponseDelayedinMins").where(col("ResponseDelayedinMins") > 5).show(5, False)
+---------------------+
|ResponseDelayedinMins|
+---------------------+
|5.233333             |
|6.9333334            |
|6.116667             |
|7.85                 |
|77.333336            |
+---------------------+
only showing top 5 rows

# 4. What were the most common call types?

(fire_ts_df
 .select("CallType").where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))
+-------------------------------+-------+
|CallType                       |count  |
+-------------------------------+-------+
|Medical Incident               |2843475|
|Structure Fire                 |578998 |
|Alarms                         |483518 |
|Traffic Collision              |175507 |
|Citizen Assist / Service Call  |65360  |
|Other                          |56961  |
|Outside Fire                   |51603  |
|Vehicle Fire                   |20939  |
|Water Rescue                   |20037  |
|Gas Leak (Natural and LP Gases)|17284  |
+-------------------------------+-------+
only showing top 10 rows

# 5. What zip codes accounted for the most common calls?

display(fire_ts_df
 .select("CallType", "ZipCode")
 .where(col("CallType").isNotNull())
 .groupBy("CallType", "Zipcode")
 .count()
 .orderBy("count", ascending=False))

# 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

display(fire_ts_df.select("Neighborhood", "Zipcode").where((col("Zipcode") == 94102) | (col("Zipcode") == 94103)).distinct())

# 7. What was the sum of all calls, average, min, and max of the call response times?

fire_ts_df.select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins")).show()
+--------------+--------------------------+--------------------------+--------------------------+
|sum(NumAlarms)|avg(ResponseDelayedinMins)|min(ResponseDelayedinMins)|max(ResponseDelayedinMins)|
+--------------+--------------------------+--------------------------+--------------------------+
|       4403441|         3.902170335891614|               0.016666668|                 1879.6167|
+--------------+--------------------------+--------------------------+--------------------------+

# 8. How many distinct years of data are in the CSV le?

fire_ts_df.select(year('IncidentDate')).distinct().orderBy(year('IncidentDate')).show()
+------------------+
|year(IncidentDate)|
+------------------+
|              2000|
|              2001|
|              2002|
|              2003|
|              2004|
|              2005|
|              2006|
|              2007|
|              2008|
|              2009|
|              2010|
|              2011|
|              2012|
|              2013|
|              2014|
|              2015|
|              2016|
|              2017|
+------------------+

# 9. What week of the year in 2018 had the most re calls?

fire_ts_df.filter(year('IncidentDate') == 2018).groupBy(weekofyear('IncidentDate')).count().orderBy('count', ascending=False).show()
+------------------------+-----+
|weekofyear(IncidentDate)|count|
+------------------------+-----+
|                       1| 6401|
|                      25| 6163|
|                      13| 6103|
|                      22| 6060|
|                      44| 6048|
|                      27| 6042|
|                      16| 6009|
|                      40| 6000|
|                      43| 5986|
|                       5| 5946|
|                       2| 5929|
|                      18| 5917|
|                       9| 5874|
|                       8| 5843|
|                       6| 5839|
|                      21| 5821|
|                      38| 5817|
|                      10| 5806|

# 10. What neighborhoods in San Francisco had the worst response time in 2018?

fire_ts_df.select("Neighborhood", "ResponseDelayedinMins").filter(year("IncidentDate") == 2018).show(10, False)

# 11. How can we use Parquet files or SQL table to store data and read it back?

fire_ts_df.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")