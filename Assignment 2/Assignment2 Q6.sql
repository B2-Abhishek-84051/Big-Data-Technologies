-- 6. Upload busstops.json data into HDFS directory. Then create hive external table to fetch data using JsonSerDe.*/



> hadoop fs -mkdir -p /user/abhishek/busstops/input

> hadoop fs -put /home/abhishek/busstops.json /user/abhishek/busstops/input

> hadoop fs -cat /user/abhishek/busstops/input/busstops.json


CREATE EXTERNAL TABLE busstops (
`_id` STRUCT<oid: STRING>,
stop STRING,
code STRING,
seq DOUBLE,
stage DOUBLE,
name STRING,
location STRUCT<type: STRING,coordinates: ARRAY<DOUBLE>>
)
ROW FORMAT 
SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/abhishek/busstops/input';

SELECT * FROM busstops;


+---------------+----------------+----------------+---------------+-----------------+---------------------+----------------------------------------------------+
| busstops._id  | busstops.stop  | busstops.code  | busstops.seq  | busstops.stage  |    busstops.name    |                 busstops.location                  |
+---------------+----------------+----------------+---------------+-----------------+---------------------+----------------------------------------------------+
| {"oid":null}  | Non-BRTS       | 103B-D-04      | 4.0           | 1.0             | Aranyeshwar Corner  | {"type":"Point","coordinates":[73.857675,18.486381]} |
+---------------+----------------+----------------+---------------+-----------------+---------------------+----------------------------------------------------+