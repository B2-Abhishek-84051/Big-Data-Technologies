# Find deptwise total sal from emp.csv and dept.csv. 
# Print dname and total sal.

# SELECT d.dname,SUM(e.sal) FROM emp e INNER JOIN dept d;

# import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

# create SparkSession
spark = SparkSession.builder \
            .appName("EmpDeptJoin") \
            .getOrCreate()

# load data and create dataframe for emp
emp_filepath = '/home/abhishek/Desktop/BigData/data/emp.csv'
emp = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(emp_filepath) \
        .toDF('empno','ename','job','mgr','hire','sal','comm','deptno')

# load data and create dataframe for dept
dept_filepath = '/home/abhishek/Desktop/BigData/data/dept.csv'
dept = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(dept_filepath) \
        .toDF('deptno','dname','loc')

# test dataframes
emp.printSchema()
emp.show(n=5, truncate=False)
dept.printSchema()
dept.show(n=5, truncate=False)

# perform join and aggregation
result = emp.alias('e') \
        .join(dept.alias('d'), emp['deptno'] == dept['deptno']) \
        .groupBy('d.dname') \
        .agg(_sum('e.sal').alias('total_sal'))

# display result
result.show()

# stop spark session
spark.stop()
