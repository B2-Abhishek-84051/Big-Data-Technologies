-- Execute following queries on "emp" and "dept" dataset.
-- 1. Create table "emp_staging" and load data from emp.csv in it.

CREATE TABLE emp_staging(
empno INT,
ename STRING,
job STRING,
mgr INT,
hire DATE,
sal DOUBLE,
comm DOUBLE,
deptno INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH
'/home/abhishek/Desktop/BigData/data/emp.csv'
INTO TABLE emp_staging;

-- 2. Create table "dept_staging" and load data from dept.csv in it.

CREATE TABLE dept_staging(
deptno INT,
dname VARCHAR(40),
loc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH
'/home/abhishek/Desktop/BigData/data/dept.csv'
INTO TABLE dept_staging;


-- 3. Display dept name and number of emps in each dept.

SELECT d.dname,COUNT(e.empno) FROM dept_staging d inner join emp_staging e on d.deptno=e.deptno GROUP BY d.dname;
+-------------+------+
|   d.dname   | _c1  |
+-------------+------+
| ACCOUNTING  | 3    |
| RESEARCH    | 5    |
| SALES       | 6    |
+-------------+------+

-- 4. Display emp name and his dept name.

select e.ename,d.dname from emp_staging e inner join dept_staging d on  d.deptno=e.deptno;
+----------+-------------+
| e.ename  |   d.dname   |
+----------+-------------+
| SMITH    | RESEARCH    |
| ALLEN    | SALES       |
| WARD     | SALES       |
| JONES    | RESEARCH    |
| MARTIN   | SALES       |
| BLAKE    | SALES       |
| CLARK    | ACCOUNTING  |
| SCOTT    | RESEARCH    |
| KING     | ACCOUNTING  |
| TURNER   | SALES       |
| ADAMS    | RESEARCH    |
| JAMES    | SALES       |
| FORD     | RESEARCH    |
| MILLER   | ACCOUNTING  |
+----------+-------------+

-- 5. Display all emps (name, job, deptno) with their manager (name, job, deptno), who are not in their department.

select e1.ename,e1.job,e1.deptno,e2.ename,e2.job,e2.deptno from emp_staging e1 
inner join emp_staging e2 on e1.empno=e2.mgr where e1.deptno != e2.deptno;
+-----------+------------+------------+-----------+----------+------------+
| e1.ename  |   e1.job   | e1.deptno  | e2.ename  |  e2.job  | e2.deptno  |
+-----------+------------+------------+-----------+----------+------------+
| KING      | PRESIDENT  | 10         | JONES     | MANAGER  | 20         |
| KING      | PRESIDENT  | 10         | BLAKE     | MANAGER  | 30         |
+-----------+------------+------------+-----------+----------+------------+

-- 6. Display all manager names with list of all dept names (where they can work).

select e.ename,d.dname from emp_staging e 
inner join dept_staging d on e.deptno=d.deptno 
where e.empno in (select distinct e1.mgr from emp_staging e1);
+----------+-------------+
| e.ename  |   d.dname   |
+----------+-------------+
| JONES    | RESEARCH    |
| BLAKE    | SALES       |
| CLARK    | ACCOUNTING  |
| SCOTT    | RESEARCH    |
| KING     | ACCOUNTING  |
| FORD     | RESEARCH    |
+----------+-------------+


-- 7. Display job-wise total salary along with total salary of all employees.

select job,sum(sal) from emp_staging group by job with rollup;
+------------+----------+
|    job     |   _c1    |
+------------+----------+
| NULL       | 29025.0  |
| ANALYST    | 6000.0   |
| CLERK      | 4150.0   |
| MANAGER    | 8275.0   |
| PRESIDENT  | 5000.0   |
| SALESMAN   | 5600.0   |
+------------+----------+

-- 8. Display dept-wise total salary along with total salary of all employees.

select d.dname,sum(e.sal) from dept_staging d inner join emp_staging e on d.deptno=e.deptno group by d.dname with rollup;
+-------------+----------+
|   d.dname   |   _c1    |
+-------------+----------+
| NULL        | 29025.0  |
| ACCOUNTING  | 8750.0   |
| RESEARCH    | 10875.0  |
| SALES       | 9400.0   |
+-------------+----------+

-- 9. Display per dept job-wise total salary along with total salary of all employees.

select d.dname,e.job,sum(e.sal) from dept_staging d 
inner join emp_staging e on d.deptno=e.deptno group by d.dname,e.job with cube;
+-------------+------------+----------+
|   d.dname   |   e.job    |   _c2    |
+-------------+------------+----------+
| NULL        | NULL       | 29025.0  |
| NULL        | ANALYST    | 6000.0   |
| NULL        | CLERK      | 4150.0   |
| NULL        | MANAGER    | 8275.0   |
| NULL        | PRESIDENT  | 5000.0   |
| NULL        | SALESMAN   | 5600.0   |
| ACCOUNTING  | NULL       | 8750.0   |
| ACCOUNTING  | CLERK      | 1300.0   |
| ACCOUNTING  | MANAGER    | 2450.0   |
| ACCOUNTING  | PRESIDENT  | 5000.0   |
| RESEARCH    | NULL       | 10875.0  |
| RESEARCH    | ANALYST    | 6000.0   |
| RESEARCH    | CLERK      | 1900.0   |
| RESEARCH    | MANAGER    | 2975.0   |
| SALES       | NULL       | 9400.0   |
| SALES       | CLERK      | 950.0    |
| SALES       | MANAGER    | 2850.0   |
| SALES       | SALESMAN   | 5600.0   |
+-------------+------------+----------+

-- 10. Display number of employees recruited per year in descending order of employee count.

select year(hire) year,count(empno) emp_cnt from emp_staging group by year(hire) order by count(empno) desc;
+-------+----------+
| year  | emp_cnt  |
+-------+----------+
| 1981  | 10       |
| 1982  | 2        |
| 1983  | 1        |
| 1980  | 1        |
+-------+----------+

-- 11. Display unique job roles who gets commission.

select distinct job from emp_staging where comm is not NULL;
+-----------+
|    job    |
+-----------+
| SALESMAN  |
+-----------+

-- 12. Display dept name in which there is no employee (using sub-query).

select dname from dept_staging where deptno not in (select dictinct deptno from emp_staging);
+-------------+
|    dname    |
+-------------+
| OPERATIONS  |
+-------------+

-- 13. Display emp-name, dept-name, salary, total salary of that dept (using sub-query).

SELECT e.ename,d.dname,e.sal,table2.totalSal from emp_staging as e inner join dept_staging as d on e.deptno=d.deptno
INNER JOIN 
(SELECT d1.dname,SUM(e1.sal) as totalSal FROM dept_staging d1 inner join emp_staging e1 on d1.deptno=e1.deptno group by d1.dname) as table2
on d.dname=table2.dname;

+----------+-------------+---------+------------------+
| e.ename  |   d.dname   |  e.sal  | table2.totalsal  |
+----------+-------------+---------+------------------+
| SMITH    | RESEARCH    | 800.0   | 10875.0          |
| ALLEN    | SALES       | 1600.0  | 9400.0           |
| WARD     | SALES       | 1250.0  | 9400.0           |
| JONES    | RESEARCH    | 2975.0  | 10875.0          |
| MARTIN   | SALES       | 1250.0  | 9400.0           |
| BLAKE    | SALES       | 2850.0  | 9400.0           |
| CLARK    | ACCOUNTING  | 2450.0  | 8750.0           |
| SCOTT    | RESEARCH    | 3000.0  | 10875.0          |
| KING     | ACCOUNTING  | 5000.0  | 8750.0           |
| TURNER   | SALES       | 1500.0  | 9400.0           |
| ADAMS    | RESEARCH    | 1100.0  | 10875.0          |
| JAMES    | SALES       | 950.0   | 9400.0           |
| FORD     | RESEARCH    | 3000.0  | 10875.0          |
| MILLER   | ACCOUNTING  | 1300.0  | 8750.0           |
+----------+-------------+---------+------------------+
14 rows selected (147.164 seconds)


-- 14. Display all managers and presidents along with number of (immediate) subbordinates.

select e1.ename,e1.job,collect_list(e2.ename) from emp_staging AS e1
inner join
emp_staging AS e2
on e1.empno=e2.mgr
where e1.job in ("MANAGER","PRESIDENT")
group by e1.ename,e1.job;

+-----------+------------+---------------------------------------------+
| e1.ename  |   e1.job   |                     _c2                     |
+-----------+------------+---------------------------------------------+
| BLAKE     | MANAGER    | ["ALLEN","WARD","MARTIN","TURNER","JAMES"]  |
| CLARK     | MANAGER    | ["MILLER"]                                  |
| JONES     | MANAGER    | ["SCOTT","FORD"]                            |
| KING      | PRESIDENT  | ["JONES","BLAKE","CLARK"]                   |
+-----------+------------+---------------------------------------------+
