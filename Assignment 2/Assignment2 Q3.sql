-- 3. Execute following queries on "emp" and "dept" dataset using CTE.

-- 1. Find emp with max sal of each dept.

WITH MaxSalaries AS (
    SELECT deptno, MAX(sal) AS maxSal
    FROM emp
    GROUP BY deptno
)
SELECT e.ename, e.deptno, e.sal
FROM emp e
JOIN MaxSalaries m ON e.deptno = m.deptno AND e.sal = m.maxSal;


select ename,deptno,sal from emp_staging where sal in (select max(sal) maxSal from emp_staging group by deptno);
+--------+---------+---------+
| ename  | deptno  |   sal   |
+--------+---------+---------+
| BLAKE  | 30      | 2850.0  |
| SCOTT  | 20      | 3000.0  |
| KING   | 10      | 5000.0  |
| FORD   | 20      | 3000.0  |
+--------+---------+---------+

-- 2. Find avg of deptwise total sal.

WITH avgsal AS
(select deptno,avg(sal) avg_sal from emp_staging group by deptno)
SELECT deptno,round(avg_sal) avg from avgsal;

+---------+---------+
| deptno  |   avg   |
+---------+---------+
| 10      | 2917.0  |
| 20      | 2175.0  |
| 30      | 1567.0  |
+---------+---------+

-- 3. Compare (show side-by-side) sal of each emp with avg sal in his dept and avg sal for his job.

select deptno,round(avg(sal),2) avgSal from emp_staging group by deptno;
+---------+----------+
| deptno  |  avgsal  |
+---------+----------+
| 10      | 2916.67  |
| 20      | 2175.0   |
| 30      | 1566.67  |
+---------+----------+

select job,deptno,round(avg(sal),2) avg_sal_job from emp_staging group by job,deptno;
+------------+---------+--------------+
|    job     | deptno  | avg_sal_job  |
+------------+---------+--------------+
| ANALYST    | 20      | 3000.0       |
| CLERK      | 10      | 1300.0       |
| CLERK      | 20      | 950.0        |
| CLERK      | 30      | 950.0        |
| MANAGER    | 10      | 2450.0       |
| MANAGER    | 20      | 2975.0       |
| MANAGER    | 30      | 2850.0       |
| PRESIDENT  | 10      | 5000.0       |
| SALESMAN   | 30      | 1400.0       |
+------------+---------+--------------+

WITH dept_avg_table AS(
    select deptno,round(avg(sal),2) avgSal from emp_staging group by deptno
),
job_avg_table AS(
    select job,deptno,round(avg(sal),2) avg_sal_job from emp_staging group by job,deptno
)
SELECT e.ename,j.job,j.avg_sal_job,d.avgSal as avg_sal_dept FROM emp_staging e
INNER JOIN job_avg_table j on e.job=j.job
INNER JOIN dept_avg_table d ON e.deptno = d.deptno;


-- 4. Divide emps by category -- Poor < 1500, 1500 <= Middle <= 2500, Rich > 2500. Hint: CASE ... WHEN. 
-- Count emps for each category.

SELECT ename,(CASE WHEN sal<1500 THEN "Poor" WHEN sal>=1500 AND sal<=2500 THEN "Middle" WHEN sal>2500 THEN "Rich" END) AS category from emp_staging;
+---------+-----------+
|  ename  | category  |
+---------+-----------+
| SMITH   | Poor      |
| ALLEN   | Middle    |
| WARD    | Poor      |
| JONES   | Rich      |
| MARTIN  | Poor      |
| BLAKE   | Rich      |
| CLARK   | Middle    |
| SCOTT   | Rich      |
| KING    | Rich      |
| TURNER  | Middle    |
| ADAMS   | Poor      |
| JAMES   | Poor      |
| FORD    | Rich      |
| MILLER  | Poor      |
+---------+-----------+

WITH category_table AS(
    SELECT ename,(CASE 
            WHEN sal<1500 THEN "Poor" 
            WHEN sal>=1500 AND sal<=2500 THEN "Middle" 
            WHEN sal>2500 THEN "Rich" 
            END) AS category 
            from emp_staging
)
SELECT category,COUNT(ename) FROM category_table GROUP BY category;

+-----------+------+
| category  | _c1  |
+-----------+------+
| Middle    | 3    |
| Poor      | 6    |
| Rich      | 5    |
+-----------+------+

-- 5. Display emps with category (as above), empno, ename, sal and dname.

WITH category_table AS(
SELECT ename,sal,deptno,(CASE 
        WHEN sal<1500 THEN "Poor" 
        WHEN sal>=1500 AND sal<=2500 THEN "Middle" 
        WHEN sal>2500 THEN "Rich" 
        END) AS category 
        FROM emp_staging
)
SELECT c.ename,d.dname,c.category FROM category_table c 
INNER JOIN dept_staging d ON c.deptno=d.deptno;

+----------+----------+---------+-------------+-------------+
| c.empno  | c.ename  |  c.sal  |   d.dname   | c.category  |
+----------+----------+---------+-------------+-------------+
| 7369     | SMITH    | 800.0   | RESEARCH    | Poor        |
| 7499     | ALLEN    | 1600.0  | SALES       | Middle      |
| 7521     | WARD     | 1250.0  | SALES       | Poor        |
| 7566     | JONES    | 2975.0  | RESEARCH    | Rich        |
| 7654     | MARTIN   | 1250.0  | SALES       | Poor        |
| 7698     | BLAKE    | 2850.0  | SALES       | Rich        |
| 7782     | CLARK    | 2450.0  | ACCOUNTING  | Middle      |
| 7788     | SCOTT    | 3000.0  | RESEARCH    | Rich        |
| 7839     | KING     | 5000.0  | ACCOUNTING  | Rich        |
| 7844     | TURNER   | 1500.0  | SALES       | Middle      |
| 7876     | ADAMS    | 1100.0  | RESEARCH    | Poor        |
| 7900     | JAMES    | 950.0   | SALES       | Poor        |
| 7902     | FORD     | 3000.0  | RESEARCH    | Rich        |
| 7934     | MILLER   | 1300.0  | ACCOUNTING  | Poor        |
+----------+----------+---------+-------------+-------------+


-- 6. Count number of emps in each dept for each category (as above).

WITH cat_table as(
SELECT ename,sal,deptno,(CASE 
        WHEN sal<1500 THEN "Poor" 
        WHEN sal>=1500 AND sal<=2500 THEN "Middle" 
        WHEN sal>2500 THEN "Rich" 
        END) AS category 
        FROM emp_staging
)
SELECT d.dname,c.category,COUNT(c.ename) FROM dept_staging d 
INNER JOIN cat_table c on d.deptno=c.deptno 
GROUP BY d.dname,c.category;

+-------------+-------------+------+
|   d.dname   | c.category  | _c2  |
+-------------+-------------+------+
| ACCOUNTING  | Middle      | 1    |
| ACCOUNTING  | Poor        | 1    |
| ACCOUNTING  | Rich        | 1    |
| RESEARCH    | Poor        | 2    |
| RESEARCH    | Rich        | 3    |
| SALES       | Middle      | 2    |
| SALES       | Poor        | 3    |
| SALES       | Rich        | 1    |
+-------------+-------------+------+
8 rows selected (54.475 seconds)
