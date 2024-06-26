SELECT Name FROM STUDENTS WHERE Marks > 75 ORDER BY SUBSTRING(Name,-3,1), SUBSTRING(Name,-2,1) DESC;

-- CREATE TABLE Customers(
-- CustomerID INT PRIMARY KEY,
-- Name VARCHAR(100),
-- Age INT,
-- LocationID INT
-- )
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY ','
-- STORED AS TEXTFILE;

Hive does not support the ENABLE or ENFORCED keywords for 
constraints. Instead, you can define constraints as DISABLE or NOT ENFORCED.

-- Customers
CREATE TABLE Customers (
    CustomerID INT,
    Name VARCHAR(100),
    Age INT,
    LocationID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Customers
INSERT INTO customers(CustomerID,Name,Age,LocationID) VALUES (1, 'John Doe', 30, 1);
INSERT INTO Customers VALUES (2, 'Jane Smith', 25, 2);
INSERT INTO Customers VALUES (3, 'Bob Johnson', 35, 1);
INSERT INTO Customers VALUES (4, 'Alice Brown', 28, 3);
INSERT INTO Customers VALUES (5, 'Charlie Davis', 32, 2);

-- Products
CREATE TABLE Products (
ProductID INT,
ProductName VARCHAR(100),
Category VARCHAR(50),
Price DECIMAL(10, 2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Products
INSERT INTO Products VALUES (1, 'Laptop', 'Electronics', 800.00);
INSERT INTO Products VALUES (2, 'Smartphone', 'Electronics', 400.00);
INSERT INTO Products VALUES (3, 'T-shirt', 'Clothing', 20.00);
INSERT INTO Products VALUES (4, 'Shoes', 'Footwear', 50.00);
INSERT INTO Products VALUES (5, 'Bookshelf', 'Furniture', 150.00);

-- Sales
CREATE TABLE Sales (
SaleID INT,
CustomerID INT,
ProductID INT,
SaleDate DATE,
Quantity INT,
TotalAmount DECIMAL(10, 2),
CONSTRAINT pk_sales PRIMARY KEY (SaleID) DISABLE NOVALIDATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Sales
INSERT INTO Sales VALUES (1, 1, 1, '2023-01-01', 2, 1600.00);
INSERT INTO Sales VALUES (2, 2, 3, '2023-01-02', 3, 60.00);
INSERT INTO Sales VALUES (3, 3, 2, '2023-01-03', 1, 400.00);
INSERT INTO Sales VALUES (4, 4, 4, '2023-02-01', 2, 100.00);
INSERT INTO Sales VALUES (5, 5, 5, '2023-02-02', 1, 150.00);

-- Locations table
CREATE TABLE Locations (
LocationID INT,
City VARCHAR(50),
State VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Locations
INSERT INTO Locations VALUES (1, 'Pune', 'Maharashtra');
INSERT INTO Locations VALUES (2, 'Mumbai', 'Maharashtra');
INSERT INTO Locations VALUES (3, 'Bangalore', 'Karnataka');

-- A. Retrieve the names of all customers who made a purchase.

select name from customers;
+----------------+
|      name      |
+----------------+
| John Doe       |
| Jane Smith     |
| Bob Johnson    |
| Alice Brown    |
| Charlie Davis  |
+----------------+

-- B. List the products and their total sales amounts for a given date range.

select Products.Productname,Sales.TotalAmount,Sales.SaleDate from Products 
inner join Sales on Products.ProductID=Sales.ProductID;

+-------------+-------------+------------+
| Productname | TotalAmount | SaleDate   |
+-------------+-------------+------------+
| Laptop      |     1600.00 | 2023-01-01 |
| T-shirt     |       60.00 | 2023-01-02 |
| Smartphone  |      400.00 | 2023-01-03 |
| Shoes       |      100.00 | 2023-02-01 |
| Bookshelf   |      150.00 | 2023-02-02 |
+-------------+-------------+------------+
5 rows selected (29.504 seconds)

-- C. Find the total sales amount for each product category.

select products.category,sum(sales.totalamount) from products inner join sales on products.productid=sales.productid group by products.category;
+--------------------+----------+
| products.category  |   _c1    |
+--------------------+----------+
| Clothing           | 60.00    |
| Electronics        | 2000.00  |
| Footwear           | 100.00   |
| Furniture          | 150.00   |
+--------------------+----------+

-- D. Identify the customers who made purchases in a specific city.

select customers.name,locations.city from customers inner join locations on customers.locationid=locations.locationid;
+-----------------+-----------------+
| customers.name  | locations.city  |
+-----------------+-----------------+
| John Doe        | Pune            |
| Jane Smith      | Mumbai          |
| Bob Johnson     | Pune            |
| Alice Brown     | Bangalore       |
| Charlie Davis   | Mumbai          |
+-----------------+-----------------+

-- E. Calculate the average age of customers who bought products in the 'Electronics' category.

select avg(customers.age) from customers 
inner join sales on customers.customerid=sales.customerid 
inner join products on sales.productid=products.productid where products.category='Electronics';
+-------+
|  _c0  |
+-------+
| 32.5  |
+-------+

-- F. List the top 3 products based on total sales amount.

select products.productname,sales.totalamount from products 
inner join sales on products.productid=sales.productid order by sales.totalamount desc limit 3;
+-----------------------+--------------------+
| products.productname  | sales.totalamount  |
+-----------------------+--------------------+
| Laptop                | 1600.00            |
| Smartphone            | 400.00             |
| Bookshelf             | 150.00             |
+-----------------------+--------------------+

-- G. Find the total sales amount for each month.

select month(sales.saledate) as month, sum(sales.totalamount) from sales group by month;
+--------+----------+
| month  |   _c1    |
+--------+----------+
| 1      | 2060.00  |
| 2      | 250.00   |
+--------+----------+

-- H. Identify the products with no sales.

select products.productname from products where products.productid not in (select sales.productid from sales);
+-----------------------+
| products.productname  |
+-----------------------+
+-----------------------+
No rows selected (115.824 seconds)

-- I. Calculate the total sales amount for each state.

select Locations.State,sum(Sales.TotalAmount) from Locations 
inner join Customers on Locations.LocationID=Customers.LocationID 
inner join Sales on Customers.CustomerID=Sales.Customerid group by Locations.State;
+-------------+------------------------+
| State       | sum(Sales.TotalAmount) |
+-------------+------------------------+
| Maharashtra |                 610.00 |
| Karnataka   |                 100.00 |
+-------------+------------------------+

-- J. Retrieve the customer names and their highest purchase amount.

select Customers.Name,max(Sales.TotalAmount) from Customers inner join Sales on Customers.CustomerID=Sales.CustomerID group by Customers.Name;
+---------------+-------------+
| Name          | TotalAmount |
+---------------+-------------+
| Jane Smith    |       60.00 |
| Bob Johnson   |      400.00 |
| Alice Brown   |      100.00 |
| Charlie Davis |      150.00 |
+---------------+-------------+

-- Prepared by: Nilesh Ghule.
-- Submitted by:Abhishek Bidwe
-- Under Guidance of Aniket Khandelwal
