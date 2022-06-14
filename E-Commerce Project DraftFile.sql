--E-Commerce Project
--CHANGES ON prod_dimen
UPDATE
    prod_dimen
SET
    Prod_id = TRIM('Prod_' FROM Prod_id)

ALTER TABLE  prod_dimen
	ALTER COLUMN Prod_id SMALLINT

ALTER TABLE prod_dimen
	ALTER COLUMN Prod_id SMALLINT NOT NULL

ALTER TABLE prod_dimen
	ADD PRIMARY KEY (Prod_id);

--CHANGES ON shipping_dimen
UPDATE
    shipping_dimen
SET
    Ship_id = TRIM('SHP_' FROM Ship_id)

ALTER TABLE  shipping_dimen
	ALTER COLUMN Ship_id SMALLINT

ALTER TABLE shipping_dimen
	ALTER COLUMN Ship_id SMALLINT NOT NULL

ALTER TABLE shipping_dimen
	ADD PRIMARY KEY (Ship_id);

--CHANGES ON cust_dimen
UPDATE
    cust_dimen
SET
    Cust_id = TRIM('Cust_' FROM Cust_id)

ALTER TABLE  cust_dimen
	ALTER COLUMN Cust_id SMALLINT

ALTER TABLE cust_dimen
	ALTER COLUMN Cust_id SMALLINT NOT NULL

ALTER TABLE cust_dimen
	ADD PRIMARY KEY (Cust_id);

--CHANGES ON Ord_id
UPDATE
    orders_dimen
SET
   Ord_id = TRIM('Ord_' FROM Ord_id)

ALTER TABLE  orders_dimen
	ALTER COLUMN Ord_id SMALLINT

ALTER TABLE orders_dimen
	ALTER COLUMN Ord_id SMALLINT NOT NULL

ALTER TABLE orders_dimen
	ADD PRIMARY KEY (Ord_id);

--CHANGES ON market_fact
UPDATE
    market_fact
SET
   Ord_id = TRIM('Ord_' FROM Ord_id)

ALTER TABLE  market_fact
	ALTER COLUMN Ord_id SMALLINT

ALTER TABLE market_fact
	ALTER COLUMN Ord_id SMALLINT NOT NULL

ALTER TABLE market_fact
	ADD FOREIGN KEY (Ord_id) REFERENCES orders_dimen(Ord_id)
 
 UPDATE
    market_fact  
SET
   Prod_id = TRIM('Prod_' FROM Prod_id)

ALTER TABLE  market_fact
	ALTER COLUMN Prod_id SMALLINT

ALTER TABLE market_fact
	ALTER COLUMN Prod_id SMALLINT NOT NULL

ALTER TABLE market_fact
	ADD FOREIGN KEY (Prod_id) REFERENCES prod_dimen(Prod_id)

UPDATE
    market_fact  
SET
	Ship_id = TRIM('SHP_' FROM Ship_id)

ALTER TABLE  market_fact
	ALTER COLUMN Ship_id SMALLINT

ALTER TABLE market_fact
	ALTER COLUMN Ship_id SMALLINT NOT NULL

ALTER TABLE market_fact
	ADD FOREIGN KEY (Ship_id) REFERENCES shipping_dimen(Ship_id)

UPDATE
    market_fact  
SET
	Cust_id = TRIM('Cust_' FROM Cust_id)

ALTER TABLE  market_fact
	ALTER COLUMN Cust_id SMALLINT

ALTER TABLE market_fact
	ALTER COLUMN Cust_id SMALLINT NOT NULL

ALTER TABLE market_fact
	ADD FOREIGN KEY (Cust_id) REFERENCES cust_dimen(Cust_id)


--1. Join all the tables and create a new table called combined_table. (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

SELECT * INTO combined_table
FROM 
(
SELECT        market_fact.*, orders_dimen.Order_Date, orders_dimen.Order_Priority, prod_dimen.Product_Category, prod_dimen.Product_Sub_Category, cust_dimen.Customer_Name, cust_dimen.Province, cust_dimen.Region, 
                         cust_dimen.Customer_Segment, shipping_dimen.Order_ID, shipping_dimen.Ship_Mode, shipping_dimen.Ship_Date
FROM            market_fact INNER JOIN
                         cust_dimen ON market_fact.Cust_id = cust_dimen.Cust_id INNER JOIN
                         orders_dimen ON market_fact.Ord_id = orders_dimen.Ord_id INNER JOIN
                         prod_dimen ON market_fact.Prod_id = prod_dimen.Prod_id INNER JOIN
                         shipping_dimen ON market_fact.Ship_id = shipping_dimen.Ship_id
						 ) A;

SELECT * FROM combined_table



--///////////////////////


--2. Find the top 3 customers who have the maximum count of orders.


select Cust_id, Customer_Name, count (DISTINCT Ord_id) as cnt_orders --burada count i�inde distinct kullanmak daha mant�kl�
from combined_table
group by Cust_id, Customer_Name
ORDER BY cnt_orders DESC

/*SELECT DISTINCT TOP 3  cust_id, Customer_Name,
	COUNT(Ord_id) OVER (partition by cust_id) total_orders_by_customers  
FROM combined_table 
ORDER BY  total_orders_by_customers DESC */
--/////////////////////////////////



--3.Create a new column at combined_table as DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.
--Use "ALTER TABLE", "UPDATE" etc.

ALTER TABLE combined_table ADD DaysTakenForDelivery SMALLINT 

UPDATE combined_table 
SET DaysTakenForDelivery = DATEDIFF(day, Order_Date, Ship_Date)

SELECT  * FROM #combined_table
/*
SELECT * INTO combined_table2
FROM 
	(
	SELECT *, DATEDIFF(day, Order_Date, Ship_Date) AS DaysTakenForDelivery
	FROM combined_table
	) A;
SELECT * FROM combined_table2

ALTER TABLE combined_table ADD DaysTakenForDelivery SMALLINT 

UPDATE combined_table 
	SET DaysTakenForDelivery = combined_table2.DaysTakenForDelivery 
								FROM combined_table
								INNER JOIN combined_table2 ON combined_table.Ship_id = combined_table2.Ship_id

SELECT Order_Date,Ship_Date,DaysTakenForDelivery FROM combined_table;

DROP TABLE combined_table2;
*/

--////////////////////////////////////


--4. Find the customer whose order took the maximum time to get delivered.
--Use "MAX" or "TOP"

SELECT TOP 1 Customer_Name, DaysTakenForDelivery 
FROM combined_table
ORDER BY DaysTakenForDelivery DESC

--ALTERNATIVE
select top 1 FIRST_VALUE(daystakenfordelivery) over (order by daystakenfordelivery desc),Customer_Name, Order_Date,Ship_Date
from combined_table
order by DaysTakenForDelivery desc
--////////////////////////////////



--5. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
--You can use date functions and subqueries

--Count the total number of unique customers
SELECT COUNT (DISTINCT Cust_id)
FROM combined_table
WHERE Order_Date LIKE '2011-01%'

--how many of them came back every month over the entire year in 2011
SELECT DISTINCT Cust_id
FROM combined_table
WHERE Order_Date LIKE '2011-01%'

SELECT YEAR(order_date) [Year], MONTH(order_date) [Month],COUNT (DISTINCT Cust_id) Total_Unique_Customers
FROM combined_table
WHERE YEAR(order_date) ='2011'
AND Cust_id IN 
(SELECT DISTINCT Cust_id
FROM combined_table
WHERE Order_Date LIKE '2011-01%')
GROUP BY YEAR(order_date), MONTH(order_date)


--ALTERNATIVE
WITH T1 AS (
SELECT Cust_id
FROM combined_table
WHERE YEAR(Order_Date) = 2011
AND MONTH(Order_Date ) = 1
)
SELECT MONTH(Order_Date) ORD_MONTH, COUNT(DISTINCT A.Cust_id) CNT_CUST
FROM  combined_table A, T1
WHERE	A.Cust_id = T1.Cust_id
AND		YEAR(Order_Date) = 2011
GROUP BY MONTH(Order_Date)

--////////////////////////////////////////////


--6. write a query to return for each user acording to the time elapsed between the first purchasing and the third purchasing, 
--in ascending order by Customer ID
--Use "MIN" with Window Functions

WITH T1 AS
(
SELECT *,
MIN(order_date) OVER(partition by cust_id) AS MinOfOrderDate
FROM combined_table
), T2 AS
(
SELECT DISTINCT Cust_id, Order_Date,
				DENSE_RANK() OVER(PARTITION BY Cust_id ORDER BY Order_Date, Ord_id) rank_num --ord
FROM combined_table
)
SELECT DISTINCT A.Cust_id, A.Customer_Name, A.MinOfOrderDate, B.Order_Date AS Third_Order, 
		DATEDIFF(DAY,A.MinOfOrderDate,B.Order_Date) AS Difference_First_Third_Orders
FROM T1 A
LEFT JOIN T2 B ON A.Cust_id = B.Cust_id
WHERE B.rank_num = 3;

--ALTERNATIVE
WITH sub1 AS (
SELECT Cust_id,
       MAX(CASE WHEN ord_num = 1 THEN C.Order_Date END) as OrderDate_1,
       MAX(CASE WHEN ord_num = 3 THEN C.Order_Date END) as OrderDate_3,
       COUNT(DISTINCT C.Ord_id) AS TotalNumberOfOrders   
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Cust_id ORDER BY Order_Date) as ord_num
      FROM combined_table 
     ) C 
GROUP BY Cust_id
)
select CONVERT(VARCHAR(5),DATEDIFF(day, OrderDate_1, OrderDate_3)) from sub1 where OrderDate_3 is Not null


-- ALTERNATIVE WITH lead

--Join 3 lists and group by this list by cust_id and ord_id. So we obtain a list with unique ord_id.
WITH T3 AS(
SELECT A.Cust_id, A.Customer_Name, B.Ord_id, B.order_date
FROM dbo.cust_dimen A, dbo.orders_dimen B, dbo.market_fact C
WHERE A.Cust_id=C.Cust_id AND
	  B.Ord_id=C.Ord_id
GROUP BY A.Cust_id, A.Customer_Name, B.Ord_id, B.order_date
),
--By lead function get the third order date and give row number to the orders for each customer
T4 AS(
SELECT Cust_id, Customer_Name, order_date,Ord_id,
	   LEAD(order_date,2) OVER(PARTITION BY Cust_id ORDER BY order_date) third_order,
	   ROW_NUMBER() OVER(PARTITION BY Cust_id ORDER BY order_date) row_num
FROM T3)
--Find the difference between first and third order and
--filter customers who had third and more orders.
SELECT Cust_id, Customer_Name, order_date, Ord_id, third_order,
	   DATEDIFF(day, order_date, third_order) diff_first_third
FROM T4
WHERE third_order IS NOT NULL AND
      row_num=1;

--ALTERNATIVE
CREATE VIEW CUST_ORD_TBL as
        (SELECT DISTINCT Cust_id,
						Order_Date,
						YEAR(Order_Date) as orderyear,
						MONTH(Order_Date) as ordermonth 		
		 FROM combined_table)
CREATE VIEW Next_ord_1_3 as (
						SELECT  *,LEAD(Order_Date,2) over(partition by cust_id ORDER BY Order_Date) AS order_3
						FROM CUST_ORD_TBL
						)
SELECT Cust_id, 
	Order_Date,
	order_3 ,
	DATEDIFF(DAY,Order_Date,order_3) as order_diff
FROM Next_ord_1_3
WHERE DATEDIFF(DAY,Order_Date,order_3) IS NOT NULL
ORDER BY Cust_id
--//////////////////////////////////////

--7. Write a query that returns customers who purchased both product 11 and product 14, 
--as well as the ratio of these products to the total number of products purchased by all customers.
--Use CASE Expression, CTE, CAST and/or Aggregate Functions

--There is 19 customer who bought product 11 and 14 at the same time
SELECT DISTINCT Cust_id, Customer_Name
FROM combined_table A
WHERE Prod_id = 11
AND EXISTS (
SELECT DISTINCT Cust_id, Customer_Name 
FROM combined_table B
WHERE Prod_id = 14
AND A.Cust_id = B.Cust_id)
ORDER BY Customer_Name

-- 8103 product saled, there is some same order id with same product id so that i use distinct
SELECT DISTINCT Ord_id, Prod_id
FROM combined_table
GROUP BY Ord_id, Prod_id
ORDER BY Ord_id
--THERE is 107 products which are product 11 and product 14 and bought by same customers
SELECT Cust_id, Ord_id, Prod_id
FROM combined_table C
WHERE Prod_id = 14 or Prod_id = 11
		AND Cust_id IN (56,138,186,428,466,561,583,595,696,1194,1244,1309,1371,1401,1497,1538,1680,1799,1833)
ORDER BY Cust_id

--OR
WITH T1 AS
(
SELECT DISTINCT Cust_id, Customer_Name
FROM combined_table A
WHERE Prod_id = 11
AND EXISTS (
SELECT DISTINCT Cust_id, Customer_Name 
FROM combined_table B
WHERE Prod_id = 14
AND A.Cust_id = B.Cust_id))
SELECT Cust_id, Ord_id, Prod_id
FROM combined_table C
WHERE Prod_id = 14 or Prod_id = 11
		AND Cust_id IN (SELECT Cust_id FROM T1)
ORDER BY Cust_id
--the ratio
SELECT CAST((((1.0*107)/(1.0*8103))*100) AS numeric (3,1)) AS The_ratio 
SELECT CAST((((1.0*107)/(1.0*8399))*100) AS numeric (3,1)) AS The_ratio

--OR calling from tables directly
WITH T1 AS
(
SELECT DISTINCT Cust_id, Customer_Name
FROM combined_table A
WHERE Prod_id = 11
AND EXISTS (
SELECT DISTINCT Cust_id, Customer_Name 
FROM combined_table B
WHERE Prod_id = 14
AND A.Cust_id = B.Cust_id)), T2 AS
(
SELECT Cust_id, Ord_id, Prod_id
FROM combined_table C
WHERE Prod_id = 14 or Prod_id = 11
		AND Cust_id IN (SELECT Cust_id FROM T1)
),
T3 AS
(
SELECT DISTINCT Ord_id, Prod_id
FROM combined_table
GROUP BY Ord_id, Prod_id
)
--I am calling from tables directly
SELECT CAST((((1.0*(SELECT COUNT(*) FROM T2))/(1.0*(SELECT COUNT(*) FROM T3)))*100) AS numeric (3,1)) AS The_ratio

--If the total sale of product 11 and 14 asked
--There is 442 sales
SELECT DISTINCT Ord_id, Prod_id
FROM combined_table
WHERE Prod_id =14 OR Prod_id =11
GROUP BY Ord_id, Prod_id
ORDER BY Ord_id
SELECT CAST((((1.0*442)/(1.0*8103))*100) AS numeric (3,1)) AS The_ratio 
SELECT CAST((((1.0*442)/(1.0*8399))*100) AS numeric (3,1)) AS The_ratio

--ALTERNATIVE

select cust_id
from combined_table
where prod_id = �Prod_14�
INTERSECT
select cust_id
from combined_table
where prod_id = �Prod_11�
9:40
select ((select count(*)
from combined_table
where prod_id = �Prod_11�) + (select count(*)
from combined_table
where prod_id = �Prod_14�)) / ((select count(*) from combined_table) * 1.0)

--ALTERNATIVE
SELECT SUM(Order_Quantity) AS prod1114_quant�ty
FROM combined_table
WHERE Cust_id IN(
				SELECT  Cust_id FROM combined_table where Prod_id=11
						INTERSECT
				SELECT Cust_id FROM combined_table where Prod_id=14)
	  AND Prod_id IN (11,14);


--/////////////////

--CUSTOMER SEGMENTATION



--1. Create a view that keeps visit logs of customers on a monthly basis. (For each log, three field is kept: Cust_id, Year, Month)
--Use such date functions. Don't forget to call up columns you might need later.
CREATE VIEW Customer_by_month_years AS
SELECT Cust_id, YEAR(order_Date) AS [Year], MONTH(order_date) AS [Month], COUNT(cust_id) AS [CountOfCustomer]
FROM combined_table
GROUP BY Cust_id,YEAR(order_Date), MONTH(order_date)

SELECT * FROM Customer_by_month_years
ORDER BY Cust_id

--//////////////////////////////////



  --2.Create a �view� that keeps the number of monthly visits by users. (Show separately all months from the beginning  business)
--Don't forget to call up columns you might need later.
CREATE VIEW month_years_totalcustomer AS
SELECT YEAR(order_Date) AS [Year], MONTH(order_date) AS [Month], COUNT(cust_id) AS [CountOfCustomer]
FROM combined_table
GROUP BY YEAR(order_Date), MONTH(order_date)

SELECT * FROM month_years_totalcustomer
ORDER BY [YEAR],[Month]


--//////////////////////////////////


--3. For each visit of customers, create the next month of the visit as a separate column.
--You can order the months using "DENSE_RANK" function.
--then create a new column for each month showing the next month using the order you have made above. (use "LEAD" function.)
--Don't forget to call up columns you might need later.
CREATE VIEW NEXT_VISIT_TABLE AS
WITH T1 AS
(
SELECT DISTINCT cust_id, customer_name, order_date, YEAR(order_date) year_, MONTH(order_date) month_, 
DENSE_RANK () over (partition by cust_id order by YEAR(order_date), MONTH(order_date)) RankOfMonth
from combined_table
), T2 AS
(
SELECT *, ( RankOfMonth+1) as NextVisitRank
FROM T1
), T3 AS
(
SELECT T2.*, T1.RankOfMonth AS T1_RANK, T1.Order_Date AS Next_Visit_Date
FROM T2
LEFT JOIN T1 ON T2.Cust_id = T1.Cust_id
WHERE T2.NextVisitRank = T1.RankOfMonth
)
SELECT T2.Cust_id,T2.Customer_Name,T2.Order_Date, YEAR(T2.order_date) order_date_year, MONTH(T2.order_date) order_date_month, 
		T3.Next_Visit_Date, YEAR(T3.Next_Visit_Date) Next_Visit_Year, MONTH(T3.Next_Visit_Date) Next_Visit_Month
FROM T2
LEFT JOIN T3 ON T2.Cust_id  = T3.Cust_id AND T2.NextVisitRank = T3.RankOfMonth+1


SELECT * FROM NEXT_VISIT_TABLE
ORDER BY Cust_id

--ALTERNATIVE
CREATE VIEW CUST_ORD_TBL as
        (SELECT DISTINCT Cust_id,
						Order_Date,
						YEAR(Order_Date) as orderyear,
						MONTH(Order_Date) as ordermonth 		
		 FROM combined_table)
SELECT * 
FROM (
		SELECT  Cust_id,
				Order_Date,
				lead(order_date) over(partition by cust_id order by order_date) AS  next_ord_date,
				DATEDIFF( MONTH, Order_Date, lead(order_date) over(partition by cust_id order by order_date)) AS order_diff
		FROM CUST_ORD_TBL) AS visits_customer
WHERE  next_ord_date IS NOT NULL
--/////////////////////////////////



--4. Calculate monthly time gap between two consecutive visits by each customer.
--Don't forget to call up columns you might need later.
SELECT *, DATEDIFF(MONTH,Order_Date, Next_Visit_Date) AS Monthly_Differences
FROM NEXT_VISIT_TABLE

--///////////////////////////////////


--5.Categorise customers using average time gaps. Choose the most fitted labeling model for you.
--For example: 
--Labeled as �churn� if the customer hasn't made another purchase for the months since they made their first purchase.
--Labeled as �regular� if the customer has made a purchase every month.
--Etc.

--THE Differences between first order and last order is 47 months IN THIS DATABASE
SELECT DATEDIFF(MONTH, (SELECT MIN(ORDER_DATE) FROM combined_table), (SELECT MAX(ORDER_DATE) FROM combined_table))

WITH T1 AS
(
SELECT DISTINCT Cust_id, Customer_Name, 
		COUNT (Ord_id) OVER(Partition by cust_id) AS Total_Orders,
		MIN (order_date) OVER(Partition by cust_id) AS First_order_of_customer,
		MAX (order_date) OVER(Partition by cust_id) AS Last_order_of_customer
		FROM combined_table
), T2 AS
(
SELECT *,(47/Total_Orders) Avg_Order_by_Month
FROM T1
)
SELECT *,
	CASE 
		WHEN Avg_Order_by_Month < 3 THEN 'Loyal Customer'
		WHEN Avg_Order_by_Month < 7 THEN 'Regular Customer'
		WHEN Avg_Order_by_Month < 13 THEN 'Customer'
		WHEN Avg_Order_by_Month < 25 THEN 'Potential Customer'
		WHEN Avg_Order_by_Month >= 25 THEN 'Ordered Just Once'
		END AS Status_Of_Customer 
FROM T2

--/////////////////////////////////////


--MONTH-WISE RETENT�ON RATE


--Find month-by-month customer retention rate  since the start of the business.


--1. Find the number of customers retained month-wise. (You can use time gaps)
--Use Time Gaps
CREATE VIEW TABLE_WITH_RANKOFMONTH
AS
	WITH T1 AS
	(
SELECT *, YEAR(order_date) year_, MONTH(order_date) month_, 
DENSE_RANK () over (order by YEAR(order_date), MONTH(order_date)) RankOfMonth
from combined_table
	)
	SELECT *, (RankOfMonth +1) AS RankPlus
	FROM T1

SELECT DISTINCT Year_, Month_, RankOfMonth FROM TABLE_WITH_RANKOFMONTH
ORDER BY RankOfMonth;

--you can find by writing ranks manually
SELECT COUNT (DISTINCT A.Cust_id) AS customers_retained_month_wise
		FROM TABLE_WITH_RANKOFMONTH A
		WHERE A.RankOfMonth = 6
				AND EXISTS (
		SELECT DISTINCT Cust_id, Customer_Name
		FROM TABLE_WITH_RANKOFMONTH B
		WHERE B.RankOfMonth = 5
			AND A.Cust_id=B.Cust_id) 

--with while

--WHILE

DECLARE @counter INT, @last_month_rank INT
SET @counter = 1
SET @last_month_rank = 48

WHILE @counter < @last_month_rank
	BEGIN
		SELECT @counter+1 as RankOfMonth, COUNT (DISTINCT A.Cust_id) AS customers_retained_month_wise
		FROM TABLE_WITH_RANKOFMONTH A
		WHERE A.RankOfMonth = @counter+1
		AND EXISTS (
		SELECT DISTINCT Cust_id, Customer_Name
		FROM TABLE_WITH_RANKOFMONTH B
		WHERE B.RankOfMonth = @counter
			AND A.Cust_id=B.Cust_id) 
		SET @counter = @counter + 1
END

-- I made the table manually by the results I took from while loops
CREATE TABLE customers_retained_month_wise
(RankOfMonth SMALLINT,
customers_retained_month_wise SMALLINT);
INSERT INTO customers_retained_month_wise (RankOfMonth, customers_retained_month_wise) VALUES
(1,NULL), (2,11),(3,13),(4,13),(5,13),(6,9),(7,12),(8,7),(9,11),(10,9),(11,5),(12,6),
(13,11),(14,7),(15,6),(16,4),(17,7),(18,9),(19,11),(20,12),(21,10),(22,16),(23,7),(24,12),
(25,4),(26,10),(27,7),(28,7),(29,11),(30,7),(31,8),(32,6),(33,3),(34,11),(35,7),(36,7),
(37,8),(38,5),(39,7),(40,12),(41,12),(42,4),(43,3),(44,7),(45,9),(46,9),(47,11),(48,8)

SELECT * FROM customers_retained_month_wise ORDER BY RankOfMonth
--//////////////////////


--2. Calculate the month-wise retention rate.

--Basic formula: o	Month-Wise Retention Rate = 1.0 * Number of Customers Retained in The Current Month / Total Number of Customers in the Current Month

--It is easier to divide the operations into parts rather than in a single ad-hoc query. It is recommended to use View. 
--You can also use CTE or Subquery if you want.

--You should pay attention to the join type and join columns between your views or tables.

CREATE VIEW TOTAL_CUSTOMER_TABLE AS
WITH T1 AS
(
SELECT year_,month_, COUNT(DISTINCT cust_id) total_customer
FROM TABLE_WITH_RANKOFMONTH
GROUP BY year_,month_
)
SELECT A.*, B.total_customer, C.customers_retained_month_wise
FROM TABLE_WITH_RANKOFMONTH A
LEFT JOIN T1 B ON A.year_ = B.year_ AND A.month_ = B.month_
LEFT JOIN customers_retained_month_wise C ON A.RankOfMonth =C.RankOfMonth;

SELECT DISTINCT year_, month_, customers_retained_month_wise, total_customer,
FORMAT(((1.0*customers_retained_month_wise)/total_customer), 'P', 'en-us') AS Month_Wise_Retention_Rate 
FROM TOTAL_CUSTOMER_TABLE

SELECT * FROM TOTAL_CUSTOMER_TABLE

---///////////////////////////////////
--Good luck!

----ALTERNATIVE FOR 7TH QUESTION
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �1�
)
group by month(order_date)
having month(Order_Date) = �2�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �2�
)
group by month(order_date)
having month(Order_Date) = �3�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �3�
)
group by month(order_date)
having month(Order_Date) = �4�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �4�
)
group by month(order_date)
having month(Order_Date) = �5�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �5�
)
group by month(order_date)
having month(Order_Date) = �6�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �6�
)
group by month(order_date)
having month(Order_Date) = �7�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �7�
)
group by month(order_date)
having month(Order_Date) = �8�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �8�
)
group by month(order_date)
having month(Order_Date) = �9�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �9�
)
group by month(order_date)
having month(Order_Date) = �10�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �10�
)
group by month(order_date)
having month(Order_Date) = �11�
union
select month(order_date) month_num, count(distinct cust_id) cust_returned
from combined_table
where cust_id in (select distinct cust_id
from combined_table
where month(order_date) = �11�
)
group by month(order_date)
having month(Order_Date) = �12�