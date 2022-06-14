---- RDB&SQL Exercise-2

----1. By using view get the average sales by staffs and years using the AVG() aggregate function.

CREATE VIEW Weekly_Agenda_8_1 AS

SELECT	first_name, last_name, year, avg_amount
FROM
	(
	SELECT	A.first_name, A.last_name, 
		YEAR(B.order_date) AS year, 
		AVG(C.quantity * C.list_price) AS avg_amount
	FROM	sale.staff A, sale.orders B, sale.order_item C
	WHERE	A.staff_id = B.staff_id AND
				B.order_id = C.order_id
	GROUP BY A.first_name, A.last_name, YEAR(B.order_date)
	) A
;

SELECT *
FROM Weekly_Agenda_8_1
ORDER BY first_name, last_name, year


--alternative

SELECT s.first_name, s.last_name, YEAR(o.order_date) AS year, AVG((i.list_price-i.discount)*i.quantity) AS avg_amount
FROM sales.staffs s
INNER JOIN sales.orders o
ON s.staff_id=o.staff_id
INNER JOIN sales.order_items i
ON o.order_id=i.order_id
GROUP BY s.first_name, s.last_name, YEAR(o.order_date)
ORDER BY first_name, last_name, YEAR(o.order_date)



----2. Select the annual amount of product produced according to brands (use window functions).

SELECT DISTINCT B.brand_name, P.model_year,
	COUNT(P.[product_id]) OVER (PARTITION BY  B.brand_name, P.model_year) AS annual_amount_brands
FROM [product].[brand] B
	INNER JOIN [product].[product] P
	ON B.brand_id = P.brand_id
ORDER BY B.brand_name, P.model_year




----3. Select the least 3 products in stock according to stores.

SELECT	*
FROM	(
		SELECT ss.store_name, p.product_name, SUM(s.quantity) product_quantity,
		ROW_NUMBER() OVER(PARTITION BY ss.store_name ORDER BY SUM(s.quantity) ASC) least_3
		FROM [sale].[store] ss
			INNER JOIN [product].[stock] s
			ON ss.store_id=s.store_id
			INNER JOIN [product].[product] p
			ON s.product_id=p.product_id
		GROUP BY ss.store_name, p.product_name
		HAVING SUM(s.quantity) > 0
		) A
WHERE	A.least_3 < 4

--alternative

;WITH temp_cte
AS(
SELECT ss.[store_name], pp.[product_name],
ROW_NUMBER() OVER(PARTITION BY ss.[store_name] ORDER BY ss.[store_name]) AS [row number]
FROM [product].[product] pp
INNER JOIN [product].[stock] ps
on pp.product_id = ps.product_id
INNER JOIN [sale].[store] ss
ON ps.store_id = ss.store_id
)
SELECT * FROM temp_cte
WHERE [row number] < 4




----4. Return the average number of sales orders in 2020 sales

SELECT AVG(A.sales_amounts) AS 'Average Number of Sales'
FROM (
    SELECT COUNT(order_id) sales_amounts
    FROM sale.orders
    WHERE order_date LIKE '%2020%' 
    GROUP BY staff_id
    ) as A


--alternative

SELECT COUNT(order_id) AS Count_of_Sales
INTO Total_Orders_2017
FROM sale.orders
WHERE YEAR(order_date) = 2020;

SELECT COUNT(first_name) AS Count_of_Staffs
INTO Staffs_Sold_2020
FROM sale.staff
WHERE staff_id IN (
				SELECT staff_id
				FROM sale.orders
				WHERE YEAR(order_date) = 2020);

SELECT A.Count_of_Sales / B.Count_of_Staffs AS 'Average Number of Sales'
FROM Total_Orders_2017 A, Staffs_Sold_2020 B;


--alternative

WITH cte_avg_sale AS(
	SELECT staff_id, COUNT(order_id) as sales_count
	FROM sale.orders
	WHERE YEAR(order_date)=2020
	GROUP BY staff_id
	)
SELECT AVG(sales_count) as 'Average Number of Sales'
FROM cte_avg_sale




----5. Assign a rank to each product by list price in each brand and get products with rank less than or equal to three.

SELECT * FROM (
				SELECT
					product_id,
					product_name,
					brand_id,
					list_price,
					RANK () OVER ( 
						PARTITION BY brand_id
						ORDER BY list_price DESC
					) price_rank 
				FROM
					product.product
			) t
WHERE price_rank <= 3;
