SELECT TOP (1000) [Order_Id]
      ,[Order_Date]
      ,[Ship_Mode]
      ,[Segment]
      ,[Country]
      ,[City]
      ,[State]
      ,[Postal_Code]
      ,[Region]
      ,[Category]
      ,[Sub_Category]
      ,[Product_Id]
      ,[Quantity]
      ,[Discount]
      ,[sales_price]
      ,[profit]
  FROM [master].[dbo].[df_orders]






 ---find top 10 highest revenue generating Products
  select  top 10 Product_Id,sum(sales_Price) as sales
  from df_orders
  group by Product_Id
  order by sales desc


   ---find top 5 highest selleing  Products in each region
  -- First query to get distinct regions
SELECT DISTINCT region
FROM df_orders;

WITH cte AS (
    SELECT region, Product_Id, SUM(sales_Price) AS sales
    FROM df_orders
    GROUP BY region, Product_Id
)
SELECT *
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC) AS rn
    FROM cte
) AS ranked
WHERE rn <= 5;

---find the month over month growth comparsion for 2022 and 2023 sales eg : jan 2022 vs jan 2023
WITH cte AS (
    SELECT 
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month,
        SUM(sales_price) AS sales
    FROM df_orders
    GROUP BY YEAR(order_date), MONTH(order_date)
)
SELECT 
    order_month,
    SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END) AS sales_2022,
    SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END) AS sales_2023
FROM cte
GROUP BY order_month
ORDER BY order_month;

---each category which month  had highest sales---
WITH cte AS (
    SELECT category, 
           FORMAT(order_date, 'yyyyMM') AS order_year_month, 
           SUM(sales_price) AS sales
    FROM df_orders
    GROUP BY category, FORMAT(order_date, 'yyyyMM')
)
SELECT * 
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
    FROM cte
) a
WHERE rn = 1
ORDER BY category;


---which subcategory had higest growth by profit in 2023 compare to 2022---

WITH cte AS (
    SELECT sub_category,
           YEAR(order_date) AS order_year,
           MONTH(order_date) AS order_month,
           SUM(sales_price) AS sales
    FROM df_orders
    GROUP BY sub_category, YEAR(order_date), MONTH(order_date)
),
cte2 AS (
    SELECT 
        sub_category,
        SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END) AS sales_2022,
        SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END) AS sales_2023
    FROM cte
    GROUP BY sub_category
)
SELECT TOP 1 *,
       (sales_2023 - sales_2022) / NULLIF(sales_2022, 0) AS sales_growth_percentage
FROM cte2
ORDER BY sales_2023 DESC;  -- Ordering by sales_2023 to get the top 1 based on sales_2023

Select * from df_orders















































