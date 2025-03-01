# ETL-data Analyst project 

Data Analyst Project: ETL Process with Kaggle API, Pandas ,Numpy, SQL Server, and Power BI Severice Visualization .
! pip install requests! pip install requests
import requests # type: ignore

url = "https://www.kaggle.com/datasets/ankitbansal06/retail-orders"  # Replace with correct full URL
headers = {
    'Authorization': 'Bearer YOUR_API_KEY'  # Replace with your actual API key
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    # Proceed with the data
    data = response.text
    print("Data fetched successfully!")
else:
    print(f"Failed to access the dataset. Status code: {response.status_code}")
    #Data fetched successfully!
 import kaggle # type: ignore
!kaggle datasets download ankitbansal06/retail-orders
! kaggle datasets download -d ankitbansal06/retail-orders --force
import zipfile

# Correct the file path using one of the above solutions
zip_file_path = r"C:\Users\santh\Downloads\orders.csv.zip"  # Raw string example

# Open and extract the dataset
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall("C:/Users/santh/Downloads/extracted_data")  # Path to extract
import pandas as pd

# Corrected read_csv statement
df = pd.read_csv(r'C:\Users\santh\Downloads\orders.csv.zip', na_values=['Not available', 'unknown'])

df.head(20)

# Step 1: Check all column names
print(df.columns)

# Step 2: Strip any leading or trailing spaces from column names
df.columns = df.columns.str.strip()

# Step 3: Access the 'Ship Mode' column and get unique values (adjust the column name if needed)
print(df['Ship Mode'].unique())
df.rename(columns={'Order Id':'order_id','City':'city'})
df.columns.str.lower()
df.columns.str.replace(' ','_')
df.head(10)

import pandas as pd
df = pd.read_csv(r'C:\Users\santh\Downloads\orders.csv.zip')

print(df.columns)
df['Discount'] = df['List Price'] * df['Discount Percent'] * 0.01

# Create 'sales price' column if not done yet
df['sales price'] = df['List Price'] - df['Discount']

# Now, calculate the 'profit' column
df['profit'] = df['sales price'] - df['cost price']
df['Order Date']=pd.to_datetime(df['Order Date'], format="%Y-%m-%d")
df.drop(columns=['cost price','List Price','Discount Percent'],inplace=True)
df.head(10)
import sqlalchemy as sql

# Example connection string for SQL Server
conn = sql.create_engine('mssql+pyodbc://sa:18122003@SANTHOSH/master?driver=ODBC+Driver+17+for+SQL+Server')

# Now you can use df.to_sql
df.to_sql('df_orders', con=conn, index=False, if_exists='replace')

SQL QURIES:
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


AND then next connect powerBI and  create interactive dashboard













