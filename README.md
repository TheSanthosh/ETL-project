# ETL-project
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




