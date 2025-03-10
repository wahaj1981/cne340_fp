import mysql.connector
import csv
import numpy as np
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
import matplotlib.pyplot as plt

# # Reviewing the data # #

online_data = r'https://api.covidtracking.com/v1/us/daily.csv'
tables = pd.read_csv(online_data)
print(tables.head(15))  # getting first 15 entries of dataframe
print(tables.tail(15))  # getting last 15 entries of dataframe
# how big is this data
tables.shape
print("This data has {} rows and {} columns.".format(tables.shape[0], tables.shape[1]))
# review columns
print(tables.columns)
# review variable types and names
print(tables.dtypes)

# install cryptography
hostname="127.0.0.1"
username="root"
passwd=""
db_name="gold_prices"

# install pymysql and sqlalchemy
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=db_name, user=username, pw=passwd))
# review and download data
tables = pd.read_csv('https://api.covidtracking.com/v1/us/daily.csv')
#tables = pd.read_csv('https://api.xxx.csv')
tables.rename(columns = {'submission_date':'date'}, inplace=True) #rename for better understanding on data

# connect to db
connection=engine.connect()
# create table
tables.to_sql('gold', con = engine, if_exists = 'append')
# create second table for DISC details
connection.execute(text('CREATE TABLE gold_2 Like gold'))
connection.execute(text('INSERT INTO gold_2 SELECT DISTINCT * FROM gold'))
# drop gols table and rename gold_2 to gold
connection.execute(text('DROP TABLE gold'))
connection.execute(text('ALTER TABLE gold_2 RENAME TO gold'))

#df = pd.read_sql_table('gold', connection)
#https://eazybi.com/blog/data-visualization-and-chart-types
df = pd.read_csv('https://api.covidtracking.com/v1/us/daily.csv')



################################################## UPDATE TO Aaron CODE ###############################################

import mysql.connector
import requests
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine, text
from datetime import date
import pymysql  # Import pymysql for MySQL connection in SQLAlchemy
import matplotlib.pyplot as plt

# Database credentials
uname = 'root'
pwd = ''
hostname = '127.0.0.1'
dbname = 'gold'

# Connect to MySQL database
def connect_to_sql():
    conn = mysql.connector.connect(user=uname, password=pwd,
                                   host=hostname,
                                   database=dbname)
    return conn

# Create the database if it doesn't exist
def create_database():
    conn = mysql.connector.connect(user=uname, password=pwd, host=hostname)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS gold")
    cursor.close()
    conn.close()

# Main function
def main():
    create_database()

    # Connect to the MySQL database using SQLAlchemy
    engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")

    # Read the Excel file
    tables = pd.read_excel(
        'https://auronum.co.uk/wp-content/uploads/2024/09/Auronum-Historic-Gold-Price-Data-5.xlsx',
        sheet_name='Sheet1',
        engine='openpyxl'
    )

    # Print column names to inspect structure
    print("Column names:", tables.columns)

    # Select only the necessary columns (adjust based on actual column names)
    tables = tables.iloc[:, [1, 3]]  # Adjust to select columns as needed, assuming Date is in column 1 and Gold_Price is in column 3
    tables.columns = ['GBP/Gold', 'Gold_Price']  # Rename to expected names

    # Convert 'GBP/Gold' column to datetime (This seems to be the actual date column)
    tables['GBP/Gold'] = pd.to_datetime(tables['GBP/Gold'], errors='coerce')

    # Reset index
    tables.reset_index(drop=True, inplace=True)

    # Connect and load the data into SQL
    connection = engine.connect()
    tables.to_sql('gold', con=engine, if_exists='replace', index=False)

    # Create a temporary table and manipulate data
    connection.execute(text('CREATE TABLE gold_temp_2 LIKE gold'))
    connection.execute(text('INSERT INTO gold_temp_2 SELECT DISTINCT `GBP/Gold`, `Gold_Price` FROM gold'))
    connection.execute(text('DROP TABLE gold'))
    connection.execute(text('ALTER TABLE gold_temp_2 RENAME TO gold'))

    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()

# Connect to the database to fetch data for visualization
conn = connect_to_sql()
cursor = conn.cursor()

# Query the database
cursor.execute("SELECT * FROM gold")
rows = cursor.fetchall()

# Close the database connection
cursor.close()
conn.close()

# Create a DataFrame from the query results
df = pd.DataFrame(rows, columns=['GBP/Gold', 'Gold_Price'])

# Convert the 'GBP/Gold' column to datetime
df['GBP/Gold'] = pd.to_datetime(df['GBP/Gold'], errors='coerce')

# Set the 'GBP/Gold' column as the index
df.set_index('GBP/Gold', inplace=True)

# Plot the data
plt.figure(figsize=(10, 5))
plt.plot(df.index, df['Gold_Price'], marker='o', linestyle='-', color='b', label='Gold Price')
plt.xlabel('GBP/Gold')
plt.ylabel('Gold Price')
plt.title('Gold Price Over Time')
plt.legend()
plt.grid()

# Show the plot
plt.show()
