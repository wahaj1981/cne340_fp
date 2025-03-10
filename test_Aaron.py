import mysql.connector
import requests
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine, text
from datetime import date
import mysql.connector

uname = 'root'
pwd = ''
hostname = '127.0.0.1'
dbname = 'gold'

# Connect to database
def connect_to_sql():
    conn = mysql.connector.connect(user=uname, password=pwd,
                                   host=hostname,
                                   database=dbname)
    return conn

# Create the database if it doesn't exist
def create_database():
    conn = connect_to_sql()
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS gold")
    cursor.close()
    conn.close()

# Main function
def main():
    create_database()

# Connect to the newly created database using SQLAlchemy
    engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")

# Read the Excel file
    tables = pd.read_excel('https://www.eia.gov/dnav/pet/hist_xls/EMD_EPD2DXL0_PTE_R5XCA_DPGw.xls', sheet_name='Data 1',
                           header=2, index_col='Date')

# Rename columns to shorter names
    tables.columns = ['Gold_Price']

# Reset index to include 'Date' as a column
    tables.reset_index(inplace=True)

# Connect and load the data into SQL
    connection = engine.connect()
    tables.to_sql('gold', con=engine, if_exists='replace', index=False)

# Create a temporary table and manipulate data
    connection.execute(text('CREATE TABLE gold_temp_2 LIKE gold'))
    connection.execute(text('INSERT INTO gold_temp_2 SELECT DISTINCT Date, Gold_Price FROM gold'))
    connection.execute(text('DROP TABLE gold'))
    connection.execute(text('ALTER TABLE gold_temp_2 RENAME TO gold'))

# Close the database connection
    connection.close()

if __name__ == "__main__":
    main()

import matplotlib.pyplot as plt

# Connect to the database
conn = connect_to_sql()
cursor = conn.cursor()

# Query the database
cursor.execute("SELECT * FROM gold")
rows = cursor.fetchall()

# Close the database connection
cursor.close()
conn.close()

# Create a DataFrame from the query results
df = pd.DataFrame(rows, columns=['Date', 'Gold_Price'])

# Convert the 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'])

# Set the 'Date' column as the index
df.set_index('Date', inplace=True)

# Plot the data
plt.plot(df.index, df['Gold_Price'])
plt.xlabel('Date')
plt.ylabel('Gold Price')
plt.title('Gold Price Over Time')

# Show the plot
plt.show()
