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

    # Print column names to inspect structure before renaming
    print("Column names before renaming:", tables.columns)

    # Rename columns after checking them carefully
    tables.rename(columns={
        "Unnamed: 0": "Index",
        #"GBP/Gold": "Date for GBP",  # Corrected column name to match the actual 'GBP/Gold'
        #"Unnamed: 2": "Gold_Price in GBP",
        "Unnamed: 3": "empty",
        "USD/Gold": "Date",  # Corrected column name to match the actual 'USD/Gold'
        "Unnamed: 5": "Gold_Price",
        # "Unnamed: 6": "Some_Value",
        # "Unnamed: 8": "Silver_Column",
        # "Unnamed: 9": "Extra_Data",
        # "Unnamed: 11": "Platinum_Column"
    }, inplace=True)

    # Print column names after renaming
    print("Column names after renaming:", tables.columns)

    # Now select only the necessary columns
    tables = tables[['Date', 'Gold_Price']]  # Ensure 'Date' and 'Gold_Price' are available
    tables.columns = ['Date', 'Gold_Price']  # Rename to expected names

    # Select only the necessary columns (adjust based on actual column names)
    tables = tables[['Date', 'Gold_Price']]  # Select Date and Gold_Price columns directly
    tables.columns = ['Date', 'Gold_Price']  # Rename to expected names

    # Convert 'Date' column to datetime
    tables['Date'] = pd.to_datetime(tables['Date'], errors='coerce')

    # Reset index
    tables.reset_index(drop=True, inplace=True)

    # Connect and load the data into SQL
    connection = engine.connect()
    tables.to_sql('gold', con=engine, if_exists='append', index=False)

    # Create a temporary table and manipulate data
    connection.execute(text('CREATE TABLE gold_temp_2 LIKE gold'))

    # Fix SQL query: Update column names as 'Date' and 'Gold_Price' since 'GBP/Gold' is not used.
    connection.execute(text('INSERT INTO gold_temp_2 SELECT DISTINCT `Date`, `Gold_Price` FROM gold'))
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
#######################################################################################
# Create a DataFrame from the query results
df = pd.DataFrame(rows, columns=['Date', 'Gold_Price'])
print(df)
# Convert the 'Gold_Price' column to datetime
df['Gold_Price'] = pd.to_datetime(df['Gold_Price'], errors='coerce')
print(df)
print(df.head)
# Set the 'Date' column as the index
df.set_index('Date', inplace=True)
# Resample the data by year and calculate the average gold price for each year
yearly_avg = df['Gold_Price'].resample('YE').mean()  #
print(yearly_avg)

df2 = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([3] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
    }
)
print(df2)

########################################################################################


# Plot 1: Line chart showing the gold price over time
plt.figure(figsize=(10, 5))
plt.plot(df.index, df['Gold_Price'], marker='o', linestyle='-', color='b', label='Gold_Price')
plt.xlabel('Date')
plt.ylabel('Gold_Price')
plt.title('Gold Price Over Time')
plt.legend()
plt.grid()
plt.show()

# Plot 2: Bar chart showing the gold price for each date
plt.figure(figsize=(10, 5))
plt.bar(df.index, df['Gold_Price'], color='g', label='Gold_Price')
plt.xlabel('Date')
plt.ylabel('Gold_Price')
plt.title('Gold Price for Each Date')
plt.legend()
plt.grid(axis='y')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.show()

# Plot 3: Histogram showing the distribution of gold prices
plt.figure(figsize=(10, 5))
plt.hist(df['Gold_Price'].dropna(), bins=20, color='r', edgecolor='black')
plt.xlabel('Gold_Price')
plt.ylabel('Frequency')
plt.title('Distribution of Gold Prices')
plt.grid(True)
plt.show()