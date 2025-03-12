import pandas as pd
import numpy as np
import mysql.connector
import requests
import time
from sqlalchemy import create_engine, text
import pymysql  # Required for SQLAlchemy MySQL connections

# Database connection details
hostname = "127.0.0.1"
uname = "root"
pwd = ""
dbname = "gold"

# Step 1: Create the database using mysql.connector
conn = mysql.connector.connect(host=hostname, user=uname, password=pwd)
cursor = conn.cursor()

# Create the database if it doesn't exist
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
cursor.close()
conn.close()

# Step 2: Connect to the newly created database using SQLAlchemy
engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")

# Step 3: Read the Excel file
file_url = 'https://www.eia.gov/dnav/pet/hist_xls/EMD_EPD2DXL0_PTE_R5XCA_DPGw.xls'
tables = pd.read_excel(file_url, sheet_name=None, header=2)

# Check available sheets
print("Available sheets:", tables.keys())  # Debugging: Check the correct sheet name

# Load the correct sheet
df = tables.get('Data 1')  # Ensure 'Data 1' exists

if df is None:
    raise ValueError("Sheet 'Data 1' not found in the Excel file.")
print("Columns in 'Data 1':", df.columns)

# Rename columns based on actual content
df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]  # Normalize column names
print("Normalized columns:", df.columns)

possible_date_cols = [col for col in df.columns if 'date' in col.lower()]
possible_price_cols = [col for col in df.columns if 'price' in col.lower()]

print("Possible date columns:", possible_date_cols)
print("Possible price columns:", possible_price_cols)

if possible_date_cols:
    df.rename(columns={possible_date_cols[0]: 'date'}, inplace=True)
if possible_price_cols:
    df.rename(columns={possible_price_cols[0]: 'gold_price'}, inplace=True)

if 'date' not in df.columns or 'gold_price' not in df.columns:
    raise KeyError(f"Missing required columns. Available columns: {df.columns}")
df.rename(columns={'gold_price': 'price'}, inplace=True)  # Rename for consistency

# Drop NaN values
df.dropna(inplace=True)

# Step 4: Load the data into SQL
connection = engine.connect()
df.to_sql('gold', con=engine, if_exists='replace', index=False)

# Step 5: Create a temporary table and manipulate data
connection.execute(text('CREATE TABLE IF NOT EXISTS gold_temp_2 LIKE gold'))
connection.execute(text('INSERT INTO gold_temp_2 SELECT DISTINCT date, price FROM gold'))
connection.execute(text('DROP TABLE gold'))
connection.execute(text('ALTER TABLE gold_temp_2 RENAME TO gold'))

# Step 6: Close the database connection
connection.close()

print("Database setup and data import completed successfully.")
