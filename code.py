# CNE 340 Final Project
# Project Name: Historic Gold Price Data Analysis
# Due date: 03/18/2025
# Project Team : 1- Wahaj AL Obid, 2- Aaron Henson, 3- Lidsyda Nouanphachan
import pandas as pd
import numpy as np
import mysql.connector
import requests
import time
import mysql.connector
from sqlalchemy import create_engine, text

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

# Step 2: Now, connect to the newly created database using SQLAlchemy
engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")

# Step 3: Read the Excel file
tables = pd.read_excel('https://www.eia.gov/dnav/pet/hist_xls/EMD_EPD2DXL0_PTE_R5XCA_DPGw.xls', sheet_name='Data 1',
                       header=2, index_col='Date')
tables.rename(columns={'gold price': 'anythin'}, inplace=True)


# Step 4: Connect and load the data into SQL
connection = engine.connect()
tables.to_sql('gold', con=engine, if_exists='replace', index=False)

# Step 5: Create a temporary table and manipulate data
connection.execute(text('CREATE TABLE gold_temp_2 LIKE gold'))
connection.execute(text('INSERT INTO gold_temp_2 SELECT DISTINCT Date, gold_Price FROM gold'))
connection.execute(text('DROP TABLE gold'))
connection.execute(text('ALTER TABLE gold_temp_2 RENAME TO gold'))

# Step 6: Close the database connection
connection.close()
