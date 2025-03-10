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
