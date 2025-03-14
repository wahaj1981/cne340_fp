# CNE 340 Final Project
# Project Name: Historic Gold Price Data Analysis
# Due date: 03/18/2025
# Project Team :1- Wahaj AL Obid,2- Aaron Henson,3- Lidsyda Nouanphachan
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Database connection details
hostname = "127.0.0.1"
uname = "root"
pwd = ""
dbname = "gold"

# Connect to the MySQL database
engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")

# Fetch data from the database
query = "SELECT date, price FROM gold ORDER BY date"
df = pd.read_sql(query, con=engine)

# Convert 'date' column to datetime format
df['date'] = pd.to_datetime(df['date'])

# Plot the gold price trend
plt.figure(figsize=(12, 6))
sns.lineplot(data=df, x='date', y='price', marker='o', color='gold')

# Customize the plot
plt.title("Gold Price Over Time", fontsize=14)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Price (USD)", fontsize=12)
plt.xticks(rotation=45)
plt.grid(True)

# Show the plot
plt.show()
