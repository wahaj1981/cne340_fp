import mysql.connector  # Library for MySQL database connection
import pandas as pd  # Library for data manipulation and analysis
import numpy as np  # Library for numerical computations
from sqlalchemy import create_engine, text  # Library for SQL operations
import matplotlib.pyplot as plt  # Library for data visualization

# Database credentials
uname = 'root'
pwd = ''
hostname = '127.0.0.1'
dbname = 'gold'

# Function to connect to MySQL database
def connect_to_sql():
    conn = mysql.connector.connect(user=uname, password=pwd,
                                   host=hostname,
                                   database=dbname)
    return conn

# Function to create the database if it doesn't exist
def create_database():
    conn = mysql.connector.connect(user=uname, password=pwd, host=hostname)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS gold")  # SQL query to create the database
    cursor.close()
    conn.close()

# Main function to execute database operations
def main():
    create_database()  # Ensure the database exists

    # Connect to the MySQL database using SQLAlchemy
    engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")

    # Read the Excel file from the given URL
    tables = pd.read_excel(
        'https://auronum.co.uk/wp-content/uploads/2024/09/Auronum-Historic-Gold-Price-Data-5.xlsx',
        sheet_name='Sheet1',
        engine='openpyxl'
    )

    # Print column names to inspect structure before renaming
    print("Column names before renaming:", tables.columns)

    # Rename columns to meaningful names based on actual structure
    tables.rename(columns={
        "USD/Gold": "Date",  # Corrected column name to match the actual 'USD/Gold'
        "Unnamed: 5": "Gold_Price",
        "Unnamed: 8": "Silver_Price",
        "Unnamed: 11": "Platinum_Price"

    }, inplace=True)

    # Print column names after renaming
    print("Column names after renaming:", tables.columns)

    # Select only relevant columns
    tables = tables[['Date', 'Gold_Price']]
    tables.columns = ['Date', 'Gold_Price']

    # Convert 'Date' column to datetime format
    tables['Date'] = pd.to_datetime(tables['Date'], errors='coerce')


    # Reset index for better organization
    tables.reset_index(drop=True, inplace=True)

    # Connect to database and insert data
    connection = engine.connect()
    tables.to_sql('gold', con=engine, if_exists='append', index=False)  # Append data to 'gold' table

    # Create a temporary table and manipulate data
    connection.execute(text('CREATE TABLE gold_temp_2 LIKE gold'))

    # Insert unique records into temporary table to remove duplicates
    connection.execute(text('INSERT INTO gold_temp_2 SELECT DISTINCT `Date`, `Gold_Price` FROM gold'))
    connection.execute(text('DROP TABLE gold'))  # Remove old table
    connection.execute(text('ALTER TABLE gold_temp_2 RENAME TO gold'))  # Rename temp table to original name

    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()

# Connect to the database to fetch data for visualization
conn = connect_to_sql()
cursor = conn.cursor()

# Execute SQL query to retrieve all data from the 'gold' table
cursor.execute("SELECT * FROM gold")
rows = cursor.fetchall()

# Close the database connection
cursor.close()
conn.close()

#######################################################################################

# Convert query results into a pandas DataFrame
df = pd.DataFrame(rows, columns=['Date', 'Gold_Price'])
print(df)

# Convert 'Gold_Price' column to numeric format
df['Gold_Price'] = pd.to_numeric(df['Gold_Price'], errors='coerce')
print(df)
print(df.head)

# Set 'Date' column as the index
df.set_index('Date', inplace=True)

# Resample the data annually and calculate the average gold price per year
yearly_avg = df['Gold_Price'].resample('YE').mean()
print(yearly_avg)

########################################################################################

# Plot 1: Line chart showing gold price over time
plt.figure(figsize=(10, 5))
plt.plot(df.index, df['Gold_Price'], marker='o', linestyle='-', color='b', label='Gold_Price')
plt.xlabel('Date')
plt.ylabel('Gold_Price')
plt.title('Gold Price Over Time')
plt.legend()
plt.grid()
plt.show()

# Plot 2: Bar chart showing gold price for each date
# Filter data for the year 2019-2020
df_2020 = df[(df.index >= '2019-01-01') & (df.index <= '2020-12-31')]

# Plot: Bar chart showing gold price for each date in 2020
plt.figure(figsize=(10, 5))
plt.bar(df_2020.index, df_2020['Gold_Price'], color='g', label='Gold_Price')
plt.xlabel('Date')
plt.ylabel('Gold_Price')
plt.title('Gold Price for Each Date in 2020')
plt.legend()
plt.grid(axis='y')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.show()

# pie 3 chart
# Filter the data to include only the years from 2015 to 2024
yearly_avg_filtered = yearly_avg[(yearly_avg.index.year >= 2015) & (yearly_avg.index.year <= 2024)]

# Sort the filtered data
yearly_avg_filtered = yearly_avg_filtered.sort_values()

# Generate a color palette using viridis colormap
colors = plt.cm.viridis(np.linspace(0, 1, len(yearly_avg_filtered)))

# Plotting the pie chart
plt.pie(yearly_avg_filtered, labels=yearly_avg_filtered.index.year, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title("Gold Price Distribution from 2015 to 2024")
plt.show()
