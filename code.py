# CNE 340 Final Project
# Project Name: Historic Gold Price Data Analysis
# Due date: 03/18/2025
# Project Team : 1- Wahaj AL Obid, 2- Aaron Henson, 3- Lidsyda Nouanphachan
import pandas as pd
import numpy as np
import mysql.connector
import requests
import time

def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='',
                                  host='127.0.0.1',
                                  database='cne340_gold')
    return conn

def main():
    conn = connect_to_sql()
    #Further code for data analysis will go here.
    print("connected to the database successfully!")
    conn.close()
if __name__=="__main__":
    main()
