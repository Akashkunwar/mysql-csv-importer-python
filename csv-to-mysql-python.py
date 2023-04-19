# Importing necessary libraries
import pymysql
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('Your_csv_path.csv')

# Name of the MySQL table you want to create and insert data into
table_name = 'Table_name'

# Clean column names
df.columns = df.columns.str.strip()
new_columns = {col: col.replace(' ', '_') for col in df.columns}
df = df.rename(columns=new_columns)

# Convert all columns to string datatype
df = df.astype(str)

# Get list of column names
col = list(df.columns)

# Create the table schema
syn = ''
for x in col:
  syn = syn +"`"+ x +"`"+ ' TEXT NULL,'
schema = 'CREATE TABLE '+ table_name + ' ( '+syn[:-1]+");"

# Connect to the MySQL database
conn = pymysql.connect(user='Your_username',
                       password='GXXXXXXXXXXXXXXXXG',
                       database='Your_DataBase_Name',
                       connect_timeout=6000,
                       host='mysql_host_name',
                       ssl={'ca': 'your_mysql_cert_path'})

# Try to create the table
try:
  with conn.cursor() as cursor:
      cursor.execute(schema)
except:
  pass

# Replace all null values with 'None'
df = df.fillna('None')

# Batch insert the data into MySQL table
try:
  batch_size = 500
  rows = []
  cnt = len(df[col[0]])
  now = 0
  cursor = conn.cursor()
  for index, row in df.iterrows():
      rows.append(tuple(row))
      if len(rows) == batch_size:
          sql = "INSERT INTO {} VALUES ({})".format(table_name, ','.join(['%s']*len(df.columns)))
          cursor.executemany(sql, rows)
          now = now+500
          print("From ", cnt,'Total : ', now, 'is inserted.')
          conn.commit()
          rows = []
  if rows:
      cursor.executemany(sql, rows)
      now = now+500
      print("From ", cnt,'Total : ', now, 'is inserted.')
      conn.commit()
except:
  cnt = len(df[col[0]])
  now = 0
  cursor = conn.cursor()
  for index, row in df.iterrows():
      sql = "INSERT INTO {} VALUES ({})".format(table_name, ','.join(['%s']*len(df.columns)))
      cursor.execute(sql, tuple(row))
      now = now+1
      print("From ", cnt,'Total : ', now, 'is inserted.')
      conn.commit()
      
print('CSV Imported to Mysql')
