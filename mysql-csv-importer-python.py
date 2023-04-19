!pip install airtable-python-wrapper --q
!pip install pymysql --q

# CSV to DB
## Airtable to DB
# Importing liberaries
import pymysql
import pandas as pd
from airtable import Airtable
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Insert details
df = pd.read_csv('Your_csv_path.csv')
table_name = 'Table_name'

df.columns = df.columns.str.strip()
new_columns = {col: col.replace(' ', '_') for col in df.columns}
df = df.rename(columns=new_columns)
df = df.astype(str)
col = list(df.columns)

syn = ''
for x in col:
  syn = syn +"`"+ x +"`"+ ' TEXT NULL,'
schema = 'CREATE TABLE '+ table_name + ' ( '+syn[:-1]+");"

conn = pymysql.connect(user='Your_username',
                       password='GXXXXXXXXXXXXXXXXG',
                       database='Your_DataBase_Name',
                       connect_timeout=6000,
                       host='mysql-123.mysql.database.gcp.com',
                       ssl={'ca': '/content/DigiCertGlobalRootCA.crt.pem'})

try:
  with conn.cursor() as cursor:
      cursor.execute(schema)
except:
  pass

df = df.fillna('None')

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
