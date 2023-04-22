# CSV to MySQL Table

This Python script reads a CSV file into a pandas DataFrame, cleans the column names, converts all columns to string datatype, creates a MySQL table schema, and inserts the data from the DataFrame into a MySQL table. 

## Requirements

- Python 3.x
- pandas
- numpy
- pymysql

## Installation

1. Clone the repository.
2. Install the required libraries using `pip install -r requirements.txt`.
3. Update the following variables in the script:
   - `Your_csv_path.csv`: path to the CSV file you want to import
   - `Your_username`: your MySQL username
   - `GXXXXXXXXXXXXXXXXG`: your MySQL password
   - `Your_DataBase_Name`: the name of your MySQL database
   - `mysql_host_name`: the hostname of your MySQL server
   - `Table_name`: the name of the table you want to create and insert data into
4. Run the script using `python csv_to_mysql.py`.

## Notes

- The script inserts data into the MySQL table in batches of 500 rows. If an error occurs during the batch insert, the script falls back to inserting one row at a time.
- Null values in the CSV file are replaced with the string 'None' before being inserted into the MySQL table.
- The script uses the text datatype for all columns in the MySQL table.
- The script suppresses warning messages that may be generated by pandas.
