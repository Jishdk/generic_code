# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import pandas as pd
import requests
import sqlite3

# Declaring entities
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
log_file = './code_log.txt'
output_path = './Largest_banks_data.csv'

# Define table parameters
table_name = 'Largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # Loading the page
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Creating a dataframe
    df = pd.DataFrame(columns=table_attribs)

    # Scraping of required information
    table = soup.find_all('tbody')
    rows = table[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Name': col[1].get_text().strip(),
                         'MC_USD_Billion': col[2].get_text().strip()}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Read CSV
    dataframe = pd.read_csv(csv_path)

    # Convert to dictionary
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']


    # Currency conversion
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * exchange_rate['EUR']
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    # Saving as csv
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Function calls

# Extract function
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

# Transform function
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)

#Show 5th largest bank in EUR
print(df['MC_EUR_Billion'][4])

# Load function
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

#SQL connection
sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# Query statemens

# List of query statements
query_statements = [
    f"SELECT * FROM {table_name}",
    f"SELECT AVG(MC_GBP_Billion) FROM {table_name}",
    f"SELECT Name FROM {table_name} LIMIT 5"
]

# Execute each query
for query in query_statements:
    run_query(query, sql_connection)


log_progress('Process Complete.')

# Close connection
sql_connection.close()