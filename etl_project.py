# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Declaring entities
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
csv_path = '/home/project/Countries_by_GDP.csv'

# Define table parameters
table_name = 'Countries_by_GDP'
table_attribs = ['Country', 'GDP_USD_millions']


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    # Loading the webpage for Webscraping
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    # Creating a dataframe
    df = pd.DataFrame(columns=table_attribs)

    # Scraping of required information
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    # Creating a dataframe
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2].text:
                country_name = col[0].get_text().strip()
                gdp_value = col[2].get_text().strip()
                data_dict = {"Country": country_name,
                         "GDP_USD_millions": gdp_value}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # Create a list
    GDP_list = df["GDP_USD_millions"].tolist()

    # Conversion to list
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]

    # Rounding & Renaming
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})



    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')


# Function calls

# Extract function
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

# Transform function
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

# Load function
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

#SQL connection
sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# Query statement
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

# Close connection
sql_connection.close()