import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import re
import numpy as np
from time import perf_counter


def create_table_currency_exchange(db_path:str = '',table_name:str = '',only_df:bool = False):
    """ Create an empty table in a SQL Lite DB with a column for each currency available in the API
    * API: https://github.com/fawazahmed0/currency-api
    * If only_df == True return the df without creating a table in the db
    """
    
    # Retrieve a json with all available currencies
    url_all_currencies = 'https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies.json'
    resp = requests.get(url_all_currencies)
    all_currencies_json = resp.json()
    
    # Create column names: currencyCode_currencyName (in snake_case)
    currency_code_list = list(all_currencies_json)
    currency_name_list = [name.lower().replace(' ','_').strip() for name in  all_currencies_json.values()]
    cols_names = [name + '_' + code for name,code in zip(currency_code_list,currency_name_list)]
    
    # Create empty df
    empty_currency_df = pd.DataFrame(columns = ['exchange_date'] +cols_names)
    if only_df == True:
        return empty_currency_df
    
    # Define the data types for each col
    dtypes_dict = {'exchange_date':'DATE'}
    for col in cols_names: 
        dtypes_dict.update({col:'FLOAT'})
    
    # Create table
    conn_lite = sqlite3.connect(db_path)
    empty_currency_df.to_sql(name = table_name,
                             con = conn_lite,
                             if_exists='fail',
                             index=False, 
                             dtype=dtypes_dict
                            )
    conn_lite.close()
    print(f'Table {table_name} created!')


def last_exchange_date(db_path:str,table_name:str):
    """ Return the last date added in the db
    """
    
    conn_lite = sqlite3.connect(db_path)
    query = ('SELECT '
             '   max(exchange_date) as last_update_date '
             'FROM '
             f'   {table_name}')
    max_date_df = pd.read_sql_query(query, conn_lite) 
    conn_lite.close()
    
    last_update_date_str = max_date_df.last_update_date[0]
    
    if last_update_date_str == None:
        # one year ago - Historical rates are only available for last 1 year  
        last_update_date = (datetime.today() - timedelta(days=365) )#.strftime('%Y-%m-%d')
    else:
        last_update_date = datetime.strptime(last_update_date_str, '%Y-%m-%d')
    
    return last_update_date


def get_currency_exchange(db_path:str,table_name:str,based_currency:str,since_date:datetime = None):
    """ Return a one row DataFrame with all the 
    based_currency -- may be 'usd' or 'eur'
    """
    
    t_start = perf_counter() # time counter
    # Retrieves the date of the last update in the table
    if since_date == None:
        since_date = last_exchange_date(db_path,table_name)
    
    apiVersion = '1'
    endpoint = f'currencies/{based_currency}.json'

    currency_df = create_table_currency_exchange(only_df = True)
    row_counter = 0
    request_date = None
    while request_date != datetime.today().strftime('%Y-%m-%d'):
        since_date = (since_date + timedelta(days=1) ) # Sum one day
        request_date = since_date.strftime('%Y-%m-%d') # convert to str (request format)
        url = f'https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@{apiVersion}/{request_date}/{endpoint}'
        req = requests.get(url)
        # data is missing for a few days, in these cases we will take the value of the previous day
        if req.status_code != 200:
            request_date = (since_date - timedelta(days=1)).strftime('%Y-%m-%d') # convert to str (request format)
            url = f'https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@{apiVersion}/{request_date}/{endpoint}'
            req = requests.get(url)
            currencies_dict = req.json()   
        else:
            currencies_dict = req.json()   
        
        # Creates a list with the correct values of each currency for each column
        currency_values_list = [request_date] # first col refers to request date
        for col in currency_df.columns[1:]:
            currency_code = re.findall('[^_]*',col)[0]
            currency_value = currencies_dict.get(based_currency).get(currency_code)
            if currency_value == None:
                currency_values_list.append(np.nan)
            else:
                currency_values_list.append(currency_value)
                
        currency_df.loc[row_counter] = currency_values_list
        row_counter+=1
    
    t_end = perf_counter()
    print(f'Df generated for {based_currency} based currency '
          f'with {len(currency_df)} recorded days.\n{t_end - t_start:.2f}s')

    return currency_df


def insert_df_sql_lite(df:pd.DataFrame, db_path:str,table_name:str):
    """ Insert a df into the specified db and table
    """
    
    conn_lite = sqlite3.connect(db_path)
    df.to_sql(
        name = table_name,
        con = conn_lite,
        if_exists='append',
        index=False, 
        method = 'multi'
        )
    conn_lite.close()
    
    
def main(create_tables:bool = False):
    """ ETL pipeline to update db
    """ 

    if create_tables == True:
        # creates a table for dollar-based exchange rates (run ony once)
        create_table_currency_exchange(
            db_path= 'currency_exchange_db.db',
            table_name = 'dollar_based_currency')
        # creates a table for euro-based exchange rates (run ony once)
        create_table_currency_exchange(
            db_path= 'currency_exchange_db.db',
            table_name = 'euro_based_currency')   

    # Returns a df with all exchange rates since the last db update and update db (Dollar based)
    dollar_currency_df = get_currency_exchange(
        db_path= 'currency_exchange_db.db',
        table_name = 'dollar_based_currency',
        based_currency= 'usd')
    insert_df_sql_lite(
        df = dollar_currency_df,
        db_path = 'currency_exchange_db.db',
        table_name = 'dollar_based_currency')
    
    # Returns a df with all exchange rates since the last db update and update db (Euro based)
    euro_currency_df = get_currency_exchange(
        db_path= 'currency_exchange_db.db',
        table_name = 'euro_based_currency',
        based_currency= 'eur')
    insert_df_sql_lite(
        df = euro_currency_df,
        db_path = 'currency_exchange_db.db',
        table_name = 'euro_based_currency')


if __name__ == '__main__':
    main(create_tables = True)