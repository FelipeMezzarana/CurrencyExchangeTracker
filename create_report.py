import pandas as pd
import numpy as np
import re
import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def complete_table_df(db_path:str,table_name:str):
    """Return a df with the complete specified table
    """
    
    conn_lite = sqlite3.connect(db_path)
    query = ('SELECT '
             '  * '
             'FROM '
             f'   {table_name}')
    df = pd.read_sql_query(query, conn_lite) 
    conn_lite.close()
    
    return df

def historical_line_plot(currency_df:pd.DataFrame,currency_code:str,save_path:str = 'my_fig.png'):
    """Saves an image with a line plot of the last 12 months average, max and min currency rates 
    * Base Currency will be defined by the df currency_df
    * currency_code may be any of the 273 currencies available
    """   
    
    # Identifies base currency (dollar or euro)
    if currency_df.usd_united_states_dollar.mean() == 1:
        base_currency = 'Dollar'
    else:
        base_currency = 'Euro'
    
    # Identifies correct column from code
    for col in currency_df.columns:
        if re.search(f'^{currency_code}',col):
            correct_col = col
            break
            
    # Group data
    currency_df.exchange_date = pd.to_datetime(currency_df.exchange_date)
    grouped_currency_df = currency_df.groupby(
        pd.Grouper(key="exchange_date", freq="1M"))[correct_col].agg([np.mean,max,min])    
    grouped_currency_df['str_date'] = [date.strftime("%b, %Y") for date in grouped_currency_df.index] #prettify name
    # Loc last 12 monts
    grouped_currency_df = grouped_currency_df.iloc[::-1][:13] # Reverse df to get right order 
    grouped_currency_df = grouped_currency_df.iloc[::-1] # Reverse back
    
    # Plot historical data (last 12 months)
    sns.set_style('whitegrid')
    fig,ax1 = plt.subplots(figsize=(18,5))

    sns.lineplot(
        x=grouped_currency_df.str_date,
        y=grouped_currency_df['mean'],
        color ='#b2b2d9',
        lw =3, ax = ax1)

    ax1.set_title(f"{base_currency} x {currency_code.upper()} Variation",fontsize = 24, loc = 'left')
    ax1.set_xlabel("",fontsize=16, loc = 'left')
    ax1.set_ylabel(f"{correct_col.replace('_',' ').title()}",fontsize=16, loc ='top')
    ax1.tick_params(labelsize=16)
    ax1.xaxis.grid(False)
    sns.despine()
    ax1.fill_between(
        grouped_currency_df.str_date,
        grouped_currency_df['max'],
        grouped_currency_df['min'],
        color="grey",
        alpha=0.12,
        label=r"Máx & Mín Values") 
    plt.legend(fontsize = 16,loc='best')
    plt.xlim(0,12)
    
    plt.savefig(save_path,facecolor= '#ffffff',edgecolor = '#ffffff',bbox_inches ='tight') 
    
    
if __name__ == '__main__':
    dollar_df = complete_table_df(
        'Database/currency_exchange_db.db',
        'dollar_based_currency')
    historical_line_plot(dollar_df,'brl')