import pandas as pd
import numpy as np
import re
import os
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
    if currency_df.usd.mean() == 1:
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
    # Save to disk
    plt.savefig(save_path,facecolor= '#ffffff',edgecolor = '#ffffff',bbox_inches ='tight') 
    

def specific_info_df(dollar_df:pd.DataFrame,euro_df:pd.DataFrame,currency_code:str):
    """Return a simple df with information regarding maximum and minimum values and currency name
    * currency_code may be any of the 273 currencies available
    """   
    
    # Identifies correct column from code
    for col in dollar_df.columns:
        if re.search(f'^{currency_code}',col):
            correct_col = col
            break
    
    # Create Features for each base currency
    infos_df = pd.DataFrame({'info':[
        'Current Rate',
        'Last Week Range',
        'Last Month Range',
        'Last 3 Months Range',
        'Last 6 Months Range',
        'Last Year Range']})
    for currency_df,base_currency in zip([dollar_df,euro_df],['Dollar','Euro']):
        specific_df = currency_df.loc[:,['exchange_date',correct_col]]
        specific_df_reverse = specific_df.iloc[::-1]    

        current_date = pd.Timestamp(specific_df_reverse.exchange_date.values[0]).strftime('%Y-%d-%m')
        current_rate = specific_df_reverse[correct_col].values[0]
        # Last week
        max_week = max((specific_df_reverse[:7][correct_col]))
        min_week = min((specific_df_reverse[:7][correct_col]))
        # Last month
        max_month = max((specific_df_reverse[:30][correct_col]))
        min_month = min((specific_df_reverse[:30][correct_col]))
        # Last 3 month
        max_tri = max((specific_df_reverse[:90][correct_col]))
        min_tri = min((specific_df_reverse[:90][correct_col]))  
        # Last 6 month
        max_seme = max((specific_df_reverse[:180][correct_col]))
        min_seme = min((specific_df_reverse[:180][correct_col]))  
        # Last 12 month
        max_year = max((specific_df_reverse[:360][correct_col]))
        min_year = min((specific_df_reverse[:360][correct_col]))   
        # Add Features
        infos_df[f'{base_currency} Based Rate'] = [
            f'{current_rate:.2f}',
            f'{min_week:.2f} - {max_week:.2f}',
            f'{min_month:.2f} - {max_month:.2f}',
            f'{min_tri:.2f} - {max_tri:.2f}',
            f'{min_seme:.2f} - {max_seme:.2f}',
            f'{min_year:.2f} - {max_year:.2f}']
    
    infos_df.set_index('info',inplace = True)
    infos_df.index.name = current_date
    # Currency name will be used to generate Excel
    currency_name = re.sub('^[^_]+_','',correct_col).replace('_',' ').title()
    return infos_df,currency_name
    

def generate_excel_report(dollar_df:pd.DataFrame,euro_df:pd.DataFrame,currency_list:list,file_path:str = ''):
    """Generates Excel with a tab for each currency listed in currency_list
    """  
    
    # Create file
    file_name =  file_path + 'Exchange Rate Report ' + datetime.today().strftime('%Y-%d-%m') + '.xlsx'
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    workbook = writer.book

    for currency_code in currency_list:
        infos_df,currency_name = specific_info_df(dollar_df,euro_df,currency_code)    
        infos_df.to_excel(writer, currency_code.upper() +' ('+currency_name + ') - Report', startrow = 1) 
        my_sheet = writer.sheets[currency_code.upper() +' ('+currency_name + ') - Report']
        # Formatting
        my_sheet.set_column(0, infos_df.shape[1], 18) # Cols width
        my_sheet.hide_gridlines(2)# Hide gridline
        # Add Boarders, round and centralize
        cell_format = workbook.add_format(
            {'border': 1,'align': 'center','valign': 'vcenter'})
        # Information needs to be rewritten
        for row in range(0,len(infos_df)):
            my_sheet.write(row+2,1, infos_df.iloc[row,0],  cell_format)
            my_sheet.write(row+2,2, infos_df.iloc[row,1],  cell_format)
        # Currency name in first row
        merge_format = workbook.add_format(
            {'bold': 1,'border': 1,'align': 'center','valign': 'vcenter','fg_color': '#b2b2d9'})
        my_sheet.merge_range('A1:C1', currency_name, merge_format)
        
        # Create and Insert image
        # Dollar
        historical_line_plot(dollar_df,currency_code,save_path = 'dollar'+currency_code+'.png')
        my_sheet.insert_image('E2', r'dollar'+currency_code+'.png',{'x_scale': 0.55, 'y_scale': 0.55,'x_offset':1})
        # Euro
        historical_line_plot(euro_df,currency_code,save_path = 'euro'+currency_code+'.png')
        my_sheet.insert_image('E15', r'euro'+currency_code+'.png',{'x_scale': 0.55, 'y_scale': 0.55,'x_offset':1})
    workbook.close()
    
    # Delete images
    for currency_code in currency_list:
        os.remove('dollar'+currency_code+'.png')
        os.remove('euro'+currency_code+'.png')


def report_pipeline():
    """Run necessary steps to generate a report in Excel
    """
    # Load Tables
    dollar_df = complete_table_df(
        'src/database/currency_exchange_db.db',
        'dollar_based_currency')
    euro_df = complete_table_df(
        'src/database/currency_exchange_db.db',
        'euro_based_currency')
    # Generate Excel
    generate_excel_report(
        dollar_df,
        euro_df,
        currency_list = ['dkk','brl','jpy','gbp','cny'], # Add desired currencies here
        file_path = r'src/reports/')
    
