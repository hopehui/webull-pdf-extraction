# import packages
import pandas as pd
import numpy as np
import pdfplumber
import re
import os
import secrets

# take output file/dir path args from user
directory = input('Enter directory path: ')
csv_filename = input('Enter csv output file name: ')

print('Extracting data...')

final_df = pd.DataFrame() # instantiate empty dataframe for final table for a dir

# loop through each file in dir
for file in os.listdir(directory):
    file_path = os.path.join(directory, file)
    
    file_df = pd.DataFrame() # instantiate empty dataframe for final table for a file

    # search for pdf files only
    if file_path.endswith('.pdf'):
        pdf = pdfplumber.open(file_path) # open pdf file

        x = 0 # counter

        # loop through each page in the pdf
        for page in pdf.pages:
            table = page.extract_table() # attempt to extract table

            # if table is found, process table into correct format
            if table != None:

                if x == 0:
                    df = pd.DataFrame(table[1:], columns = table[0])
                    table_header = df.columns
                    x += 1 # increase counter
                
                else:
                    df = pd.DataFrame(table, columns = table_header)
                    x += 1 # increase counter

                df = df.rename(columns = {df.columns[-4]:'value'}) # fill missing column name
                df.loc[df['Comm/Fee/Tax'] == '0.00', 'value'] = 0.00 # replace 'None' with 0
                df.loc[:, 'date'] = df['Trade Date'] + ' ' + df['Time'] + '00' # create date column

                # reorder and rename columns
                df = df[['Symbol & Name', 'date', 'Buy/Sell', 'Quantity', 'Traded Price', 'Comm/Fee/Tax', 'value']]
                df = df.rename(columns = {'Symbol & Name': 'symbol', 'Buy/Sell': 'side', 'Quantity': 'quantity', 'Traded Price': 'price'})
                
                # adjust symbol column
                df.loc[:, 'symbol'] = df['symbol'].str.extract('(^.+(?=\\n))')

                # adjust date column
                df['date'] = df['date'].str.replace('GMT', '')
                df.loc[:, 'date'] = pd.to_datetime(df['date'], format = '%d/%m/%Y %H:%M:%S,%z').dt.tz_convert('UTC')
                df.loc[:, 'date'] = df.loc[:, 'date'].apply(lambda x: x.isoformat().replace('+00:00', 'Z'))
                df['date'] = np.where(df['date'] == 'NaT', None, df['date'])

                # create unique identifier for each entry
                df['unique_id'] = df['symbol'].apply(lambda x: secrets.token_hex(8) if pd.notna(x) else None)
                df = df[['unique_id', 'symbol', 'date', 'side', 'quantity', 'price', 'Comm/Fee/Tax', 'value']]                

                # forward fill in preparation for pivot
                df.iloc[:, :-2] = df.iloc[:, :-2].ffill()

                # pivot table
                df_p = df.pivot(index = df.columns[:-2].to_list(), columns = 'Comm/Fee/Tax', values = 'value').reset_index()
                df_p.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df_p.columns]

                # replace NaNs with 0 for SEC and TAF
                df_p['SEC'] = df_p['SEC'].fillna(0)
                df_p['TAF'] = df_p['TAF'].fillna(0)

                # reorder and rename columns
                df_p = df_p[['symbol', 'date', 'side', 'quantity', 'price', 'SEC', 'TAF']]
                df_p = df_p.rename(columns = {'SEC': 'commission', 'TAF': 'fees'})

                # append the page df to the file df
                file_df = pd.concat([file_df, df_p], ignore_index = True)

    # append the file df to the dir df
    final_df = pd.concat([final_df, file_df], ignore_index = True)

# sort and reindex the dir df
final_df.sort_values(by = ['date'], inplace = True)
final_df.reset_index(drop = True, inplace = True)

print('...data extraction complete')

# save to csv
final_df.to_csv(f'{directory}/{csv_filename}.csv')
print('CSV file saved')