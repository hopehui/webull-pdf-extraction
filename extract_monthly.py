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
        final_page = False

        # define start page of trade records table
        for page in pdf.pages:
            if 'TRADE RECORDS' in page.extract_text():
                start_page = page.page_number - 1

        # loop through each page in pdf, starting from trade record table page
        for i in range(start_page, len(pdf.pages) - 1):
            if final_page == True:
                break
            
            # single table on page
            if len(pdf.pages[i].find_tables()) == 1:
                table = pdf.pages[i].extract_table()

                if x == 0:
                    df = pd.DataFrame(table[1:], columns = table[0])
                    table_header = df.columns
                    x += 1 # increase counter
                
                else:
                    # check for new table
                    if 'Symbol & Name' in table[0]:
                        break

                    else:
                        df = pd.DataFrame(table, columns = table_header)
                        x += 1 # increase counter


            # multiple tables on page
            elif len(pdf.pages[i].find_tables()) > 1:
                tables = pdf.pages[i].find_tables()

                # determine if top table is continuation of trade records table or new table by checking header
                if 'Symbol & Name' in tables[0].extract()[0]:
                    break

                else:
                    df = pd.DataFrame(tables[0].extract(), columns = table_header)
                    x += 1 # increase counter
                    final_page = True

            else:
                break

            # transform table
            df.loc[:, 'date'] = df['Trade Date'] + ' ' + df['Time'] + '00' # create date column
            df.loc[:, 'symbol'] = df['Symbol & Name'].str.extract('(^.+(?=\\n))') # adjust symbol column
            df['commission'] = 0.0 # set commission column to 0

            # reorder and rename columns
            df = df[['symbol', 'date', 'Buy/Sell', 'Quantity', 'Traded Price', 'commission', 'Comm/Fee/Tax']]
            df = df.rename(columns = {'Buy/Sell': 'side', 'Quantity': 'quantity', 'Traded Price': 'price', 'Comm/Fee/Tax': 'fees'})

            # adjust date column
            df['date'] = df['date'].str.replace('GMT', '')
            df.loc[:, 'date'] = pd.to_datetime(df['date'], format = '%d/%m/%Y %H:%M:%S,%z').dt.tz_convert('UTC')
            df.loc[:, 'date'] = df.loc[:, 'date'].apply(lambda x: x.isoformat().replace('+00:00', 'Z'))
            df['date'] = np.where(df['date'] == 'NaT', None, df['date'])

            # sort and reindex table
            df.sort_values(by = ['date'], inplace = True)
            df.reset_index(drop = True, inplace = True)

            # append the page df to the file df
            file_df = pd.concat([file_df, df], ignore_index = True)

    # append the file df to the dir df
    final_df = pd.concat([final_df, file_df], ignore_index = True)

# sort and reindex the dir df
final_df.sort_values(by = ['date'], inplace = True)
final_df.reset_index(drop = True, inplace = True)

print('...data extraction complete')

# save to csv
final_df.to_csv(f'{directory}/{csv_filename}.csv')
print('CSV file saved')