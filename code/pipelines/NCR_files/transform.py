import os
from datetime import datetime

import pandas as pd

from common.utility import split_preserve_quotes
from errors.FileAlreadyLoaded import FileAlreadyLoaded
from errors.WrongDateError import WrongDateError

def transform_ncr_file(source_file:str, dates_loaded:list)->pd.DataFrame: 
    device_types = ['Check Scanner', 'Cash Acceptor', 'Cash Dispenser', 'Coin Dispenser']
    post_types = ['Teller', 'ATM', 'Banker', 'CIT']
    file_name = os.path.basename(source_file)
    with open(source_file, 'r', encoding='utf-8') as openfile:
        lines = openfile.readlines()

    total_cash = {
        'device_name':[],
        'date':[],
        'amount':[],
        'file_name':[],
        }
    
    amount_change = {
        'device_type':[],
        'post_type':[],
        'device_name':[],
        'date':[],
        'amount_increased':[],
        'amount_decreased':[],
        'file_name':[],
        }
    
    start_end_amount = {
        'device_type':[],
        'post_type':[],
        'device_name':[],
        'date':[],
        'starting_amt':[],
        'ending_amt':[],
        'file_name':[],        
    }
    
    for idx, line in enumerate(lines):

        if idx == 1:
            try:
                test_time(line.split())
                end_date = datetime.strptime(line.split()[4], '%m/%d/%Y').strftime('%Y-%m-%d').strip()    
                if end_date in dates_loaded:
                    raise FileAlreadyLoaded(f'Date for file already loaded. Date: {end_date}')
                
            except WrongDateError as e:
                raise WrongDateError(e)
                
            
        if 'Total Cash on Hand' in line:

            device_name = lines[idx-1:idx][0].split(',')[0].replace('\n', '')

            total_amount = float(split_preserve_quotes(lines[idx+1:idx+2][0] )[0].replace('"', '').replace(',', '')) 
            total_cash['amount'].append(total_amount)
            total_cash['device_name'].append(device_name)
            total_cash['date'].append(end_date)
            total_cash['file_name'].append(file_name)
            for new_idx, row in enumerate(lines[idx+1:], start=idx+1):

                if 'Total Cash on Hand' in row:
                    break
                
                device_type_cell = lines[new_idx:new_idx+1][0].split(',')[0].replace('\n', '')
                
                if device_type_cell in device_types:
                    
                    for final_idx, final_row in enumerate(lines[new_idx+1:], start=new_idx+1):
                        post_type_cell = lines[final_idx:final_idx+1][0].split(',')[0].replace('\n', '')
                        
                        if post_type_cell in post_types:
                            amount_change['device_name'].append(device_name)
                            amount_change['date'].append(end_date)
                            amount_change['device_type'].append(device_type_cell)
                            amount_change['post_type'].append(post_type_cell)
                            amount_change['amount_increased'].append(float(split_preserve_quotes(final_row)[2].replace('"', '').replace(',', '').replace('\n', '')))
                            amount_change['amount_decreased'].append(float(split_preserve_quotes(final_row)[4].replace('"', '').replace(',', '').replace('\n', '')))
                            amount_change['file_name'].append(file_name)
                            
                        if post_type_cell == 'Device Totals':
                            starting_amt = split_preserve_quotes(lines[final_idx+1:final_idx+2][0])[3].replace('"', '').replace(',', '').replace('\n', '')
                            ending_amt = split_preserve_quotes(lines[final_idx+1:final_idx+2][0])[4].replace('"', '').replace(',', '').replace('\n', '')
                            start_end_amount['device_name'].append(device_name)
                            start_end_amount['date'].append(end_date)
                            start_end_amount['device_type'].append(device_type_cell)
                            start_end_amount['post_type'].append(post_type_cell)
                            start_end_amount['starting_amt'].append(starting_amt)
                            start_end_amount['ending_amt'].append(ending_amt)
                            start_end_amount['file_name'].append(file_name)                            
                            break 
                        

                            
                            
    
    total_df = pd.DataFrame(total_cash)
    total_df['date'] = pd.to_datetime(total_df['date'])
    
    amount_df = pd.DataFrame(amount_change)
    amount_df['date'] = pd.to_datetime(amount_df['date'])
    agg_amount_df = amount_df[amount_df['device_type']!='Check Scanner'].groupby(['device_name', 'date'])[['amount_increased', 'amount_decreased']].sum().reset_index().rename(columns={'amount_increased':'total_amount_increased', 'amount_decreased':'total_amount_decreased'})    
    total_df = total_df.merge(agg_amount_df, on=['date', 'device_name'], how='left')
    # total_df['previous_amount'] = total_df.groupby(['device_name'])['amount'].shift(1)
    # total_df['estimated_amount'] = total_df['previous_amount'] + total_df['total_amount_increased'] - total_df['total_amount_decreased']
    # total_df['delta'] = total_df['amount'] - total_df['estimated_amount']
    
    start_end_amount_df = pd.DataFrame(start_end_amount)
    start_end_amount_df['starting_amt'] = pd.to_numeric(start_end_amount_df['starting_amt'])
    start_end_amount_df['ending_amt'] = pd.to_numeric(start_end_amount_df['ending_amt'])
    start_end_amount_df['date'] = pd.to_datetime(start_end_amount_df['date'])
    
    return (total_df, amount_df, start_end_amount_df)
        
def test_time(line:list[str])->bool:
    
    if len(line)!=7:
        raise WrongDateError('Test is expecting length of line to be equal to 7... returning false!')
    
    start_date = line[0].replace('"', '')
    start_time = line[1]
    start_timeofday = line[2]
    end_date = line[4]
    end_time = line[5]
    end_timeofday = line[6].replace('"', '')
    
    if 'PM' not in start_timeofday or 'PM' not in end_timeofday :
        raise WrongDateError('End of time day is not PM for the file...')
    
    if start_time.split(':')[0] != '6' or end_time.split(':')[0] != '6':
        raise WrongDateError('Hour is not 6 for the file...')

    if start_time.split(':')[1] != '30' or end_time.split(':')[1] != '30':
        raise WrongDateError('Minute is not 30 for the file...')        
        
    
    try:
        datetime.strptime(start_date, '%m/%d/%Y')
        datetime.strptime(end_date, '%m/%d/%Y')
        
    except ValueError:
        raise WrongDateError('Failed datetime conversion test!')
    
    return True