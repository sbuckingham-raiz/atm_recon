import os
import logging
import datetime

import pandas as pd
from dotenv import load_dotenv
from openpyxl import load_workbook

from common.extract_data import get_sql_table
from common.utility import build_postgres_connection_string
from automations.RECON_file.build_recon_file import write_data, create_new_template

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('automations.log')

stream_handler.setLevel(logging.DEBUG)
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

def setup(): 
    global NCR_RECON_PATH

    global DBUSERNAME
    global DBPASSWORD
    global DBDATABASE
    global DBHOST
    global DBPORT 
    
    load_dotenv('./automations/.env')
    
    DBUSERNAME = os.getenv('DBUSERNAME')
    DBPASSWORD = os.getenv('DBPASSWORD')
    DBDATABASE = os.getenv('DBDATABASE')
    DBHOST = os.getenv('DBHOST')
    DBPORT = os.getenv('DBPORT')    
    NCR_RECON_PATH = os.getenv('NCR_RECON_PATH')
    
    if not os.path.exists(NCR_RECON_PATH):
        raise ValueError(f'{NCR_RECON_PATH} does not exist...')
    
def run_RECON_file():
    setup()
    logger.info('Starting RECON File automation...')
    pickDate = datetime.datetime.today() - datetime.timedelta(1)
    branchATMdf = get_sql_table('atm_locations')
    branches = branchATMdf['branch'].unique()
    year = pickDate.year
    totals_df = get_sql_table('totals')
    
    totals_df['date'] = pd.to_datetime(totals_df['date'])
    totals_df = totals_df[(totals_df['delta']!=0) & (totals_df['delta'].notna()) ]
    totals_df = totals_df[totals_df['device_name']!='Global Machines']
    totals_df = totals_df[['date', 'device_name', 'delta']]
    totals_df = totals_df.sort_values(by='date')
    totals_df['date'] = totals_df['date'].dt.date
    
    
    final_totals_dir = os.path.join(NCR_RECON_PATH, 'outages')
    os.makedirs(final_totals_dir, exist_ok=True)
    totals_df.to_excel(os.path.join(final_totals_dir, 'outages.xlsx'), index = False)
    
    for branch in branches:
        ATMlist = branchATMdf.loc[branchATMdf['branch']==branch, 'device_name'].unique()
        fileName = os.path.join(NCR_RECON_PATH, str(year), branch + f'-{str(year)}-ATMAutomation.xlsx')        
        
        try:
            if not os.path.exists(os.path.dirname(fileName)):
                os.mkdir(os.path.dirname(fileName))
                logger.info(f'Directory for RECON automation {year} does not exist... creating directory...')

            if not os.path.exists(fileName):
                wb = create_new_template(pickDate, ATMlist, dates_after=datetime.date(2024, 8, 1))
                print(f'Created new template for {branch}...')
            else:
                wb = load_workbook(fileName)

            con = build_postgres_connection_string(DBUSERNAME, DBPASSWORD, DBHOST, DBDATABASE, DBPORT)

            total_df_sql = f'''select * from totals where extract(year from date) = {year} or date >= '12/31/{year-1}'::date  '''
            start_end_amounts_sql = f'''select * from start_end_amounts where extract(year from date) = {year} or date >= '12/31/{year-1}'::date  '''
            device_amounts_sql = f'''select * from device_amounts where extract(year from date) = {year} or date >= '12/31/{year-1}'::date  '''

            total_df = pd.read_sql(total_df_sql, con)
            start_end_amounts_df = pd.read_sql(start_end_amounts_sql, con)
            device_amounts_df = pd.read_sql(device_amounts_sql, con)

            workbook = write_data(total_df=total_df, start_end_amts=start_end_amounts_df, device_amts_df=device_amounts_df, workbook=wb, current_date=pickDate)
            workbook.save(fileName)
            workbook.close()

            logger.info(f'Completed RECON file automation. {fileName} has been saved.')
        except Exception as e:
            logger.error(f'Error running recon file for {fileName}. \n{e}')