import os
import logging

from dotenv import load_dotenv

import pandas as pd

from common.extract_data import get_sql_table
from errors.WrongDateError import WrongDateError
from errors.FileAlreadyLoaded import FileAlreadyLoaded
from common.logging.log_decorator import log_decorator_args
from common.utility import get_all_files, get_distinct_list
from pipelines.NCR_files.transform import transform_ncr_file


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('pipeline.log')

stream_handler.setLevel(logging.DEBUG)
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

def run_NCR_files():
    setup()
    load_ncr_files()
    load_atm_locations()
    add_final_cols()


def verify():
    pass

@log_decorator_args(function_type='pipeline', my_logger=logger, timer=True)
def add_final_cols():
    df = get_sql_table('totals')
    df = df.sort_values(by='date', ascending=True)
    df['previous_amount'] = df.groupby(['device_name'])['amount'].shift(1)
    df['estimated_amount'] = df['previous_amount'] + df['total_amount_increased'] - df['total_amount_decreased']
    df['delta'] = df['amount'] - df['estimated_amount']
    df.to_sql('totals', con=dburl, index=False, if_exists='replace')

@log_decorator_args(function_type='pipeline', my_logger=logger, timer=True)
def load_atm_locations():
    file_name = os.path.join(NCR_ATM_LOCATIONS_SOURCE_DIRECTORY, 'atm_locations.xlsx')
    
    (
        pd.read_excel(file_name, sheet_name='atm_locations')
        .assign(gl_number = lambda x: pd.to_numeric(x['gl_number']))
        .assign(gl_number = lambda x: pd.to_numeric(x['gl_number']))
        .assign(cashbox_nbr = lambda x: pd.to_numeric(x['cashbox_nbr']))
        .assign(branch = lambda x: x['branch'].str.strip())
        .assign(device_name = lambda x: x['device_name'].str.strip())
        .to_sql('atm_locations', con=dburl, index=False, if_exists='replace')
    )
    
    (
        pd.read_excel(file_name, sheet_name='atm_owners')
        .to_sql('atm_owners', con=dburl, index=False, if_exists='replace')
    )

@log_decorator_args(function_type='pipeline', my_logger=logger, timer=True)   
def load_ncr_files():
    # sourcefiles = get_all_files(source_directory=NCR_SOURCE_DIRECTORY, filter_for='Global Machines Summary (with ATM)')
    sourcefiles = []
    for root, _, files in os.walk(NCR_SOURCE_DIRECTORY):
        for file in files:
            filename = os.path.join(root, file)
            file_base_name = os.path.basename(filename)
            
            if file_base_name.endswith('.csv'):
                if file_base_name.startswith('Global Machines Summary (with ATM)'):
                    sourcefiles.append(filename)
    
    logger.info(f'Loading {len(sourcefiles)} files...')
    successful = 0
    filesTried = 0
    for file in sourcefiles:
        filesTried += 1 
        try:
            if filesTried == 1:
                dates_loaded = []
            
            total_df, amount_df, start_end_df = transform_ncr_file(file, dates_loaded)
            
            successful+=1
            
            if successful == 1:
                if_exist_value = 'replace'
                dates_loaded = []
            else:
                if_exist_value = 'append'

            total_df.to_sql('totals', con=dburl, index=False, if_exists=if_exist_value)
            amount_df.to_sql('device_amounts', con=dburl, index=False, if_exists=if_exist_value)
            start_end_df.to_sql('start_end_amounts', con=dburl, index=False, if_exists=if_exist_value)
            dates_loaded = get_distinct_list(connection_string=dburl, column_name='"date"', table_name='totals')            
            
        except WrongDateError as e:
            logger.debug(f'Date error with file: {file}, {e}')
            
        except FileAlreadyLoaded as e:
            logger.debug(f'File date has already been loaded for file {file}, {e}')
            
        except Exception as e:
            logger.error(f'Error running program for file: {file}\n{e}')
    logger.info(f'Successfully loaded {successful} files...')
    
@log_decorator_args(function_type='pipeline')    
def setup():
    env_path = './pipelines/.env'
    
    load_dotenv(env_path, override=True)
    
    global NCR_SOURCE_DIRECTORY
    global DBUSERNAME
    global DBPASSWORD
    global DBHOST
    global DBDATABASE
    global DBPORT
    global dburl
    global NCR_ATM_LOCATIONS_SOURCE_DIRECTORY 
    
    NCR_SOURCE_DIRECTORY = os.getenv('NCR_SOURCE_DIRECTORY', None)
    NCR_ATM_LOCATIONS_SOURCE_DIRECTORY = os.getenv('NCR_ATM_LOCATIONS_SOURCE_DIRECTORY', None)
    DBUSERNAME = os.getenv('DBUSERNAME', None)
    DBPASSWORD = os.getenv('DBPASSWORD', None)
    DBHOST = os.getenv('DBHOST', None)
    DBDATABASE = os.getenv('DBDATABASE', None)
    DBPORT = os.getenv('DBPORT', None)
    dburl = f'postgresql://{DBUSERNAME}:{DBPASSWORD}@{DBHOST}:{DBPORT}/{DBDATABASE}'

    check_if_mounted = [NCR_SOURCE_DIRECTORY, NCR_ATM_LOCATIONS_SOURCE_DIRECTORY]
    
    check_if_none = [NCR_SOURCE_DIRECTORY, DBUSERNAME, DBPASSWORD, DBHOST, DBDATABASE, DBPORT, dburl, NCR_ATM_LOCATIONS_SOURCE_DIRECTORY]
    for val in check_if_none:
        if not val:
            raise ValueError('Please instantiate all necessary environment variables.')
        
    for dir in check_if_mounted:
        if not os.path.exists(dir):
            raise ValueError(f'{dir} does not exist, or is not mounted...')
