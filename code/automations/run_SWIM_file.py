import os
import logging
import shutil
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from dotenv import load_dotenv 
from jinja2 import Environment, FileSystemLoader

from common.logging import log_decorator
from common.send_email import send_email
from common.logging.log_decorator import log_decorator_args
from common.utility import build_postgres_connection_string
from common.extract_data import get_sql_table_by_date, get_sql_table, run_sql
from automations.SWIM_file.build_swim_file import build_swim_file, build_swim_filename, apply_device_type_logic, apply_delta_logic
from exchangelib import DELEGATE, Account, Credentials, Configuration, Message, Mailbox, IMPERSONATION, HTMLBody, FileAttachment, Body
env_path = './automations/.env'
load_dotenv(dotenv_path=env_path, override=True)

template_path = './automations/SWIM_file/templates'
environment = Environment(loader=FileSystemLoader(template_path))
template = environment.get_template('email.html')

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

global NCR_RECON_PATH
global NCR_RECON_SWIM_FILE_DIR
global DELTA_GL
global EMAIL_USERNAME
global EMAIL_PASSWORD

global DBUSERNAME
global DBPASSWORD
global DBDATABASE
global DBHOST
global DBPORT 
global DNAUPLOAD_PATH

NCR_RECON_PATH = os.getenv('NCR_RECON_PATH')
NCR_RECON_SWIM_FILE_DIR = os.path.join(NCR_RECON_PATH, 'SWIM_FILES')
# NCR_RECON_SWIM_FILE_DIR = r'Y:\ReportsRepo\accounting\ncr_recon_automation\SWIM_FILES'

DNAUPLOAD_PATH = os.getenv('DNAUPLOAD_PATH')
EMAIL_USERNAME = os.getenv('EMAIL_USERNAME')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
DBUSERNAME = os.getenv('DBUSERNAME')
DBPASSWORD = os.getenv('DBPASSWORD')
DBDATABASE = os.getenv('DBDATABASE')
DBHOST = os.getenv('DBHOST')
DBPORT = os.getenv('DBPORT')


DELTA_GL = 100020122535
today = datetime.today()

def run_SWIM_file():
    
    test_flag = False 
    dates = get_dates()
    
    test_device_names = ['TX004930-ESS', 'TX004931-ESS', 'TX004932-ESS']

    logger.info(f'Running SWIM file automation for {", ".join(date.strftime("%Y-%m-%d") for date in dates) }...')
    if test_flag:
        logger.info(f'Running in test mode with the following devices {", ".join(device for device in test_device_names) }...')
    
    dates.sort()
    
    df, delta_df, atm_locations_df, atm_owners_df = build_data(test_flag = test_flag, 
                                                               dates = dates,
                                                               test_device_names = test_device_names 
                                                               )
    final_swim_file(df = df)
    send_outage_emails(delta_df = delta_df, 
                       atm_locations_df = atm_locations_df, 
                       atm_owners_df = atm_owners_df
                       )


def get_dates(past_days_check:int = 7):
    '''
        Gets all dates that were posted, and ensures that every day in the past week was posted. If not, add it to dates to be processed. 
        Also adds all dates between yesterday and the max date posted already
    '''
    yesterday = datetime.today().date() - timedelta(days=1)
    day_check = []
    for x in range(past_days_check):
        day_check.append( yesterday - timedelta(days=x + 1) )

    sql = 'select distinct date from swim_event order by date asc'

    df = run_sql(sql)
    dates = df['date'].dt.date.to_list()
    curr_date = pd.to_datetime(dates[-1]).date()
    final_dates = []

    while curr_date < yesterday:
        curr_date += timedelta(days=1)
        final_dates.append(curr_date)


    dates_not_done = [x for x in day_check if x not in final_dates + dates]

    if dates_not_done:
        final_dates = sorted(set(final_dates + dates_not_done))

    return final_dates

@log_decorator_args(function_type='automations', my_logger=logger, timer=True)
def build_data(test_flag:bool, dates:list[datetime], test_device_names:list[str] = None,):
    device_amounts_df = []
    delta_df = []
        
    for date in dates:
        temp_df = get_sql_table_by_date('device_amounts', date.strftime('%Y-%m-%d'))
        if test_flag:
            temp_df = temp_df[temp_df['device_name'].isin(test_device_names)]       
        if not temp_df.empty:
            device_amounts_df.append(temp_df)
            
    for date in dates:
        delta_temp_df = get_sql_table_by_date('totals', date.strftime('%Y-%m-%d'))
        
        if test_flag:
            delta_temp_df = delta_temp_df[delta_temp_df['device_name'].isin(test_device_names)]    
            
        if not delta_temp_df.empty:
            delta_df.append(delta_temp_df)
    
    if len(device_amounts_df) > 0:
        device_amounts_df = pd.concat(device_amounts_df)
    else:
        logger.info(f'No data to write...')
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if len(delta_df) > 0:
        delta_df = pd.concat(delta_df)
        
    else:
        logger.info(f'No deltas...')
    
    delta_df = delta_df[~delta_df['device_name'].isin(['Global Machines'])]
    delta_df = delta_df[['date', 'delta','device_name']]
    delta_df = delta_df[delta_df['delta']!=0]
    
    delta_df['gl_number'] = DELTA_GL
    delta_df['cashbox_nbr'] = ''
    delta_df = delta_df.rename(columns={'delta':'amount'})
    delta_df['rtxn_type'] = delta_df.apply(apply_delta_logic, axis=1)
    
    
    device_amounts_df = device_amounts_df[device_amounts_df['post_type']=='Banker']
    device_amounts_df = device_amounts_df[device_amounts_df['device_type'].isin(['Cash Acceptor', 'Cash Dispenser'])]
    rtxn_type_mapping =  {'Cash Acceptor':'GLD', 'Cash Dispenser':'GLR'}
    device_amounts_df['rtxn_type'] = device_amounts_df['device_type'].map(rtxn_type_mapping)
    
    atm_locations_df = get_sql_table('atm_locations')
    atm_owners_df = get_sql_table('atm_owners')
    
    df = device_amounts_df.merge(atm_locations_df, how='left', on='device_name')
    
    df['amount'] = df.apply(apply_device_type_logic, axis=1)
    df = df[df['amount']>0]
    df = df[['gl_number', 'rtxn_type', 'amount', 'date', 'cashbox_nbr', 'device_name']]
    # df = pd.concat([df, delta_df], axis=0)
    
    return [df.drop_duplicates(), delta_df.drop_duplicates(), atm_locations_df.drop_duplicates(), atm_owners_df.drop_duplicates()]

def final_swim_file(df: pd.DataFrame):
    if not df.empty:
        df = df.dropna(subset='gl_number')
        data = df.sort_values(by='date', ascending=True).to_dict('records')
        filenumber = 1
        filename = build_swim_filename(date=datetime.today(), number=filenumber, target_dir=NCR_RECON_SWIM_FILE_DIR)
        dnaupload_filename = build_swim_filename(date=datetime.today(), number=filenumber, target_dir=DNAUPLOAD_PATH)
        
        while os.path.exists(filename):
            filenumber += 1
            filename = build_swim_filename(date=datetime.today(),number=filenumber, target_dir=NCR_RECON_SWIM_FILE_DIR)
            dnaupload_filename = build_swim_filename(date=datetime.today(), number=filenumber, target_dir=DNAUPLOAD_PATH)

        build_swim_file(data=data, filename=filename)
        shutil.copy(filename, dnaupload_filename)
        logger.info(f'Completed building of SWIM file. File is located at: {filename}')
        dbcon = build_postgres_connection_string(username=DBUSERNAME, password=DBPASSWORD, host=DBHOST, database=DBDATABASE, port=DBPORT)
        df.to_sql('swim_event', dbcon, if_exists='append')
    else:
        logger.info(f'No data to write... returning...')

def send_outage_emails(delta_df:pd.DataFrame, atm_locations_df:pd.DataFrame, atm_owners_df:pd.DataFrame):
    if not delta_df.empty:
        email_df = delta_df.merge(atm_locations_df, how='right', on='device_name')
        email_df = email_df.merge(atm_owners_df, how='right', on='branch')
        email_df = email_df[['date', 'amount', 'device_name', 'branch', 'email']]
        
        # for branch in email_df['branch'].unique():
        #     branch_df : str = email_df[email_df['branch']==branch]
        #     branch_data : str = branch_df.to_dict('records')
        #     branch_email_content : str = template.render(data = branch_data)
        #     branch_email : str = branch_data[0]['email']
        #     if branch_email:
        #         send_email(branch_email, EMAIL_USERNAME, EMAIL_PASSWORD, f'ATM Outages ({branch}) - {today.strftime("%Y-%m-%d")}', branch_email_content, False)
        
        all_data = email_df.to_dict('records')
        email_df = email_df.dropna(subset = 'amount')
        email_df = email_df[['date', 'amount', 'device_name', 'branch']]
        email_df = email_df.rename(columns={i:i.upper() for i in email_df.columns})
        html_message = f'''
        Good afternoon,<br><br>
        This email is to inform you that your branch has an outage in the below ATMs.
        <br><br><br><br><br><br>
        {email_df.to_html(index = False)}
        <br><br><br><br><br><br>
        If you have any questions or need further assistance, feel free to contact the Business Intelligence Team. 
        
        '''
        # html_content = template.render(data = all_data)
        all_emails = [
            'accounting@raiz.us', 
            'BusinessIntelligence@raiz.us',
            'blcupp@raiz.us'
                    #   , 'cardservices@raiz.us', 'hubsupport@raiz.us'
                      ]
        
        

        credentials = Credentials(username = os.getenv('EMAIL_USERNAME'), password = os.getenv('EMAIL_PASSWORD'))
        servername = 'mail.tfcu.coop'
        config = Configuration(
            server = servername,
            credentials = credentials
        )
        account = Account(
            primary_smtp_address='business_intelligence@raiz.us',
            credentials=credentials,
            config=config,
            access_type=DELEGATE 
        )
        subject = f'ATM Outages (All) - {today.strftime("%Y-%m-%d")}'
        
        message = Message(
            account=account,
            subject=subject,
            body=HTMLBody(html_message),
            to_recipients=all_emails
            )
        message.send()
            
        logger.info(f'Completed sending delta emails...')
    else:
        html_message = f'''
        Good afternoon,<br><br>
        No outages in all ATMs. Thank you.
        '''
        # html_content = template.render(data = all_data)
        all_emails = [
            'accounting@raiz.us', 
            'BusinessIntelligence@raiz.us',
            'blcupp@raiz.us'
                    #   , 'cardservices@raiz.us', 'hubsupport@raiz.us'
                      ]
        
        

        credentials = Credentials(username = os.getenv('EMAIL_USERNAME'), password = os.getenv('EMAIL_PASSWORD'))
        servername = 'mail.tfcu.coop'
        config = Configuration(
            server = servername,
            credentials = credentials
        )
        account = Account(
            primary_smtp_address='business_intelligence@raiz.us',
            credentials=credentials,
            config=config,
            access_type=DELEGATE 
        )
        subject = f'ATM Outages (All) - {today.strftime("%Y-%m-%d")}'
        
        message = Message(
            account=account,
            subject=subject,
            body=HTMLBody(html_message),
            to_recipients=all_emails
            )
        message.send()
            
        logger.info(f'Completed sending delta emails...')



    
    

