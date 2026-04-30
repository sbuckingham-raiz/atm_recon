import os

import pandas as pd

from common.utility import build_postgres_connection_string



def setup():
    global DBUSERNAME
    global DBPASSWORD
    global DBDATABASE
    global DBHOST
    global DBPORT 
    
    DBUSERNAME = os.getenv('DBUSERNAME')
    DBPASSWORD = os.getenv('DBPASSWORD')
    DBDATABASE = os.getenv('DBDATABASE')
    DBHOST = os.getenv('DBHOST')
    DBPORT = os.getenv('DBPORT')


def run_sql(sql:str,
            database:str = None
            ) -> pd.DataFrame:
    setup()
    con = build_postgres_connection_string(username = DBUSERNAME,
                                           password = DBPASSWORD,
                                           host = DBHOST,
                                           database = database if database else DBDATABASE,
                                           port = DBPORT
                                           )
    df = pd.read_sql(sql, con)
    return df


def get_sql_table_by_column(table:str, 
                            column:str, 
                            column_data_type:str, 
                            filter_value:str | int,
                            database:str = None,
                            )->pd.DataFrame:
    setup()
    sql = f'''SELECT * FROM {table} WHERE {column}::{column_data_type} = {"'" if isinstance(filter_value, str) else ''}{filter_value}{"'" if isinstance(filter_value, str) else ''} '''
    con = build_postgres_connection_string(username = DBUSERNAME,
                                           password = DBPASSWORD,
                                           host = DBHOST,
                                           database = database if database else DBDATABASE,
                                           port = DBPORT
                                           )
    
    df = pd.read_sql(sql, con=con)
    
    return df


def get_sql_table_by_date(table:str, 
                          date:str,
                          database:str = None, 
                          )-> pd.DataFrame:
    setup()
    sql = f'''SELECT * FROM {table} WHERE "date"::date = '{date}'::date '''
    
    con = build_postgres_connection_string(username = DBUSERNAME,
                                           password = DBPASSWORD,
                                           host = DBHOST,
                                           database = database if database else DBDATABASE,
                                           port = DBPORT
                                           )
    
    df = pd.read_sql(sql, con=con)
    
    return df

def get_sql_table(table:str,
                  database:str = None, 
                  )->pd.DataFrame:
    setup()
    sql = f'''SELECT * FROM {table} '''
    con = build_postgres_connection_string(username = DBUSERNAME,
                                           password = DBPASSWORD,
                                           host = DBHOST,
                                           database = database if database else DBDATABASE,
                                           port = DBPORT
                                           )
    df = pd.read_sql(sql, con=con)
    
    return df
