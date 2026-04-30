import os

import pandas as pd


def get_directory_of_file(file):
    abspath = os.path.abspath(file)
    dirname = os.path.dirname(abspath)
    return dirname

def build_file(filename:str, data:list):
        
    with open(filename, 'w') as openfile:
        for line in data:
            openfile.write(line + '\n')

def space_adjust_value(total_space:int, value:str, direction:str='right', filling_value:str='0')->str:
    assert direction in ['left', 'right'], f'{direction} not a valid input. Must be left or right.'
    adjusted_value = str(value)

    while len(adjusted_value) < total_space:
        if direction == 'right':
            adjusted_value = filling_value + adjusted_value
            
        elif direction == 'left':
            adjusted_value = adjusted_value + filling_value
            
        
    return adjusted_value

def add_one_to_file(filename:str, delimiter:str, position:int)->str:
    filesplit = filename.split(delimiter)
    filesplit[position] = str(int(filesplit[position]) + 1)
    return '.'.join(filesplit)

def split_preserve_quotes(s:str)->list[str]:
    result = []
    current = []
    in_quotes = False
    
    for char in s:
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
        elif char == ',' and not in_quotes:
            result.append(''.join(current))
            current = []
        else:
            current.append(char)
    

    if current:
        result.append(''.join(current))
    
    return result

def build_postgres_connection_string(username:str, password:str, host:str, database:str, port:int):
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
    return connection_string

def is_data_file(file_name:str, filter_for:str, file_ext:str='csv')->bool:
    if file_name.startswith(filter_for) and file_name.endswith(file_ext):
        return True
    
    return False

def get_all_files(source_directory:str, filter_for:str, file_ext:str='.csv')->list[str]:
    if not os.path.exists(source_directory):
        raise ValueError(f'{source_directory} is not attached or does not exist!')
    
    filepaths = []
    for file in os.listdir(source_directory):
        if is_data_file(file, filter_for, file_ext):
            file_name = os.path.join(source_directory, file)
            filepaths.append(file_name)
        
    return filepaths

def get_distinct_list(connection_string:str, column_name:str, table_name:str)->list:
    from sqlalchemy.exc import ProgrammingError
    sql = f'select distinct {column_name}  from {table_name} '
    try:
        df = pd.read_sql(sql, connection_string)
        return df[column_name.replace('"', '')].values.tolist()
    
    except ProgrammingError:
        return []
