import os

from datetime import datetime, timedelta

from common.utility import space_adjust_value, build_file

def apply_device_type_logic(row):
    if row['device_type']=='Cash Acceptor':
        return row['amount_increased']
    elif row['device_type']=='Cash Dispenser':
        return row['amount_decreased']

def apply_delta_logic(row):
    if row['amount']<0:
        return 'GLR'

    return 'GLD'


def build_swim_file(filename: str, data: dict):
    lines = []
    for val in data:
        lines.append(
            write_swim_file_line(gl_number=val['gl_number'],
                                 rtxn_type=val['rtxn_type'],
                                 amount=val['amount'],
                                 post_date=val['date'] + timedelta(days=1),
                                 cashbox=val['cashbox_nbr'],
                                 device_name=val['device_name']))
    build_file(filename, lines)

def build_swim_filename(date:datetime, target_dir:str, number:int=1):
    filename = f"1543.ATMGL.{date.strftime('%m%d%y')}.{number}.SWM@dnaappwx.intjc.xfrp"
    final_filename = os.path.join(target_dir, filename)
    return final_filename

def write_swim_file_line(gl_number:int,
                         rtxn_type:str,
                         amount:float,
                         post_date:datetime,
                         cashbox:int,
                         device_name:str,
                         )->str:
    GL_TOTAL_LENGTH = len('00000100020453568')
    adjusted_gl_number = space_adjust_value(GL_TOTAL_LENGTH, int(gl_number))

    RTXN_TYPE_TOTAL_LENGTH = 4
    adjusted_rtxn_type = space_adjust_value(RTXN_TYPE_TOTAL_LENGTH, rtxn_type.upper(), filling_value=' ', direction='left')

    AMOUNT_TOTAL_LENGTH = len('0001727700')
    adjusted_amount = space_adjust_value(AMOUNT_TOTAL_LENGTH, f'{amount:.2f}'.replace('.', ''), direction='right')

    adjusted_post_date = post_date.strftime('%Y%m%d')

    MESSAGE_TOTAL_LENGTH = len('DISPENSER 12/02/24 NCR AUTOMATION                            ')
    device_type = ''
    if rtxn_type == 'GLR':
        device_type = 'DISPENSER'

    elif rtxn_type == 'GLD':
        device_type = 'ACCEPTOR'

    message = f'{device_type} {post_date.strftime("%Y/%m/%d")} {device_name}'
    adjusted_message = space_adjust_value(MESSAGE_TOTAL_LENGTH, message, filling_value=' ', direction='left')

    CASHBOX_LENGTH = len('1679             ')

    adjusted_cashbox = space_adjust_value(CASHBOX_LENGTH, int(cashbox) if cashbox else '', filling_value=' ', direction='left')

    line = f'{adjusted_gl_number}{adjusted_rtxn_type}{adjusted_amount}000001{adjusted_post_date}{adjusted_message}{adjusted_cashbox}CASH ALLIMED          NOTEBAL N'
    return line
