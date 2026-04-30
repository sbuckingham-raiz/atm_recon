import pandas as pd

from common.extract_data import get_sql_table

def get_verification_table(database:str) -> list[pd.DataFrame, pd.DataFrame]:
    totals = get_sql_table('totals', database=database)
    totals = totals[totals['device_name']=='Global Machines']
    totals = totals[['date', 'amount']]

    device_amounts = get_sql_table('device_amounts', database=database)
    device_amounts = device_amounts[device_amounts['device_name']=='Global Machines']
    device_amounts['totals'] = device_amounts['amount_increased'] + device_amounts['amount_decreased']
    device_amounts = device_amounts[['date', 'totals']]
    device_amounts = device_amounts.groupby('date')['totals'].sum().reset_index()
    
    return [totals, device_amounts]

def get_actual_table() -> list[pd.DataFrame, pd.DataFrame]:
    totals = get_sql_table('totals')
    totals = totals[totals['device_name']=='Global Machines']
    totals = totals.rename(columns={'date':'verification_date', 'amount':'verification_amount'})
    totals = totals[['verification_date', 'verification_amount']]

    device_amounts = get_sql_table('device_amounts')
    device_amounts = device_amounts[device_amounts['device_name']=='Global Machines']
    device_amounts['totals'] = device_amounts['amount_increased'] + device_amounts['amount_decreased']
    device_amounts = device_amounts.rename(columns={'date':'verification_date', 'amount':'verification_amount'})
    device_amounts = device_amounts[['verification_date', 'verification_totals']]
    device_amounts = device_amounts.groupby('date')['verification_totals'].sum().reset_index()
    
    return [totals, device_amounts]

def compare_tables(verification_total:pd.DataFrame,
                   actual_total:pd.DataFrame,
                   verification_device_amounts:pd.DataFrame,
                   actual_device_amounts:pd.DataFrame
                   ) -> list[pd.DataFrame, pd.DataFrame]:
    
    final_total = verification_total.merge(actual_total,
                                           right_on = 'date',
                                           left_on = 'verification_date',
                                           how = 'left'
                                           )
    final_device_amounts = verification_device_amounts.merge(actual_device_amounts,
                                                             right_on = 'date',
                                                             left_on = 'verification_date',
                                                             how = 'left'
                                                             )
    final_total['delta'] = final_total['amount'] - final_total['verification_amount']
    final_device_amounts['delta'] = final_device_amounts['totals'] - final_total['verification_totals']
    final_total = final_total[round(final_total['delta'],2)!=0]
    final_device_amounts = final_device_amounts[round(final_total['delta'],2)!=0]
    return [final_total, final_device_amounts]