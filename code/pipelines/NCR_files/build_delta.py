from datetime import datetime, timedelta

import pandas as pd

from common.extract_data import get_sql_table

def get_previous_total(pick_date:datetime):
    previous_date = pick_date - timedelta(1)
    df = get_sql_table('amount_delta', previous_date)
    return df

def build_balance_delta():
    pass 

def make_delta_sql(pick_date:datetime, device_name:str):
    pick_date = pick_date.strftime('%m/%d/%Y')
    SQL = f'''  
        with data as (
        select 
            t.*, 
            lag(amount) over(partition by device_name order by "date") prev_amount,
            (
                select DISTINCT 
                    amount_increased 
                from device_amounts da 
                where 
                    da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Acceptor'
                    and da.post_type = 'Banker' 
                ) as cashAcceptorBankerPlus,
            (
                select DISTINCT
                    amount_increased 
                from device_amounts da 
                where 
                    da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Acceptor'
                    and da.post_type = 'ATM' 
                ) as cashAcceptorATMPlus,
            (
                select DISTINCT
                    amount_decreased 
                from device_amounts da 
                where 
                    da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Acceptor'
                    and da.post_type = 'CIT' 
                ) as cashAcceptorCITMinus,
            (
                select DISTINCT
                    amount_increased 
                from device_amounts da 
                where da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Dispenser'
                    and da.post_type = 'CIT' 
                ) as cashDispenserCITPlus,
            (
                select DISTINCT
                    amount_decreased 
                from device_amounts da 
                where da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Dispenser'
                    and da.post_type = 'Banker' 
                ) as cashDispenserBankerMinus,
            (
                select DISTINCT
                    amount_decreased 
                from device_amounts da 
                where da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Dispenser'
                    and da.post_type = 'ATM' 
                ) as cashDispenserATMMinus,	
            (
                select DISTINCT
                    amount_decreased 
                from device_amounts da 
                where da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Cash Dispenser'
                    and da.post_type = 'CIT' 
                ) as cashDispenserCITMinus,		
            (
                select DISTINCT
                    amount_increased 
                from device_amounts da 
                where da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Coin Dispenser'
                    and da.post_type = 'CIT' 
                ) as coinDispenserCITPlus,
            (
                select DISTINCT
                    amount_decreased 
                from device_amounts da 
                where da.device_name = t.device_name 
                    and da."date" = t."date" 
                    and da.device_type = 'Coin Dispenser'
                    and da.post_type = 'CIT' 
                ) as coinDispenserCITMinus
        from totals t
        --left join device_amounts da on da.device_name = t.device_name and da."date" = t."date" 
        --left join start_end_amounts sea on sea.device_name = t.device_name and t."date" = sea."date" 
        where 1=1
            and t.device_name = '{device_name}'
            and t.date in ( ('{pick_date}'::date), ('{pick_date}'::date - 1) )
        ),
        totals as (select 
            device_name,
            "date",
            lag(amount) over(partition by device_name order by "date") prevAmount,
            amount,
            cashacceptorbankerplus + cashacceptoratmplus + cashdispensercitplus + coindispensercitplus as plus,
            cashacceptorcitminus + cashdispenserbankerminus + cashdispenseratmminus + cashdispensercitminus + coindispensercitminus as minus
        from data),
        final as (select device_name, date, prevAmount + plus - minus as estimated_amount, amount from totals)
        select final.*, estimated_amount - amount as delta from final where date = '{pick_date}'::date
    '''
    return SQL