# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 13:44:22 2020

@author: dding
"""

from dbConn import dbConn

def get_lot_stage(days=30):
    db = dbConn('epi','epi','epi')
    sql = """select ms.category, ms.name as manufacturing_stage, p.sales_order_number as project,
    l.name as lot_name, w.wafer_name,
    concat(w.ingot_number, "-", w.substrate_sn) as scribe, w.structure_name,
    m.device_mask, 
    date(max(me.date)) as last_event_date, datediff(NOW(), date(max(me.date))) as days_since_last_event
    from epi.manufacturing_lot l 
    inner join epi.manufacturing_lot_wafer lw on l.id= lw.lot_id
    inner join epi.epi_wafer w on lw.wafer_id = w.wafer_id
    left join epi.epi_device_mask m on w.device_mask_id = m.device_mask_id
    left join epi.manufacturing_stage ms on w.manufacturing_stage_id = ms.id
    inner join epi.manufacturing_event_log me on w.wafer_id = me.wafer_id
    left join epi.production_order_wafers pw on pw.wafer_name = w.wafer_name
    left join epi.production_orders p on p.id = pw.production_order_id
    where ms.category in ('Back-Fab','Front-Fab','Test')
    and ms.id not in ('0221','0222','0223','0480','0490','0495')"""
    sql = sql + "\r\n" + "and me.date BETWEEN NOW() - INTERVAL " + str(int(days)) + " DAY AND NOW()\r\n"
    sql = sql + "group by w.wafer_id\r\n" + "order by ms.id"
    #print (sql)
    df = db.getDF_bySQL(sql)
    return df

def arrange_df_todict(df):
    pass

if __name__ == "__main__":
    df = get_lot_stage()