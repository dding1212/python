# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 14:11:17 2020

@author: dding
"""
from dbConn import dbConn
import pandas as pd


def get_device_list(devices):
    d_list = "("
    for index, row in devices.iterrows():
        d_list = d_list + str(row['device_id']) + ","
    d_list = d_list[:-1] + ")"
    return d_list
    

if __name__ == "__main__":
    #get all the wafer_id list
    time_period = "'2020-02-01' and '2020-03-16'"
    db0 = dbConn('epi','epi','epi')
    db1 = dbConn('db','laser','db00')
    sql = 'SELECT distinct wafer_id FROM laser.eel_device'
    df = db1.getDF_bySQL(sql)
    for index,row in df.iterrows():

        wafer_id = row['wafer_id']
        wafer_name = db0.getWaferName_byWaferID(wafer_id)
        sql1 = 'select distinct v.label_name, date(v.test_time) as test_date\r\n' + \
        'from v_eel_pliv_internal v\r\n' + \
        'where v.wafer_id = ' + str(wafer_id) +'\r\n' + \
        'and v.test_time between ' + time_period + ";"
        temp = db1.getDF_bySQL(sql1)
        n = len(temp)
        if index==0:
            df_out = pd.DataFrame({'wafer_id':[wafer_id]*n,
                                   'wafer_name':[wafer_name]*n,
                                   'label_name':temp['label_name'].tolist(),
                                   'test_date':temp['test_date'].tolist()
                                   })
        else:
            temp1 = pd.DataFrame({'wafer_id':[wafer_id]*n,
                                   'wafer_name':[wafer_name]*n,
                                   'label_name':temp['label_name'].tolist(),
                                   'test_date':temp['test_date'].tolist()
                                   })
            df_out = df_out.append(temp1,ignore_index=True)
    df_out.to_excel('tested_eels.xlsx',index=False)
