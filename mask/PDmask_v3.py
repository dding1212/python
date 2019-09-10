# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 14:55:41 2019
This prog read the excel file and populates pd_mask_info in SJ DB00
@author: dding
"""
import math
import numpy as np
import pandas as pd
import sys
import tkinter as tk
from tkinter import filedialog
#import datetime
from dbConn import dbConn

class pdMask():
    def __init__(self,name,die_x,die_y):
        self.name = name
        self.die_x = die_x
        self.die_y = die_y
        self.db_epi = dbConn('epi','epi','epi')
        self.db_db = dbConn('pd','test','DB00')
        self.epi_mask_id = self.get_epi_mask_id()
    
    def get_epi_mask_id(self):
        ID = self.db_epi.getID_byUniqueName('epi_device_mask','device_mask_id','device_mask',self.name)
        return ID
    
    def get_mask_db(self,mask_name):
        mask_id = self.get_mask_id(mask_name)
        df_mask = self.db_db.getDF_byID('pd_mask_info','mask_id',mask_id)
        return df_mask
    
    def get_CR(self,df_mask):
        for index, row in df_mask.iterrows():
            df_mask.at[index,'col'] = int(round(row['x_loc']/(self.die_x*1000)))
            df_mask.at[index,'row'] = int(round(row['y_loc']/(self.die_y*1000)))
        return df_mask
    
    # update CR after file has been uploaded by V1
    def update_CR(self,df_mask):
        conn = self.db_db.eg.connect()
        trans = conn.begin()
        for index, row in df_mask.iterrows():
            sql = "update test.pd_mask_info m\r\n" + \
                  "set m.col=" + str(row['col']) + ", m.row="+ str(row['row']) +"\r\n" + \
                  "where m.mask_info_id=" + str(row['mask_info_id'])
            conn.execute(sql)
        trans.commit()
        # Close connection
        conn.close()
    
    def get_die_number(self,loc,offset,direction):
        if direction == 'x':
            die_number = math.floor((loc-offset)/(self.die_x*1000))
        elif direction == 'y':
            die_number = math.floor((loc-offset)/(self.die_y*1000))
        return die_number

    def get_mask_id(self,mask_name):
        ID = self.db_db.getID_byUniqueName('pd_mask','mask_id','mask_name',mask_name)
        return ID
    
    def sync_df_mask(self):
        #get df
        df = self.db_db.initDF_byTable('pd_mask')
        df = df.append({'mask_name':self.name, \
                   'epi_mask_id':self.epi_mask_id, \
                   'die_x':self.die_x, \
                   'die_y':self.die_y, \
                   'comment':''}, ignore_index=True)
        ID = self.get_mask_id(self.name)
        if ID==0:
            df = self.db_db.sync_byDF('pd_mask','mask_id',df)
        else:
            df.at[0,'mask_id'] = ID
        print ('mask sync done')
        return df
    
    def get_excel(self):
        root = tk.Tk()
        root.withdraw()
        file = filedialog.askopenfilename()
        if file == "":
            sys.exit("canceled")
        else:
            print("Read Excel file")
            devices = pd.read_excel(file,'Sheet1').dropna(how='all')
            print("file imported")
        return devices
    
    def arrange_info(self,devices):
        print("start to arrange info")
        df_m = self.sync_df_mask()
        mask_id = df_m['mask_id'][0]
        
        #init df
        df = self.db_db.initDF_byTable('pd_mask_info')
        
        for index, row in devices.iterrows():
            pName = row['Device Name']
            pType = row['Device Design Type']
            x = row['X Loc']
            y = row['Y Loc']
            mArea = row['Mesa Area']
            aArea = row['Active Area']
            if np.isnan(row['Mesa R'])==True:
                mR = 0
            else:
                mR = row['Mesa R']
            if np.isnan(row['Active R'])==True:
                aR = 0
            else:
                aR = row['Active R']
            col = self.get_die_number(x,0,'x')
            if mask_id==2:
                offset = -6000
            else:
                offset = 0
            row = self.get_die_number(y,offset,'y')
            df = df.append({'mask_id':mask_id, \
                            'part_name':pName, \
                            'part_type':pType, \
                            'x_loc':x, \
                            'y_loc':y, \
                            'mesa_area':mArea, \
                            'active_area':aArea, \
                            'mesa_r':mR, \
                            'active_r':aR, \
                            'col':col, \
                            'row':row}, \
                            ignore_index=True)
        df = self.db_db.sync_byDF('pd_mask_info','mask_info_id',df)
        return df
    

    
if __name__ == "__main__":
    name = "PH009" #verify it before start!!!
    die_x = 9.6
    die_y = 12.0
    test = pdMask(name,die_x,die_y)
    devices = test.get_excel()
    df_m = test.sync_df_mask()
    df = test.arrange_info(devices)
    #df_mask = test.get_mask_db(name)
    #df_mask = test.get_CR(df_mask)
    #test.update_CR(df_mask)
    
    