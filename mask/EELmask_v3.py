# -*- coding: utf-8 -*-
"""
Spyder Editor

This prog reads the KLayout query and populates eels_mask_info in pho_test DB
"""
import pandas as pd
import sys
import tkinter as tk
from tkinter import filedialog
import datetime
from dbConn import dbConn

class mask():
    
    def __init__(self,mask_name, device_type):
        self.mask_name = mask_name
        self.device_type = device_type
        self.db_el=dbConn('el','laser','el')
        
    def get_excel(self):
        #pick the folder
        root = tk.Tk()
        root.withdraw()
        file = filedialog.askopenfilename()
        #find files start with mask_name
        if file == "":
            sys.exit("canceled")
        else:
            print("Read excel file")
            devices = pd.read_excel(file,'devices').dropna(how='all')
            bars = pd.read_excel(file,'bars').dropna(how='all')
            print("file imported")
        return devices, bars
    
    def sync_mask(self):
        ID = self.db_el.getID_byUniqueName('mask','mask_id','mask_name',self.mask_name)
        if ID == 0:
            df = self.db_el.initDF_byTable('mask')
            df = df.append({'mask_name':self.mask_name, \
                            'mask_type':self.device_type, \
                            }, ignore_index=True)
            df = self.db_el.sync_byDF('mask','mask_id',df)
            mask_id = df['mask_id'][0]
        else:
            mask_id = ID
        return mask_id
    
    def arrange_info(self,devices,bars):
        
        print('verify mask first')
        mask_id=self.sync_mask()
        
        print("start to arrange info")
        df = self.db_el.initDF_byTable('el_part')
        
        for index, row in bars.iterrows():
            C = row['Column']
            R = row['Row']
            length = row['Mesa Y']
            ds = devices[devices['Name']==row['Device_name']]
            for idx, d in ds.iterrows():
                x = d['index']
                width = d['ridge_width']
                pname = self.get_part_name(R,C,x,width,length)
                df = df.append({'part_name':pname, \
                                'row':R, \
                                'col':C, \
                                'x_pos':int(x),\
                                'cavity_length_um':length,\
                                'cavity_width_um':width,\
                                'mask_id':mask_id \
                                }, ignore_index=True)
        return df
    
    def update_db(self,df):
        print("sync with DB now...")
        df = self.db_el.sync_byDF('el_part','part_id',df)
        print("done!")
        return df
        
    def get_part_name(self,row,col,x,width,length):
        if str(width) == str(int(width)):
            wname = str(width).zfill(2)+".0"
        else:
            wname = str(int(width)).zfill(2)+str(width-int(width))[1:3]
        
        part_name = str(col) + "-" + \
                    str(row) + "-" + \
                    str(int(x)).zfill(2) + "-" + \
                    wname + "-" + \
                    str(int(length))
        return part_name
    
    def get_curent_datetime(self):
        dt = datetime.datetime.now()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
        
    
if __name__ == "__main__":
    mask_name = "PH008"
    device_type = "EEL"
    
    test = mask(mask_name, device_type)
    devices,bars = test.get_excel()
    df = test.arrange_info(devices,bars)
    df = test.update_db(df)
