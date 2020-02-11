# -*- coding: utf-8 -*-
"""
Spyder Editor

This prog reads the KLayout query and populates eels_mask_info in pho_test DB
"""
import pandas as pd
import numpy as np
import sys
from dbConn import dbConn
from abc import abstractmethod
import tkinter as tk
from tkinter import filedialog


class mask():

    def __init__(self,mask_name, mask_type):
        self.mask_name = mask_name
        self.mask_type = mask_type
        if mask_type.lower() == 'eel' or mask_type.lower == 'eel':
            schema = 'laser'
        elif mask_type.lower() == 'pd':
            schema = 'test'
        self.db = dbConn('db00',schema,'db00')
        return
    
    @abstractmethod
    def get_mask_data(self):
        pass
    
    @abstractmethod
    def arrange_mask_data(self):
        pass

    @abstractmethod
    def sync_mask(self):
        pass
    
    @abstractmethod
    def check_mask_data(self):
        pass
    
    @abstractmethod
    def del_mask_data(self):
        pass

    def get_epi_mask_id(self):
        dbEpi = dbConn('epi','epi','epi')
        epi_mask_id = dbEpi.getDF_byUniqueName('epi_device_mask','device_mask_id','device_mask',self.mask_name)
        return epi_mask_id

    def pick_file(self):
        #pick the folder
        root = tk.Tk()
        root.withdraw()
        file = filedialog.askopenfilename()
        return file

class eel_mask(mask):
    def __init__(self,mask_name):
        super().__init__(mask_name,'EEL')
        return
    
    def get_df_mask(self, die_x = 0, die_y = 0, comment=""):
        df_mask = self.db.initDF_byTable('mask')
        df_mask = df_mask.append({'mask_name':self.mask_name, \
                                  'mask_type':self.mask_type, \
                                  'die_x':die_x, \
                                  'die_y':die_y, \
                                  'comment':comment \
                                  },ignore_index=True)
        return df_mask
    
    def  sync_mask(self, df_mask):
        df_mask = self.db.sync_byDF_byuniqueName('mask','mask_id','mask_name',df_mask)
        return df_mask
    
    def del_mask_data(self,mask_id,content='bar'):
        if content == 'bar':
            #delete content from eel_bar and wafer_part
            bar_id_list = self.db.getIDlist_byValue('eel_bar','eel_bar_id','mask_id',self.name)
            wpart_id_list = self.db.getIDlist_byValuelist('eel_wafer_part','eel_wafer_part_id','eel_bar_id',bar_id_list)
            self.db.del_byIDList('eel_bar','eel_bar_id',bar_id_list)
            self.db.del_byIDList('eel_wafer_part','eel_wafer_part_id',wpart_id_list)
        elif content == 'all':
            #delete contents from all tables
            pass
        else:
            sys.exit("canceled")
        return
    
    def check_mask_data(self,mask_id):
        ID = self.db.getID_byUniqueName('eel_bar','eel_bar_id','mask_id',str(mask_id))
        return ID

    def get_mask_data(self):
        file = self.pick_file()
        if self.mask_name in file:
            pass
        else:
            print('The file name has no mask name!')
            print ('y to continue, n or any other input to quit')
            reply = input()
            if reply == 'y':
                pass
            else:
                sys.exit("canceled!")
        if file == "":
            sys.exit("canceled")
        else:
            print("Reading file")
            devices = pd.read_excel(file,'devices').dropna(how='all')
            bars = pd.read_excel(file,'bars').dropna(how='all')
            print("file imported")
        return devices, bars
    
    def __get_eel_group(self,devices):
        df_group = self.db.initDF_byTable('eel_group')
        for _,grp in devices.groupby('Name'):
            df_group = df_group.append({'eel_group_name':grp['Name'].iloc[0], \
                                        'sin_offset':grp['SiN offset'].iloc[0], \
                                        'trench_width':grp['trench_width'].iloc[0], \
                                        'ridge_center_offset':grp['ridge_center_offset'].iloc[0],\
                                        'mesa_offset':grp['mesa_offset'].iloc[0],\
                                        'metal_x_offset':grp['metal_Xoffset'].iloc[0],\
                                        'metal_y_offset':grp['metal_Yoffset'].iloc[0],\
                                        'metal_sin_y_offset':grp['metal_SiN_Yoffset'].iloc[0] \
                                        }, ignore_index=True)
        return df_group
    
    def arrange_mask_data(self,devices,bars):
        df_mask = self.get_df_mask()
        df_mask = self.sync_mask(df_mask)
        mask_id = df_mask['mask_id'].iloc[0]
        
        print("start to arrange data")
        df_bar = self.db.initDF_byTable('eel_bar')
        df_group = self.__get_eel_group(devices)
        df_part = self.db.initDF_byTable('eel_part')
        df_wpart = self.db.initDF_byTable('eel_wafer_part')
        
        print("writing eel_group...")
        # update eel_group first
        df_group = self.db.sync_byDF('eel_group','eel_group_id',df_group) #line by line 
        
        for index, row in bars.iterrows():
            bar_name = row['Bar_name']
            x = row['X loc']
            y = row['Y loc']
            C = row['Column']
            R = row['Row']
            length = row['Active Y']
            group_id = int(df_group[df_group.eel_group_name==row['Device_name']]['eel_group_id'].iloc[0])
            df_bar = df_bar.append({'eel_bar_name':bar_name, \
                                    'eel_group_id':group_id, \
                                    'x_loc':x, \
                                    'y_loc':y, \
                                    'row':R, \
                                    'col':C, \
                                    'cavity_length_um':length,\
                                    'mask_id': mask_id \
                                    }, ignore_index=True)
        # update eel_bar
        print("writing eel_bar...")
        df_bar, isAdded = self.db.sync_byDF_SingleValueGroup('eel_bar','eel_bar_id','mask_id',df_bar)
        
        # update eel_part
        print("writing eel_part...")
        for _,grp in devices.groupby('Name'):
            n = len(grp)
            group_name = grp['Name'].iloc[0]
            group_id = int(df_group[df_group.eel_group_name==group_name]['eel_group_id'].iloc[0])
            
            df_temp = pd.DataFrame({'eel_part_id':[np.nan]*n, \
                                    'eel_group_id':[group_id]*n, \
                                    'x_pos':grp['index'].tolist(), \
                                    'cavity_width_um':grp['ridge_width'].tolist() \
                                    })
            df_part = df_part.append(df_temp, ignore_index=True)
        vList = df_part['eel_group_id'].unique().tolist()
        df_part,isAdded = self.db.sync_byDF_MultiValueGroup('eel_part','eel_part_id','eel_group_id',vList,df_part)
        print("writing eel_wafer_part...")
        for index, row in df_bar.iterrows():
            C = row['col']
            R = row['row']
            cav_length = row['cavity_length_um']
            group_id = row['eel_group_id']
            bar_id = row['eel_bar_id']
            dp = df_part[df_part['eel_group_id']==group_id]
            for idx, p in dp.iterrows():
                x = p['x_pos']
                cav_width = p['cavity_width_um']
                pname = self.get_part_name(R,C,x,cav_width,cav_length)
                df_wpart = df_wpart.append({'label_name':pname, \
                                            'eel_part_id':p['eel_part_id'], \
                                            'eel_bar_id':bar_id \
                                            }, ignore_index=True)
        vList = df_wpart['label_name'].unique().tolist()
        df_wpart, isAdded = self.db.sync_byDF_MultiValueGroup('eel_wafer_part', \
                                                              'eel_wafer_part_id', \
                                                              'label_name', \
                                                              vList, \
                                                              df_wpart)
        return df_group, df_bar, df_part, df_wpart
        
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

    
if __name__ == "__main__":
    pass