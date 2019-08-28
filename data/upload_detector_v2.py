# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 16:32:25 2019
Detector IV data uploading
@author: dding
"""
import numpy as np
import pandas as pd
import os
from datetime import datetime
import shutil
#Please include dbConn.py file in the same folder!
from dbConn import dbConn

class detector():
    
    def __init__(self,folder):
        self.folders = folders
        #self.dbEpi = dbConn('epi','epi','epi')
        self.dbPD = dbConn('pd','test','pd')
        
    def get_file_list(self,path):
        print('get file list')
        #remove last / 
        if path[-1] == '/':
            path = path[:-1]
        files = []
        for f in os.listdir(path):
            if os.path.isfile(path+'/'+f) and f[-4:] == '.csv':
                files.append(path+'/'+f)
        return files

    def process_files(self,files):
        print('process files')
        if files!=[]:
            for f in files:
                # folders are upload and failedd folders
                
                isGood, reason = self.parse_file(f)
                if isGood == True:
                    self.move_file(f,folders['uploaded'])
                else:
                            self.move_file(f,folders['failed'])
                print ("Completed")
        else:
            print("no file to process")
        return 
    
    def split_file(self,file):
        fsplit = file.split('/')
        fname = fsplit[-1]
        fpath = "/".join(fsplit[:-1])
        return fname, fpath

    def move_file(self,file, path):
        fname,fpath = self.split_file(file)
        f_init = file
        f_final = path+"/"+fname
        shutil.move(f_init, f_final) 
    
    def read_file(self,file,test_type):
        if test_type == 'PT':
            df = pd.read_csv(file,index_col=False)
            df_iv = []
        elif test_type == 'ST':
            df = pd.read_csv(file,index_col=0,header=None,names=['0'], nrows=11).T
            df_iv = pd.read_csv(file,skiprows=14)
        return df, df_iv
    
    def update_map(self,df,map_time):
        wafer_id = df['wafer_id'][0]
        temperature = df['temperature'][0]
        if pd.isna(df['comment'][0]) == True:
            comment = ""
        else:
            comment = df['comment'][0]
        df_map = self.arrange_map(wafer_id,map_time,temperature,comment)
        df_map = self.dbPD.sync_byDF('pd_map','map_id',df_map,'comment')
        map_id = df_map['map_id'][0]
        return map_id, wafer_id
    
    def parse_file(self,file):
        print (file)
        
        test_type,wafer_name,operator,fTime=self.get_info_fname(file)
        df, df_value = self.read_file(file, test_type)
        if test_type=='PT':
            map_time = fTime
        elif test_type=='ST':
            map_time = self.get_datetime(df['map_time'][0])
        map_id, wafer_id = self.update_map(df,map_time)
        
        if test_type == 'PT':
            # work on each row of df
            for index, row in df.iterrows():
                part_id = row['part_id']
                device_id = self.get_device_id(wafer_id,part_id)
                test_time = self.get_datetime(row['test_time'])
                if device_id!=0:
                    df_pt = pd.DataFrame({'rawtest_id':np.nan,\
                                          'map_id':map_id,\
                                          'device_id':device_id,\
                                          'test_time':test_time,\
                                          'laser_mw':row['laser_mw'],\
                                          'i_meas_a':row['i_meas'],\
                                          'recipe_id':row['recipe_id']\
                                          },index=[0])
                    df_pt = self.dbPD.sync_byDF('pd_rawtest','rawtest_id',df_pt)
            isGood = True
            reason = ""

        elif test_type == 'ST':
            part_id = df['part_id'][0]
            device_id = self.get_device_id(wafer_id,part_id)
            test_time = fTime
            if device_id!=0:
                df_st = pd.DataFrame({'iv_id':np.nan,\
                                      'map_id':map_id,\
                                      'device_id':device_id,\
                                      'test_time':test_time,\
                                      'laser_mw':df['laser_mw'][0],\
                                      'iv_recipe_id':df['recipe_id'][0],\
                                      'temperature':df['temperature'][0]\
                                      },index=[0])
                df_st = self.dbPD.sync_byDF('pd_iv','iv_id',df_st)
                iv_id = df_st['iv_id'][0]
                points = len(df_value.index)
                df_st_value = pd.DataFrame({'iv_id':[iv_id]*points,\
                                            'v':list(df_value.v),\
                                            'i':list(df_value.i)\
                                            })
                isExist = self.dbPD.sync_byDF_group('pd_iv_meas','iv_id',df_st_value)
                if isExist == True:
                    isGood = True
                    reason = "skip"
                else:
                    isGood = True
                    reason = ""
            else:
                isGood = False
                reason = "no device_id"
        else:
            isGood = False
            reason = "No test type"
        return isGood, reason
    
    def get_device_id(self,wafer_id,part_id):
        if part_id != 0:
            df = pd.DataFrame({'device_id':[np.nan],\
                               'wafer_id':[wafer_id],\
                               'part_id':[part_id]})
            idCol = 'device_id'
            df = self.dbPD.sync_byDF('pd_device',idCol,df)
            ID = df[idCol][0]
        else:
            ID = 0
        return ID
        
    def get_datetime(self,dt_str):
        if dt_str.endswith('M'):
            dt = datetime.strptime(dt_str,'%m/%d/%Y %I:%M:%S %p')
        else:
            try:
                dt = datetime.strptime(dt_str,'%m/%d/%Y %H:%M')
            except:
                dt = datetime.strptime(dt_str,'%m/%d/%Y')
        return datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')
    
    def arrange_map(self,wafer_id,map_time,temperature,comment):
        df_map = pd.DataFrame({'map_id':np.nan,\
                               'wafer_id':wafer_id,\
                               'map_time':map_time,\
                               'temperature':temperature,\
                               'comment':comment}, index=[0])
        return df_map

    def get_info_fname(self,file):
        name = file.split('/')
        part = name[-1][:-4].split('_')
        wafer_name = part[0]
        test_type = part[1]
        operator = part[2]
        fTime = self.get_fTime(part[3]) #fTime is the time obtained from file name
        return test_type,wafer_name,operator,fTime
    
    def get_fTime(self,dt_str):
        dt = datetime.strptime(dt_str,'%Y%m%d%H%M%S')
        return datetime.strftime(dt,'%Y-%m-%d %H:%M:%S')

    
if __name__ == "__main__":
    path = '//sjt-fs00/MaterialsTeam/ALL/Characterization Data/DetectorIV'
    folders = {'root':path,\
               'uploaded':path+'/'+'0_Uploaded',\
               'failed':path+'/'+'0_Upload_Failed',\
               'old':path+'/'+'0_Old'}
    test = detector(folders)
    files = test.get_file_list(path)
    test.process_files(files)