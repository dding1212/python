# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 10:11:13 2019
This is a class for data uploading
@author: dding
"""

import numpy as np
import pandas as pd
import os
from datetime import datetime
import shutil
from abc import abstractmethod
from dbConn import dbConn
from LIV import LIV
#Please include dbConn.py file in the same folder!

class DataInterface():

    def __init__(self,folders):
        self.folders = folders
        self.dbEpi = dbConn('epi','epi','epi')
        self.dbPD = dbConn('pd','test','pd')
        self.dbLS = dbConn('laser','laser','db00')
        return

    @abstractmethod
    def process_files(self):
        pass
    @abstractmethod
    def parse_file(self):
        pass
    
    @staticmethod
    def getInterface(test,folders):
        #return an instance of the class
        if test == 'TLM': return tlm(folders)
        if test == 'detector' or test == 'Detector' or test== 'PD':
            return detector(folders)
        if test == 'laser' or test == 'EEL' or test == 'VCSEL':
            return laser(folders)
        raise NotImplementedError('Bad DateInterface called: '+ test)

    def move_file(self,file, path):
        fname,fpath = self.split_file(file)
        f_init = file
        f_final = path+"/"+fname
        shutil.move(f_init, f_final)
        return
    
    def get_info_fname(self,file):
        name = file.split('/')
        part = name[-1][:-4].split('_')
        wafer_name = part[0]
        test_type = part[1]
        operator = part[2]
        fTime = self.get_fTime(part[3]) #fTime is the time obtained from file name
        return test_type,wafer_name,operator,fTime
        
    def get_info_fname1(self,file):
        name = file.split('/')
        part = name[-1][:-4].split('_')
        wafer_name = part[0]
        test_type = part[1]
        device_name = part[2]
        fTime = self.get_fTime(part[3]) #fTime is the time obtained from file name
        return test_type,wafer_name,device_name,fTime
    
    def get_fTime(self,dt_str):
        dt = datetime.strptime(dt_str,'%Y%m%d%H%M%S')
        return datetime.strftime(dt,'%Y-%m-%d %H:%M:%S')
    
    def get_file_list(self,path,extension):
        print('get file list')
        #remove last / 
        if path[-1] == '/':
            path = path[:-1]
        files = []
        for f in os.listdir(path):
            if os.path.isfile(path+'/'+f) and f[-4:] == '.'+extension:
                files.append(path+'/'+f)
        return files

    def get_datatime_list(self,dt_strlist):
        out=[]
        for dt_str in dt_strlist:
            temp = self.get_datetime(dt_str)
            out.append(temp)
        return out
    
    def get_datetime(self,dt_str):
        if dt_str.endswith('M'):
            dt = datetime.strptime(dt_str,'%m/%d/%Y %I:%M:%S %p')
        else:
            try:
                dt = datetime.strptime(dt_str,'%m/%d/%Y %H:%M')
            except:
                dt = datetime.strptime(dt_str,'%m/%d/%Y')
        return datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')    

    def split_file(self,file):
        fsplit = file.split('/')
        fname = fsplit[-1]
        fpath = "/".join(fsplit[:-1])
        return fname, fpath

    def read_file(self,file):
        df = pd.read_csv(file,index_col=False)
        return df

    def get_device_id(self,wafer_id,part_id):
        if part_id != 0:
            df = pd.DataFrame({'device_id':[np.nan],
                               'wafer_id':[wafer_id],
                               'part_id':[part_id]})
            idCol = 'device_id'
            df = self.dbPD.sync_byDF('pd_device',idCol,df)
            ID = df[idCol][0]
        else:
            ID = 0
        return ID

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
    
    def arrange_map(self,wafer_id,map_time,temperature,comment):
        df_map = pd.DataFrame({'map_id':np.nan,
                               'wafer_id':wafer_id,
                               'map_time':map_time,
                               'temperature':temperature,
                               'comment':comment}, index=[0])
        return df_map

class tlm(DataInterface):
    def __init__(self,folders):
        super().__init__(folders)
        return

    def process_files(self):
        print('move plots')
        path = self.folders['root']
        plot_files = self.__get_plots_list(path)
        self.__move_plots_files(plot_files)
        
        files=self.get_file_list(path,'csv')
        print('process test files')
        if files!=[]:
            for f in files:
                # folders are upload and failedd folders
                
                isGood, reason = self.parse_file(f)
                if isGood == True:
                    self.move_file(f,self.folders['uploaded'])
                else:
                            self.move_file(f,self.folders['failed'])
                print (reason)
        else:
            print("no file to process")
        return 
    
    def parse_file(self,file):
        fname,_=self.split_file(file)
        print('parsing ' + fname)
        test_type,wafer_name,operator,fTime=self.get_info_fname(file)
        if test_type=='TLM':
            df = self.read_file(file)
            n = len(df)
            if n>=1:
                lst_test_time = self.get_datatime_list(list(df.test_time))
                lst_map_time = self.get_datatime_list(list(df.map_time))
                
                df_tlm = pd.DataFrame({'ctlm_id':[np.nan]*n,
                                       'wafer_id':list(df.wafer_id),
                                       'tlm_mask_id':list(df.tlm_mask_id),
                                       'operator':list(df.operator),
                                       'test_time':lst_test_time,
                                       'map_time':lst_map_time,
                                       'r_total_median':list(df.rtotal_med),
                                       'r_sheet':list(df.r_sheet),
                                       'r_contact':list(df.r_contact),
                                       'transfer_length_um':list(df['lt']),
                                       'r_square':list(df.r_square),
                                       'residual':list(df.residual),
                                       'min_r2_iv':list(df.min_r2_iv),
                                       'max_residual_iv':list(df.max_residual_iv),
                                       'fitting_method':list(df.fitting_method),
                                       'comment':list(df.comment)
                                       })
            isExist = self.dbPD.sync_byDF_grouptime('ctlm','ctlm_id','map_time',df_tlm)
            if isExist == True:
                isGood = True
                reason = "Skip"
            else:
                isGood = True
                reason = "Completed"
        else:
            print('wrong test type = ' + test_type)
            isGood = False
            reason = "wrong test type"
        return isGood, reason
    
    def __get_plots_list(self,path):
        files=self.get_file_list(path,'png')
        return files

    def __move_plots_files(self,files):
        if files!=[]:
            for f in files:
                self.move_file(f,self.folders['plot'])
        else:
            print('No plots to move')
        return
    
class detector(DataInterface):
    
    def __init__(self,folders):
        super().__init__(folders)
        return
    
    def process_files(self):
        path = self.folders['root']
        files=self.get_file_list(path,'csv')
        print('process files')
        if files!=[]:
            for f in files:
                # folders are upload and failedd folders
                
                isGood, reason = self.parse_file(f)
                if isGood == True:
                    self.move_file(f,self.folders['uploaded'])
                else:
                            self.move_file(f,self.folders['failed'])
                print ("Completed")
        else:
            print("no file to process")
        return 
    
    def parse_file(self,file):
        fname,_=self.split_file(file)
        print('parsing ' + fname)
        test_type,wafer_name,operator,fTime=self.get_info_fname(file)
        df, df_value = self.read_file(file, test_type)
        if test_type == 'PT' or test_type == 'CP':
            map_time = fTime
        elif test_type == 'ST':
            map_time = self.get_datetime(df['map_time'][0])
        map_id, wafer_id = self.update_map(df,map_time)
        
        if test_type == 'PT':
            # work on each row of df
            for index, row in df.iterrows():
                part_id = row['part_id']
                device_id = self.get_device_id(wafer_id,part_id)
                test_time = self.get_datetime(row['test_time'])
                if device_id!=0:
                    df_pt = pd.DataFrame({'rawtest_id':np.nan,
                                          'map_id':map_id,
                                          'device_id':device_id,
                                          'test_time':test_time,
                                          'laser_mw':row['laser_mw'],
                                          'i_meas_a':row['i_meas'],
                                          'recipe_id':row['recipe_id']
                                          },index=[0])
                    df_pt = self.dbPD.sync_byDF('pd_rawtest','rawtest_id',df_pt)
            isGood = True
            reason = ""

        elif test_type == 'ST':
            part_id = df['part_id'][0]
            device_id = self.get_device_id(wafer_id,part_id)
            test_time = fTime
            if device_id!=0:
                df_st = pd.DataFrame({'iv_id':np.nan,
                                      'map_id':map_id,
                                      'device_id':device_id,
                                      'test_time':test_time,
                                      'laser_mw':df['laser_mw'][0],
                                      'iv_recipe_id':df['recipe_id'][0],
                                      'temperature':df['temperature'][0]
                                      },index=[0])
                df_st = self.dbPD.sync_byDF('pd_iv','iv_id',df_st)
                iv_id = df_st['iv_id'][0]
                points = len(df_value.index)
                df_st_value = pd.DataFrame({'iv_id':[iv_id]*points,
                                            'v':list(df_value.v),
                                            'i':list(df_value.i)
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
        
        elif test_type == 'CP':
            # work on each row of df
            for index, row in df.iterrows():
                part_id = row['part_id']
                device_id = self.get_device_id(wafer_id,part_id)
                test_time = self.get_datetime(row['test_time'])
                if device_id!=0:
                    df_cp = pd.DataFrame({'ctest_id':np.nan,
                                          'map_id':map_id,
                                          'device_id':device_id,
                                          'cv_setting_id':row['setting_id'],
                                          'test_time':test_time,
                                          'tool_id':'3',
                                          'c_pf':row['c_pf'],
                                          'v_set_v':row['v_set'],
                                          'g_mho':row['g_mho'],
                                          'overload':row['overload']
                                          },index=[0])
                    df_cp = self.dbPD.sync_byDF('pd_ctest','ctest_id',df_cp)
                else:
                    isGood = False
                    reason = "no device_id"
            isGood = True
            reason = ""

        else:
            isGood = False
            reason = "No test type"
        return isGood, reason


    def read_file(self,file,test_type):
        if test_type == 'CP':
            df = pd.read_csv(file,index_col=False)
            df = df.dropna(subset = ['c_pf'])
            df_values = []
        elif test_type == 'PT':
            df = pd.read_csv(file,index_col=False)
            df_values = []
        elif test_type == 'ST':
            df = pd.read_csv(file,index_col=0,header=None,names=['0'], nrows=11).T
            df_values = pd.read_csv(file,skiprows=14)
        return df, df_values
        
class laser(DataInterface):
    def __init__(self,folders):
        super().__init__(folders)
        self.LIV_processor = LIV()
        return
    
    def process_files(self,checkID=0):
        path = self.folders['root']
        files=self.get_file_list(path,'csv')
        print('process files')
        if files!=[]:
            for f in files:
                # folders are upload and failedd folders
                
                isGood, reason = self.parse_file(f,checkID)
                if isGood == True:
                    self.move_file(f,self.folders['uploaded'])
                else:
                            self.move_file(f,self.folders['failed'])
                print ("Completed")
        else:
            print("no file to process")
        return 
    
    def parse_file(self,file,checkID=0):
        fname,_=self.split_file(file)
        print('parsing ' + fname)
        test_type,wafer_name,device_name,fTime = self.get_info_fname1(file)
        df, df_value = self.read_file(file, test_type)

        if test_type=='ELV':
            #smu_id is 1 (K2520 for EEL)
            map_id = 0 #no wafer mapping
            test_time = fTime
            if wafer_name == 'External':
                external_id = int(device_name) #same as wpart_id in df
                df_liv = pd.DataFrame({'liv_id':np.nan,
                                       'recipe_id':df['recipe_id'].iloc[0],
                                       'device_id':external_id,
                                       'map_id':map_id,
                                       'test_time':test_time,
                                       'temperature':df['temperature'].iloc[0],
                                       'assigned_wl_nm':df['wavelength'].iloc[0],
                                       'operator':df['operator'].iloc[0],
                                       'is_external':1,
                                       'data_status_id':1,
                                       'is_pulse':1,
                                       'laser_type_id':3,
                                       'liv_sensor_id':df['sensor_id'].iloc[0],
                                       'liv_smu_id':1,
                                       'comment':df['comment'].iloc[0]
                                       },index=[0])
            else:
                wafer_id = df['wafer_id'].iloc[0]
                wpart_id = df['wpart_id'].iloc[0]
                if checkID==1:
                    newID = self.check_EEL_wpartID(device_name,wpart_id)
                    if newID!=0:
                        wpart_id = newID
                device_id = self.get_eel_device_id(wafer_id,wpart_id)
                if device_id != 0:
                    df_liv = pd.DataFrame({'liv_id':np.nan,
                                           'recipe_id':df['recipe_id'].iloc[0],
                                           'device_id':device_id,
                                           'map_id':map_id,
                                           'test_time':test_time,
                                           'temperature':df['temperature'].iloc[0],
                                           'assigned_wl_nm':df['wavelength'].iloc[0],
                                           'operator':df['operator'].iloc[0],
                                           'is_external':0,
                                           'data_status_id':1,
                                           'is_pulse':1,
                                           'laser_type_id':1,
                                           'liv_sensor_id':df['sensor_id'].iloc[0],
                                           'liv_smu_id':1,
                                           'comment':df['comment'].iloc[0]
                                           },index=[0])
            
            df_liv = self.dbLS.sync_byDF('liv','liv_id',df_liv)
            liv_id = df_liv['liv_id'][0]
            points = len(df_value.index)
            df_liv_value = pd.DataFrame({'liv_id':[liv_id]*points,
                                         'i_ma':list(df_value.I_mA),
                                         'v':list(df_value.V_V),
                                         'l_mw':list(df_value.L_mW)
                                         })
            isExist = self.dbLS.sync_byDF_group('liv_meas','liv_id',df_liv_value)
            
            #process LIV data
            self.LIV_processor.process_data_byID(liv_id)
                
            if isExist == True:
                isGood = True
                reason = "skip"
            else:
                isGood = True
                reason = ""
        else:
            isGood = False
            reason = "no device_id"
        return isGood, reason
    
    def check_EEL_wpartID(self, device_name, wpart_id):
        C,R,X = self.get_EEL_part(device_name)
        ID = self.get_EEL_wpartID(C,R,X)
        if ID!=0 and ID!=wpart_id:
            newID = ID
        else:
            newID = 0
        return newID
    
    def get_EEL_part(self, device_name, mask_id=1):
        name = device_name.split('-')
        eC = int(name[0])
        eR = int(name[1])
        eX = int(name[2])
        return eC, eR, eX
    
    def get_EEL_wpartID(self, eC, eR, eX, mask_id=1):
        sql = "SELECT m.eel_wpart_id\r\n" + \
              "FROM laser.v_eel_mask m\r\n" + \
              "WHERE m.mask_id = "+str(mask_id)+ \
              " AND m.col = "+str(eC)+ \
              " AND m.row = "+str(eR)+ \
              " AND m.x_pos = "+str(eX) + ";"
        ID = self.dbLS.getID_bySQL_public('eel_wpart_id',sql)
        return ID
        
    
    def read_file(self,file,test_type):
        if test_type == 'ELV':
            df = pd.read_csv(file,index_col=0,header=None,nrows=11).T
            df = df.drop([2])
            df_values = pd.read_csv(file,skiprows=15)
        return df, df_values
    
    def get_eel_device_id(self,wafer_id,wpart_id):
        if wpart_id != 0:
            df = pd.DataFrame({'device_id':[np.nan],
                               'wafer_id':[wafer_id],
                               'wpart_id':[wpart_id]})
            idCol = 'device_id'
            df = self.dbLS.sync_byDF('eel_device',idCol,df)
            ID = df[idCol][0]
        else:
            ID = 0
        return ID
        