# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 13:09:25 2019
Change to class pdPL
@author: dding
"""
import pandas as pd
import numpy as np
from dbConn import dbConn
from scipy import interpolate
import math

class pdPL():

    def __init__(self,wafer_list,rot_list=['TGR-491-1-A','TGR-491-4-A','TGR-491-11-A','TGR-491-12-A','TGR-491-13-A'], rot=45):
        self.wafer_list = wafer_list
        self.rot_list = rot_list
        self.theta = -rot*math.pi/180
        self.dbPD = dbConn('pd','test','pd')
        self.dbEpi = dbConn('epi','epi','epi')

    def __get_exclusion(self,exc,pos=2):
        unit = exc[-pos:]
        # value in um
        value = float(exc[:-pos])*1000
        return value, unit
    
    def get_info(self,wafer_name):
        wafer = self.dbEpi.getDF_byUniqueName('epi_wafer','wafer_name',wafer_name)
        wafer_id = wafer['wafer_id'][0]
        substrate_id = wafer['substrate_id'][0]
        substrate = self.dbEpi.getDF_byID('epi_substrate','substrate_id',substrate_id)
        substrate_size = 25*substrate['substrate_diameter'][0]*1000

        return wafer_id, substrate_size
    
    def get_device(self,wafer_name,wafer_id):
        # get device coordinates for a wafer
        dv = self.dbPD.getDF_byID('v_pd_device','wafer_id',wafer_id)
        if wafer_name in self.rot_list:
            dv['xr'] = dv['x_loc']*math.cos(self.theta)+dv['y_loc']*math.sin(self.theta)
            dv['yr'] = -dv['x_loc']*math.sin(self.theta)+dv['y_loc']*math.cos(self.theta)
        else:
            dv['xr'] = dv['x_loc']
            dv['yr'] = dv['y_loc']
        return dv
    
    def getPL(self,wafer_id,substrate_size):
        # get PL data for the wafer; using idSort=pl2_meas_id and limit=1
        pl_summary = self.dbEpi.getDF_byID('epi_pl2_meas','wafer_id',wafer_id,'pl2_meas_id',limit=1)
        if len(pl_summary.index) > 0:
            pl_id = pl_summary['pl2_meas_id'][0]
            pl = self.dbEpi.getDF_byID('epi_pl2_meas_values','pl2_meas_id',pl_id)
            exclusion, _ = self.__get_exclusion(pl_summary['exc_zone'][0])
            x_left_pl = pl['x'].min(axis=0)
            x_right_pl = pl['x'].max(axis=0)
            y_bottom_pl = pl['y'].min(axis=0)
            y_top_pl = pl['y'].max(axis=0)
            
            pl['x_um'] = pl['x']*(substrate_size-2*exclusion)/(x_right_pl-x_left_pl)
            pl['y_um'] = pl['y']*(substrate_size-2*exclusion)/(y_top_pl-y_bottom_pl)
        else:
            pl = self.dbEpi.initDF_byTable('epi_pl2_meas_values')
        return pl

    def pl_interp(self,dv,pl):
        x = np.sort(pl.x_um.unique())
        y = -np.sort(pl.y_um.unique())
        
        z1 = np.zeros((len(x),len(y)))
        z2 = np.zeros((len(x),len(y)))
        z3 = np.zeros((len(x),len(y)))
        z4 = np.zeros((len(x),len(y)))
        
        for index,row in pl.iterrows():
            ix = np.where(x == row['x_um'])
            iy = np.where(y == row['y_um'])
            z1[iy,ix]=row['peak_lambda']
            z2[iy,ix]=row['peak_int']
            z3[iy,ix]=row['int_signal']
            z4[iy,ix]=row['fwhm']
        f1 = interpolate.interp2d(x,y,z1,kind='linear')
        f2 = interpolate.interp2d(x,y,z2,kind='linear')
        f3 = interpolate.interp2d(x,y,z3,kind='linear')
        f4 = interpolate.interp2d(x,y,z4,kind='linear')
        for index,row in dv.iterrows():
            dv.at[index,'pk_lambda_ev']=f1(row['xr'],row['yr'])
            dv.at[index,'pk_int']=f2(row['xr'],row['yr'])
            dv.at[index,'int_signal']=f3(row['xr'],row['yr'])
            dv.at[index,'fwhm']=f4(row['xr'],row['yr'])
        return dv
    
    def write_db(self,dv):
        n = len(dv.index)
        pd_pl = pd.DataFrame({'pl_id':[np.nan]*n})
        pd_pl['device_id'] = dv['device_id']
        pd_pl['pk_lambda_ev'] = dv['pk_lambda_ev']
        pd_pl['pk_int'] = dv['pk_int']
        pd_pl['int_signal'] = dv['int_signal']
        pd_pl['fwhm'] = dv['fwhm']
        pd_pl = self.dbPD.sync_byDF('pd_pl','pl_id',pd_pl)
        return pd_pl

    def process(self):
        
        for wafer_name in self.wafer_list:
            print("work on " + wafer_name)
            wafer_id,substrate_size = self.get_info(wafer_name)
            dv = self.get_device(wafer_name,wafer_id)
            pl = self.getPL(wafer_id,substrate_size)
            if len(pl.index)>0:
                dv = self.pl_interp(dv,pl)
                dv_pl = self.write_db(dv)
                print (wafer_name + " done")
            else:
                print(wafer_name + " has no PL2 data")
        return 

if __name__ == "__main__":
    wafer_list = ['TGR-668-6-A']
    test = pdPL(wafer_list)
    test.process()
    test.dbEpi.close()
    test.dbPD.close()
