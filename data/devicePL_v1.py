# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 13:09:25 2019

@author: dding
"""

import pandas as pd
import numpy as np
from dbConn import dbConn
from scipy import interpolate
import math

rotation = 0 # degreee, ccw
theta = -rotation*math.pi/180

def get_exclusion(exc,pos=2):
    unit = exc[-pos:]
    value = float(exc[:-pos])*1000
    return value, unit

dbPD = dbConn('pd','test','pd')
dbEpi = dbConn('epi','epi','epi')

wafer_name = 'TGR-734-11-A'
wafer = dbEpi.getDF_byUniqueName('epi_wafer','wafer_name',wafer_name)
wafer_id = wafer['wafer_id'][0]
substrate_id = wafer['substrate_id'][0]
substrate = dbEpi.getDF_byID('epi_substrate','substrate_id',substrate_id)
substrate_size = substrate['substrate_diameter_mm'][0]*1000
# get point test data for a wafer
# table,idCol,idValue
pt = dbPD.getDF_byID('v_pd_pointtest','wafer_id',wafer_id)
pt['i_nA'] = pt['i_meas_a']*1e9
pt['absI_nA'] = abs(pt['i_nA'])
pt['xr'] = pt['x_loc']*math.cos(theta)+pt['y_loc']*math.sin(theta)
pt['yr'] = -pt['x_loc']*math.sin(theta)+pt['y_loc']*math.cos(theta)
    
# get PL data for the wafer
pl_summary = dbEpi.getDF_byID('epi_pl2_meas','wafer_id',wafer_id,limit=1)

if len(pl_summary.index) > 0:
    pl_id = pl_summary['pl2_meas_id'][0]
    pl = dbEpi.getDF_byID('epi_pl2_meas_values','pl2_meas_id',pl_id)
    exclusion, _ = get_exclusion(pl_summary['exc_zone'][0])
    x_left_pl = pl['x'].min(axis=0)
    x_right_pl = pl['x'].max(axis=0)
    y_bottom_pl = pl['y'].min(axis=0)
    y_top_pl = pl['y'].max(axis=0)
    
    pl['x_um'] = pl['x']*(substrate_size-2*exclusion)/(x_right_pl-x_left_pl)
    pl['y_um'] = pl['y']*(substrate_size-2*exclusion)/(y_top_pl-y_bottom_pl)

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
    for index,row in pt.iterrows():
        pt.at[index,'peak_eg']=f1(row['xr'],row['yr'])
        pt.at[index,'peak_int']=f2(row['xr'],row['yr'])
        pt.at[index,'int_signal']=f3(row['xr'],row['yr'])
        pt.at[index,'fwhm']=f4(row['xr'],row['yr'])