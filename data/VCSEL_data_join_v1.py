# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 12:59:37 2019
Read raw data from Ladybug and Ge sensor
@author: dding/alec
"""

import pandas as pd
#import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()
fd= filedialog.askdirectory()

files = [f for f in listdir(fd) if isfile(join(fd, f))]
fRaw = []

for f in files:
    if f.find('LivRaw')>=0:
        fRaw.append(f)
i = 0

for f in fRaw:
    name1 = fd + "/" + f
    fSummary = f.replace('Raw','Summary')
    name2 = fd + "/" + fSummary
    
    dfRaw = pd.read_csv(name1,header=None,skiprows=1,index_col=0)
    cols1=pd.read_csv(name1, nrows=0, index_col=0).columns.tolist()
    cols1=[x.strip() for x in cols1]
    dfRaw.columns = cols1
    
    dfSummary = pd.read_csv(name2,header=None, skiprows=1, index_col=0)
    cols2 = pd.read_csv(name2, nrows=0,index_col=0).columns.tolist()
    cols2 = [x.strip() for x in cols2]
    del cols2[0]
    num_missing_cols = len(dfSummary.columns)-len(cols2)
    new_cols = ['col' + str(i+1) for i in range(num_missing_cols)]
    dfSummary.columns = cols2 + new_cols
    dfTemp = dfSummary.join(dfRaw, how='inner',sort=False)
    if i==0:
        dfOut = dfTemp
    else:
        dfOut = dfOut.append(dfTemp)
    i+=1

dfOut.to_csv(fd+'/'+fd.rsplit('/')[-1]+'.csv', index_label='ID')