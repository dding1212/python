# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 11:53:20 2020

@author: dding
"""

import numpy as np
from scipy import stats
from scipy import interpolate
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from dbConn import dbConn
import matplotlib.pyplot as plt

class LIV():
    
    def __init__(self,isPulse=True):
        self.isPulse = isPulse
        self.db = dbConn('db00','laser','db00')
        return
    
    def pick_files(self):
        root = tk.Tk()
        root.withdraw()
        files = filedialog.askopenfilenames(filetypes=[("CSV Files",".csv")])
        return files

    def pick_file(self):
        root = tk.Tk()
        root.withdraw()
        file = filedialog.askopenfilenames(filetypes=[("CSV File",".csv")])
        return file

    def get_info_fname1(self,file):
        name = file.split('/')
        part = name[-1][:-4].split('_')
        wafer_name = part[0]
        test_type = part[1]
        device_name = part[2]
        fTime = self.get_fTime(part[3]) #fTime is the time obtained from file name
        return test_type,wafer_name,device_name,fTime

    def split_file(self,file):
        fsplit = file.split('/')
        fname = fsplit[-1]
        fpath = "/".join(fsplit[:-1])
        return fname, fpath
    
    def read_file(self,file,test_type):
        if test_type == 'ELV':
            df_file = pd.read_csv(file,index_col=0,header=None,nrows=10).T
            df_file = df_file.drop([2])
            df_values = pd.read_csv(file,skiprows=11)
            df_values.rename(columns={"I_mA":"i_ma","V_V":"V","L_mW":"l_mw"})
        return df_file, df_values
    
    def read_data_byPickFile(self):
        file = self.pick_file
        fname,_=self.split_file(file)
        test_type,_,_,_=self.get_info_fname1(fname)
        df_values = self.read_file(file,test_type)
        return df_values
    
    def read_data_byID(self,liv_id):
        df = self.db.getDF_byID('liv','liv_id',liv_id)
        df_values = self.db.getDF_byID('liv_meas','liv_id',liv_id)
        return df,df_values
    
    def process_data_byID(self,liv_id):
        print('processing liv, ID = ' + str(liv_id))
        df, df_values = self.read_data_byID(liv_id)
        if df_values.empty == False:
            df_summary = self.get_summary(liv_id,df_values)
            self.db.sync_byDF('liv_summary','liv_summary_id',df_summary)
            check = df_values[df_values['l_mw']>1]
            if check.empty == False:
                
                isFailed1,ith1 = self.get_ith_1(df_values)
                isFailed2,ith2 = self.get_ith_2(df_values)
                isFailed3,ith3 = self.get_ith_3(df_values)
                isFailed4,ith4 = self.get_ith_4(df_values)
                
                if isFailed1 == False:
                    df_th1 = self.get_th_data(liv_id,df_values,ith1,method=1)
                    self.db.sync_byDF('liv_th','liv_th_id',df_th1)
                else:
                    print('method 1 failed')
        
                if isFailed2 == False:
                    df_th2 = self.get_th_data(liv_id,df_values,ith2,method=2)
                    self.db.sync_byDF('liv_th','liv_th_id',df_th2)
                else:
                    print('method 2 failed')
                    
                if isFailed3 == False:
                    df_th3 = self.get_th_data(liv_id,df_values,ith3,method=3)
                    self.db.sync_byDF('liv_th','liv_th_id',df_th3)
                else:
                    print('method 3 failed')
        
                if isFailed4 == False:
                    df_th4 = self.get_th_data(liv_id,df_values,ith4,method=4)
                    self.db.sync_byDF('liv_th','liv_th_id',df_th4)
                else:
                    print('method 4 failed')
    
            print ('liv summary done!')
        else:
            print('empty liv_meas!')
        return 

    def plot_raw(self,df_values):
        I = df_values['i_ma']
        V = df_values['v']
        L = df_values['l_mw']
        SE = self.get_slope_eff(I,L)
        PCE = self.get_pce(I,L,V)
        secDiff = self.get_slope_eff(I,SE)

        fig = plt.figure(figsize=(8,6),dpi=70)
        fig.suptitle ('LIV',fontsize=24)
        fig,(axLI,axVI,ax3,ax4,ax5) = plt.subplots(5,1)
        axLI.plot(I,L)
        axLI.set_ylabel('L (mW)')
        axVI.plot(I,V)
        axVI.set_ylabel('V (V)')
        ax3.plot(I,SE)
        ax3.set_ylabel('Slope')
        ax4.plot(I,secDiff)
        ax4.set_ylabel('2nd')
        ax5.plot(I,PCE)
        ax5.set_xlabel('I (mA)')
        ax5.set_ylabel('PCE')
        plt.show()
        return fig, axLI, axVI, ax3
    '''
    def process_LIV(self,df_values,method=4):
        if method==1:
            slope, interception, r_quare = self.process_LIV_1(df_values)
        return df_summary
    '''
    
    def get_above_tol(self,df_values,tol=1e-4,r2_min=0.9995):
        above_tol = df_values[df_values['l_mw']>tol]
        if above_tol.empty == True:
            isFail = True
            slope = 0
            interception = 0
        else:
            n = len(above_tol.index)
            above_tol = above_tol.assign(slope = [np.nan]*n)
            above_tol = above_tol.assign(interception = [np.nan]*n)
            above_tol = above_tol.assign(r_square = [np.nan]*n)
            i0 = above_tol.index[0]
            for i,row in above_tol.iterrows():
                if i>i0:
                    x = above_tol.loc[i0:i,'i_ma'].values
                    y = above_tol.loc[i0:i,'l_mw'].values
                    
                    slope, interception, r_square = self.linear_fit(x,y)
                else:
                    slope=np.nan
                    interception=np.nan
                    r_square = 1
                above_tol.at[i,'slope'] = slope
                above_tol.at[i, 'interception'] = interception
                above_tol.at[i,'r_square']= r_square
            df_select = above_tol[above_tol['r_square']<r2_min]
            
            if df_select.empty == True:
                isFail = True
                slope = 0
                interception = 0
            else:
                isFail = False
                slope = df_select['slope'].iloc[0]
                interception = df_select['interception'].iloc[0]
        return isFail,slope,interception
    
    def get_below_tol(self,df_values,tol=1e-4):
        below_tol = df_values[df_values['l_mw']<=tol]
        if below_tol.empty==True:
            isFail = True
            slope = 0
            interception = 0
        else:
            isFail = False
            x = below_tol['i_ma'].values
            y = below_tol['l_mw'].values
            slope,interception,_ = self.linear_fit(x,y)
        return isFail, slope, interception
    
    def get_ith_1(self,df_values,tol=1e-4,r2_min=0.9995,isplot=0):
        # linear fit method
        if df_values['l_mw'].iloc[0] > tol:
            tol = df_values['l_mw'].iloc[0]*100
        isFail,slope,interception = self.get_above_tol(df_values,tol=tol,r2_min=r2_min)
        
        if isFail == True:
            isFailed = True
            ith = -1
        else:
            ith = -interception/slope
            if ith<0 or ith>df_values['i_ma'].max():
                isFailed = True
                ith = -1
            else:
                isFailed = False
            if isplot==1:
                
                I = df_values['i_ma']
                L = df_values['l_mw']
                I1 = np.arange(0,int(max(I))+1,1)
                L1 = interception + I1*slope
                
                fig,ax = plt.subplots()
                ax.plot(I,L,I1,L1)
                s = 'Ith = ' +str(int(ith*1000+0.5)/1000) + 'mA'
                plt.text(0.1,50,s)
                plt.show()
        
        return isFailed, ith
    
    def get_ith_2(self,df_values,tol=1e-4,r2_min=0.9995,isplot=0):
        # two-segment fit
        if df_values['l_mw'].iloc[0] > tol:
            tol = df_values['l_mw'].iloc[0]*100
        isFail1,m1,b1 = self.get_above_tol(df_values,tol=tol,r2_min=r2_min)
        isFail2,m2,b2 = self.get_below_tol(df_values,tol=tol)
        
        if isFail1==False and isFail2 == False:
            ith = (b1-b2)/(m2-m1)
            if ith<0 or ith>df_values['i_ma'].max():
                isFailed = True
                ith = -1
            else:
                isFailed = False
            if isplot==1:
                I = df_values['i_ma']
                L = df_values['l_mw']
                I1 = np.arange(0,int(max(I))+1,1)
                L1 = I1*m1+b1
                L2 = I1*m2+b2
                
                fig,ax=plt.subplots()
                ax.plot(I,L,I1,L1,I1,L2)
                s = 'Ith = ' +str(int(ith*1000+0.5)/1000) + 'mA'
                plt.text(0.1,50,s)
                plt.show()
        else:
            ith = -1
            isFailed = True
        
        return isFailed, ith
    
    def get_ith_3(self,df_values,min_diff=0.001,isplot=0):
        # first derivative
        I = df_values['i_ma'].values
        L = df_values['l_mw'].values
        SE = self.get_slope_eff(I,L)
        imax = np.argmax(SE)
        if imax < len(SE)-1:
            pkSE = SE[imax+1]
            x_temp = I[0:imax+1];
            y_temp = SE[0:imax+1];
            diff = np.gradient(y_temp,x_temp)
            i = np.argmax(diff>min_diff)
            if i==0:
                isFailed =True
                ith = -1
            else:
                isFailed = False
                x = x_temp[i:]
                y = y_temp[i:]
                f= interpolate.interp1d(y,x)
                y1 = pkSE/2.0
                ith = f(y1).item()
                if isplot==1:
                    fig,ax=plt.subplots()
                    ax.plot(I,SE)
                    ax.plot(ith,y1,'o')
                    s = 'Ith = ' +str(int(ith*1000+0.5)/1000) + 'mA'
                    plt.text(0.1,0.1,s)
                    plt.show()
        else:
            isFailed=True
            ith = -1
        return isFailed,ith
    
    def get_ith_4(self,df_values,isplot=0):
        #second derivative
        
        I = df_values['i_ma'].values
        L = df_values['l_mw'].values
        SE = self.get_slope_eff(I,L)
        d2L = self.get_slope_eff(I,SE)
        Ih = np.arange(np.amin(I),np.amax(I),0.1)
        f = interpolate.interp1d(I,d2L,kind='cubic')
        d2Lh = f(Ih)
        imax = np.argmax(d2Lh)
        if imax==0:
            isFail = True
            ith = 0
        else:
            
            isFail = False
            ith = Ih[imax]
            if isplot == 1:
                fig,ax=plt.subplots()
                ax.plot(I,d2L,'o')
                ax.plot(Ih,d2Lh,'-')
                ax.set_xlim(0,200)
                s = 'Ith = ' +str(int(ith*1000+0.5)/1000) + 'mA'
                plt.text(10,0.002,s)
                plt.show()
        return isFail, ith
    
    def get_th_data(self,liv_id,df_values,ith,method):
        I = df_values['i_ma'].values
        V = df_values['v'].values
        f = interpolate.interp1d(I,V)
        vth = f(ith)
        df_th = pd.DataFrame({'liv_th_id':[np.nan],
                              'liv_id':[liv_id],
                              'i_th_ma':[ith],
                              'v_th':[vth],
                              'method_id':[method],
                              'data_status_id':[1]
                              },index=[0])
        return df_th
    
    def get_summary(self,liv_id,df_values):
        
        I = df_values['i_ma'].values
        L = df_values['l_mw'].values
        V = df_values['v'].values

        imax = np.argmax(L)
        lpk = L[imax]
        ipk = I[imax]
        vpk = V[imax]
        SE = self.get_slope_eff(I,L)
        imax = np.argmax(SE)
        pkSE = SE[imax]
        ipkSE = I[imax]
        vpkSE = V[imax]
        PCE = self.get_pce(I,L,V)
        imax = np.argmax(PCE)
        pkPCE = PCE[imax]
        ipkPCE = I[imax]
        vpkPCE = V[imax]
        
        df_summary = pd.DataFrame({'liv_summary_id':[np.nan],
                                   'liv_id':[liv_id],
                                   'pk_power_mw':[lpk],
                                   'i_pk_power':[ipk],
                                   'v_pk_power':[vpk],
                                   'pk_slope_efficiency':[pkSE],
                                   'i_pk_slope_efficiency':[ipkSE],
                                   'v_pk_slope_efficiency':[vpkSE],
                                   'pk_pce':[pkPCE],
                                   'i_pk_pce':[ipkPCE],
                                   'v_pk_pce':[vpkPCE]
                                   },index=[0])
        return df_summary
    
    def get_slope_eff(self,I,L):
        se = np.gradient(L,I)
        return se
    
    def get_pce(self,I,L,V):
        np.seterr(divide='ignore', invalid='ignore')
        pce = L/(I*V)
        pce[pce == np.inf] = 0
        return pce
    
    def linear_fit(self,x,y):
        slope, interception, r_quare, _, _ = stats.linregress(x,y)
        return slope, interception, r_quare
    
if __name__ == "__main__":
    test = LIV()
    df, df_values = test.read_data_byID(218)
    fig, axLI, axVI, ax3 = test.plot_raw(df_values)
    isFailed1,ith1 = test.get_ith_1(df_values,isplot=1)
    isFailed2,ith2 = test.get_ith_2(df_values,isplot=1)
    isFailed3,ith3 = test.get_ith_3(df_values,isplot=1)
    isFailed4,ith4 = test.get_ith_4(df_values,isplot=1)
    if isFailed1 == False:
        df_th1 = test.get_th_data(1,df_values,ith1,1)
    df_summary = test.get_summary(1,df_values)
    
    
    