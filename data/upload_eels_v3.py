# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 16:55:58 2020

@author: dding
"""

from DataInterface import DataInterface as DI

if __name__ == "__main__":
    path = '//sjt-fs00/MaterialsTeam/ALL/Characterization Data/EEL'
    folders = {'root':path,\
               'uploaded':path+'/'+'0_Uploaded',\
               'failed':path+'/'+'0_Upload_Failed',\
               'old':path+'/'+'0_Old'}
    test = DI.getInterface('laser',folders)
    test.process_files()
