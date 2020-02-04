# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 16:32:25 2019 for TLM
Revised on 12/12/2019 include detector IV and CV
Move overlapped methods to parent class in DI
Detector IV data uploading
@author: dding
"""

from DataInterface import DataInterface as DI

if __name__ == "__main__":
    path = '//sjt-fs00/MaterialsTeam/ALL/Characterization Data/DetectorIV'
    folders = {'root':path,\
               'uploaded':path+'/'+'0_Uploaded',\
               'failed':path+'/'+'0_Upload_Failed',\
               'old':path+'/'+'0_Old'}
    test = DI.getInterface('detector',folders)
    test.process_files()
    
