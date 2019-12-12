# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 09:58:49 2019
upload PL for all the tested pd wafers
@author: dding
"""

from dbConn import dbConn
from pdPL import pdPL

dbPD = dbConn('pd','test','pd')
dbEpi = dbConn('epi','epi','epi')

lot = 'SJC_ENG_432_4-GaAs'
_,wafer_lst,_=dbEpi.getWaferList_byLotName(lot)
pl = pdPL(wafer_lst)
pl.process()