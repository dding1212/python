# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 22:57:46 2020

@author: dding
"""
from mask import eel_mask

if __name__ == "__main__":
    mask_name = "PH008"
    test = eel_mask(mask_name)
    devices,bars = test.get_mask_data()
    df1,df2,df3,df4 = test.arrange_mask_data(devices,bars)