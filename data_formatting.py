# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 13:25:52 2017

@author: Daniel
"""

def chng_date(date, simple = False):
    months = {"sty": "01", "lut": "02", "mar": "03", "kwi": "04", "maj": "05", 
          "cze": "06", "lip": "07", "sie": "08", "wrz": "09", "paź": "09",
          "lis": "10", "gru": "12"}
    
    if not simple:
        date = date.split()
        date[1] = date[1].replace(date[1], months[date[1]])
        date = [date[2], date[1], date[0]]
        date = '-'.join(date)
        return date
    else:
        return months[date]