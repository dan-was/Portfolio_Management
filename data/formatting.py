# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 13:25:52 2017

@author: Daniel
"""

def chng_date(date, simple = False):
    months = {"sty": "01", "lut": "02", "mar": "03", "kwi": "04", "maj": "05", 
          "cze": "06", "lip": "07", "sie": "08", "wrz": "09", "paź": "10",
          "lis": "11", "gru": "12"}
    
    if not simple:
        date = date.split()
        date[1] = date[1].replace(date[1], months[date[1]])
        date = [date[2], date[1], date[0]]
        date = '-'.join(date)
        return date
    else:
        return months[date]
    
def slice_dataframe(df, window):
    """Returns a list of slices of a dataframe (rolling window of all columns)"""
    dfs = []
    i = window
    if window < len(df):
        while i<=len(df):
            start = i-window
            end = i
            # from start to end and all columns
            df_slice = df.iloc[start:end,:]
            dfs.append(df_slice)
            i+=1
    return dfs
            
    