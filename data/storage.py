# -*- coding: utf-8 -*-
"""
Created on Sat Jan 27 23:16:27 2018

@author: Daniel
"""

import sqlite3
import pandas as pd


def save_price_data_to_db(table, price_data, silent=False):
    conn = sqlite3.connect('C:/Users/Daniel/Desktop/PythonProjects/PortfolioManagement/price_data.db')
    price_data.to_sql('px_' + table, conn, if_exists='replace')
    if not silent:
        print("Price data saved in {} table".format(table))

def read_price_data_from_db(table):
    conn = sqlite3.connect('C:/Users/Daniel/Desktop/PythonProjects/PortfolioManagement/price_data.db')
    df = pd.read_sql_query("SELECT * FROM {}".format('px_' + table), conn, index_col='date', 
                             parse_dates={'date': '%Y-%m-%d %H:%M:%S'})    
    df.index.name = 'date'
    return df

def save_fin_data_to_db(table, fin_data, period, silent=False):
    conn = sqlite3.connect('C:/Users/Daniel/Desktop/PythonProjects/PortfolioManagement/fin_data.db')
    fin_data.to_sql('fin_{}_{}'.format(table, period), conn, if_exists='replace')
    if not silent:
        print("Financial data saved in {} table".format(table)) 

def read_fin_data_to_db(table, period):
    conn = sqlite3.connect('C:/Users/Daniel/Desktop/PythonProjects/PortfolioManagement/fin_data.db')
    return pd.read_sql_query('SELECT * FROM fin_{}_{}'.format(table, period), 
                             conn, index_col='date')
