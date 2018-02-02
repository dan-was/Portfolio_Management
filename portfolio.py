# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 18:23:28 2018

@author: Daniel
"""

from price_series import PriceSeries


class Portfolio():

    def __init__(self, name):
       self.name = name
       
    def add_position(position):
        pass
    
class Position():
    
    def __init__(self, security, trade_date, quantity, open_price):
        self.security = security
        self.trade_date = trade_date
        self.quantity = quantity
        self.open_price = open_price
        
class Security():
    
    def __init__(self):
        pass
        
class Stock(Security):
    
    def __init__(self, symbol):
        self.type = 'stock'
        self.price_series = PriceSeries(symbol)
        
class Bond(Security):
    
    def __init__(self, symbol):
        self.type = 'bond'
        self.price_series = PriceSeries(symbol)
    