# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 00:47:33 2017

@author: Daniel
"""

import numpy as np
import pandas as pd
from data_gathering import download_historical_prices

class PriceSeries():
    """Class that stores historical price data with various methods required
    in price analysis"""
    
    def __init__(self, stooq_symbol, OHLCV_df=None):
        self.symbol = stooq_symbol
        if isinstance(OHLCV_df, pd.core.frame.DataFrame):
            self.data = OHLCV_df.copy()
        else:
            self.data = download_historical_prices(self.symbol)
        self.add_returns()
        
    def add_returns(self):
        self.data['log_return'] = np.log(self.data['close'].pct_change()+1)
        self.data.dropna()
        
    def add_monthly_returns(self):
        self.monthly_returns = self.data['log_return'].resample('MS').sum()

    def add_rolling_avg(self, window=21):
        col_name = 'rolling_avg_{}'.format(window)
        self.data[col_name] = self.data['close'].rolling(window).mean()
        self.data.dropna()
    
    def add_rolling_std(self, window=21, annualized=True, weighted=False, decay='lin'):
        if not weighted:
            col_name = 'rolling_std_{}'.format(window)
            self.data[col_name] = self.data['log_return'].rolling(window).std()
        else:
            col_name = 'rolling_w_std_{}'.format(window)
            roll = self.data['log_return'].rolling(window)
            weights = np.linspace(0,1,window)/sum(np.linspace(0,1,window))
            self.data[col_name] = roll.apply(lambda x: (x*weights).std())
        if annualized:
            self.data[col_name] = self.data[col_name] * np.sqrt(252)
        self.data.dropna()
    
    def add_weighted_rolling_avg(self, window=21):
        pass
    
        
    
