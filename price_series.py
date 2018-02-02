# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 00:47:33 2017

@author: Daniel
"""

import numpy as np
from data.gathering import download_historical_prices, download_stooq_symbols
from data.gathering import download_last_40_prices
from data.storage import save_price_data_to_db, read_price_data_from_db

class PriceSeries():
    """Class that stores historical price data with various methods required
    in price analysis"""
    
    def __init__(self, stooq_symbol):
        self.symbol = stooq_symbol
        try:
            self.data = read_price_data_from_db(self.symbol)
        except:
            self.data = download_historical_prices(self.symbol)
            self.save_data_to_db()
        self.add_returns()
        
    def download_prices_of_all_stocks(self):
        """Downloads list of stock symbols from stooq.pl and iterates through
        it to download historical OHLCV data and store it in a database file.
        Can be run few times on the same instance to continue downloading after
        the limit of downloads is reached. Method is relatively slow and can be
        used to recreate the full price database from scratch"""
        # check if downloaded and symbols lists are already initialized
        if not hasattr(self, 'downloaded'):
            self.downloaded = []
        if not hasattr(self, 'symbols'):
            # download available stooq symbols
            self.symbols = download_stooq_symbols()
        # set a variable that stops iterations when limit is reached
        limit = False
        for symbol in self.symbols:
            if not limit:
                if symbol[0] not in self.downloaded:
                    try:
                        # download prices
                        data = download_historical_prices(symbol[0])
                        # check if response contains any data
                        if len(data)>0:    
                            # save to db, print status and add to downloaded list
                            save_price_data_to_db(symbol[0], data)
                            print("Downloaded {}".format(symbol))
                            self.downloaded.append(symbol[0])
                        else:
                            # stop downloading when limit is reached
                            print("Limit reached, change IP and run again")
                            limit = True
                            break
                    except:
                        continue
    @classmethod
    def update_prices(cls):
        """Method downloads up to 40 most recent prices (OHLCV) and updates the
        database. Can be subject to limits (around 150 site launches per day)"""
        symbols = download_stooq_symbols()
        for symbol in symbols:
            px = cls(symbol[0])
            try:
                new_data = download_last_40_prices(symbol[0])
                rows_updated = 0
                for date in new_data.index:
                    if date not in px.data.index:
                        row = new_data.xs(date)
                        px.data.append(row)
                        rows_updated += 1
                px.save_data_to_db()
            except:
                print("Error with {}".format(symbol[0]))
                continue
            print('{} - added {} new prices'.format(symbol[0], rows_updated))
        
    def save_data_to_db(self):
        columns = ['open', 'high', 'low', 'close', 'volume']
        save_price_data_to_db(self.symbol, self.data[columns])
        
    def add_returns(self):
        self.data['log_return'] = np.log(self.data['close'].pct_change()+1)
        self.data.dropna()
        
    def add_monthly_returns(self):
        self.monthly_returns = self.data['log_return'].resample('MS').sum()

    def add_rolling_avg(self, column='close', window=21):
        col_name = 'r_avg_{}_{}'.format(column, window)
        self.data[col_name] = self.data[column].rolling(window).mean()
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
    
        
    
