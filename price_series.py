# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 00:47:33 2017

@author: Daniel
"""

import numpy as np
import pandas as pd
from data.gathering import download_historical_prices, download_stooq_symbols
from data.gathering import download_last_40_prices, download_last_price
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
    def update_prices(cls, only_last = True, date_offset = 0):
        """When only_last is True method downloads only the last price of all
        available symbols. If set to False downloads up to 40 but can be limited
        to ~150 downloads per day. Offset used to set last business day and 
        prevent downloading data multiple times on weekends/non-trading days
        For example for Saturday: date_offset=-1 (if Friday was a trading day"""
        # list of stock symbols avaliable
        symbols = download_stooq_symbols()
        # find the last business/trading day
        last_b_day = pd.datetime.today()+pd.Timedelta(days=date_offset)
        # last business day tuple
        bd_tuple = (last_b_day.day, last_b_day.month, last_b_day.year)
        # boolean value that stops the loop when set to false
        keep_iterating = True
        # number of prices failed to update
        error_count = 0
        for symbol in symbols:
            if keep_iterating:
                # load price data from database by initializing price series object
                px = cls(symbol[0])
                # find last price observation day in the database
                last_date = px.data.tail(1).index[0]
                # if last observation date is different than last business day
                # some data is missing so try to download it
                if (last_date.day, last_date.month, last_date.year) != bd_tuple:
                    try:
                        # by default download only last price
                        if only_last:
                            new_data = download_last_price(symbol[0])
                        # if last try downloading last 40 (subject to daily limits)
                        else:
                            new_data = download_last_40_prices(symbol[0])
                        # count new rows added to the database
                        rows_updated = 0
                        # for each downloaded date
                        for date in new_data.index:
                            # if it's not already in the database then add it
                            if date not in px.data.index:
                                row = new_data.xs(date)
                                px.data = px.data.append(row)
                                rows_updated += 1
                        # save updated price series to the database
                        px.save_data_to_db(silent=True)
                        print('{} - added {} new prices'.format(symbol[0], rows_updated))
                    except:
                        print("Error with {}".format(symbol[0]))
                        error_count += 1
                        if error_count > 5:
                            keep_iterating = False
                        continue
                else:
                    print('{} already up to date'.format(symbol[0]))
        
    def save_data_to_db(self, silent=False):
        columns = ['open', 'high', 'low', 'close', 'volume']
        save_price_data_to_db(self.symbol, self.data[columns], silent)
        
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
    
        
    
