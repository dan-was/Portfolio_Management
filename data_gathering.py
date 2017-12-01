# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 22:55:53 2017

@author: Daniel

Functions created to gather financial data
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd

def download_stooq_symbols(n_pages = 10):
    """The function downloads and returns symbols(tickers) and anmes of Polish
    stocks from stooq.pl, which can be used to acquire historical stock prices
    100 tickers are contained on one page, there are 9 ticker pages at the time
    this code is being writtnen. Can be adjusted upwards if necessary. The 
    list returned is checked for any duplciates so n_pages can be larger than
    actually required"""
    # specify a range of pages to be check for tickers
    pages = range(1,n_pages)
    # create an empty list of to store the downloaded tickers
    symbol_list = []
    for page in pages:
        # download page's content
        url = "https://stooq.pl/t/?i=513&v=0&l={}".format(page)
        # send an http request
        req = requests.get(url)
        # format the response usnign BS
        soup = BeautifulSoup(req.content, "lxml")
        # extract table that contains ticker codes
        price_table = soup.find("table", {"class": "fth1"})
        table_body = price_table.find('tbody')
        # extract rows from the table
        rows = table_body.find_all('tr')
        # for each row extract the ticker name
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            symbol_list.append([ele for ele in cols if ele][:2])
    # add index symbols to the list
    symbol_list.append(['WIG', 'WIG'])
    symbol_list.append(['WIG20', 'WIG20'])
    # return a list of symbols and names (names will be then used to match with
    # financial data sources)
    return symbol_list

def download_bankier_symbols():
    """Downloads and returns stock names available on bankier.pl. Can be used
    to downloads historical financial data"""
    # download page's content
    url = "http://www.bankier.pl/gielda/notowania/akcje?index="
    # send an http request
    req = requests.get(url)
    # format the response usnign BS
    soup = BeautifulSoup(req.content, "lxml")
    # extract te symbols from table
    data_table = soup.find("div", {"class": "boxContent"})
    table_body = data_table.find('tbody')
    rows = table_body.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])
    # create a list of symbols
    symbols = [item[0] for item in data if len(item)>0]
    # return the list of symbols
    return symbols
    
def download_historical_prices(symbol):
    """The function downloads all historical (daily) prices for a given symbol. 
    The data is acquired from stooq.pl and stooq codes should be used. There 
    is a limit of 50 downloads per day which requires IP change when reached."""
    # download page's content
    url = 'http://stooq.pl/q/d/l/?s={}&i=d'.format(symbol)
    # load prices from downloaded CSV file using pandas
    px_series = pd.read_csv(url)
    # setup column names
    px_series.columns = ["date", "open", "high", "low", "close", "volume"]
    # setup quote date as index of the DataFrame
    px_series.set_index("date", inplace = True)
    # change the index format to pythons datetime format
    px_series.index = pd.to_datetime(px_series.index, 
                                     yearfirst = True)
    return px_series


    



























