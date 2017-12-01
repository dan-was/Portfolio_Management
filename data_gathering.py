# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 22:55:53 2017

@author: Daniel

Functions created to gather financial data
"""

import requests
from bs4 import BeautifulSoup

def download_symbols(n_pages = 10):
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
        # extract table  that contains ticker codes
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