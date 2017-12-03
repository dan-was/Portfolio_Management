# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 22:55:53 2017

@author: Daniel

Functions created to gather financial data
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from data_formatting import chng_date

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

def download_historical_fin_data(symbol, period ='ann'):
    """Downloads financial results/data from bankier.pl. Can be set to annual (default)
    or quartetly ('q') period. Returns a dataframe with all acquired fin data"""
    
    def replace_multi(text, dic):
        """Function replaces multiple substrings in text"""
        for i, j in dic.items():
            text = text.replace(i, j)
        return text
    
    # dict with polish anmes for the period types (to be included in the url)
    period_dict = {"ann": "roczny", "q": "kwartalny"}
    def find_last_page_number(symbol, period):
        """Determines how many pages of financial data there are"""
        # create an url to the first page of fin data of given company
        url = "http://www.bankier.pl/gielda/notowania/akcje/{}/wyniki-finansowe/skonsolidowany/{}/standardowy/1".format(symbol, period_dict[period])
        # send an http request
        req = requests.get(url)
        # format the response usnign BS
        soup = BeautifulSoup(req.content, "lxml")
        # find all page numbers
        pg = soup.find_all("a", {"class": "numeral btn "})
        if len(pg) > 0:
            pages = [item.text for item in pg]
            # last item in the list is the last page
            last_page = int(pages[-1])
        else:
            last_page = 1
        return last_page
    
    # check the last page of fin data
    last_page = find_last_page_number(symbol, period)
    # dictionary where the downloaded data will be stored
    fin_data = {}
    # separate dictionaries for downloaded quantities
    fin_data["Net revenues"] = {}
    fin_data["Operational profit (loss)"] = {}
    fin_data["Gross profit (loss)"] = {}
    fin_data["Net profit (loss)"] = {}
    fin_data["Depreciation"] = {}
    fin_data["EBITDA"] = {}
    fin_data["Assets"] = {}
    fin_data["Equity"] = {}
    fin_data["# of shares"] = {}
    fin_data["Profit/share"] = {}
    fin_data["Book value / share"] = {}
    # loop through all pages and check one more
    for site in range(1,last_page+1):
        # create an url that points to the fin data
        url = "http://www.bankier.pl/gielda/notowania/akcje/{}/wyniki-finansowe/skonsolidowany/{}/standardowy/{}".format(symbol,period_dict[period], site)
        # send an http request    
        req = requests.get(url)
        # format the response usnign BS
        soup = BeautifulSoup(req.content, "lxml")
        # find the table that contains fin data        
        data_table = soup.find("div", {"class": "box615 boxBlue boxTable left"})
        # find period names (either year or year and quarter) names in the table
        table_head = data_table.find('thead')
        periods = [s.strip() for s in table_head.text.splitlines()][3:-1]
        # for quarterly data add quarter number after the year
        if period == "q":
            periods = [item.split() for item in periods]
            periods = ["-".join([item[2], item[0]]) for item in periods if len(item) == 3]
        # find table body
        table_body = data_table.find('tbody')
        # fin all rows in the fin data table
        rows = table_body.find_all('tr')
        # create an empty list to store data from every row
        data = []
        # extract the data from rows
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
        # remove unwanted period
        data = data[:11]
        # dictionary with strings to be replaced to transform text-currency
        # data into flot
        d = {u'\xa0': u'', " ": "", ",": ".", " ": ""}
        # add each period's data to the dicts
        for i, q in enumerate(periods):
            fin_data["Net revenues"][q] = float(replace_multi(data[0][i+1], d))
            fin_data["Operational profit (loss)"][q] = float(replace_multi(data[1][i+1], d))
            fin_data["Gross profit (loss)"][q] = float(replace_multi(data[2][i+1], d))
            fin_data["Net profit (loss)"][q] = float(replace_multi(data[3][i+1], d))
            fin_data["Depreciation"][q] = float(replace_multi(data[4][i+1], d))
            fin_data["EBITDA"][q] = float(replace_multi(data[5][i+1], d))
            fin_data["Assets"][q] = float(replace_multi(data[6][i+1], d))
            fin_data["Equity"][q] = float(replace_multi(data[7][i+1], d))
            fin_data["# of shares"][q] = float(replace_multi(data[8][i+1], d))
            fin_data["Profit/share"][q] = float(replace_multi(data[9][i+1], d))
            fin_data["Book value / share"][q] = float(replace_multi(data[10][i+1], d))
        # transfrom the dict of dicts into a data frame
        fin_data_df = pd.DataFrame()
        for k, v in fin_data.items():
            fin_data_df[k] = pd.Series(fin_data[k])
    # return the dataframe with downloaded and transformed financial data
    return fin_data_df

# article functions:

def find_last_news_page_bankier(symbol):
    """Returns last page of bankier's article news list"""
    url = "https://www.bankier.pl/gielda/notowania/akcje/{}/wiadomosci/1".format(symbol)
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "lxml")
    # search for class that contains page numbers after the separator
    pages = soup.find_all("a", {"class": "numeral btn"})
    # if nothing found it means there is no separator (less than 6 pages) and
    # antoher class (with a space at the end) has to be used
    if len(pages) == 0:
        pages = soup.find_all("a", {"class": "numeral btn "})
        # if nothing found again it means there's only one page
        if len(pages) == 0:
            return 1
        else:
            # if there are some matches return the last page found
            return int(pages[-1].text)
    # there is a page list separator return the last match
    else:
        return int(pages[-1].text)
    
def download_bankier_article(url):
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "lxml")
    # search for article content
    article_content_raw = soup.find_all("div", {"id": "articleContent"})[0]
    # find all paragraphs in the article body
    article_paragraphs = article_content_raw.find_all('p')
    # empty list to store found paragraphs
    paragraphs_list = []
    for item in article_paragraphs:
        paragraphs_list.append(item.getText())
    # transform list of paragrapsh into one string
    article = " ".join(paragraphs_list).strip()
    # remove all newlines and unnecessary spaces, leave plain text with single
    # spaces
    article_formatted = " ".join(article.split())
    # find entry date of the article
    entry_date_raw = soup.find_all("time", {"class": "entry-date"})
    entry_date = entry_date_raw[0].getText()
    # return a list of entry date, article content and url
    return [entry_date, article_formatted, url]

def download_all_bankier_articles(symbol):
    """For a given symbol downloads all available articles related to company
    with a timestamp"""
    # determine how many pages of articles are available for the given symbol
    n_pages = find_last_news_page_bankier(symbol)
    # create an empty set to store article links
    article_links = set()
    # downloand article links from every page
    for page in range(1,n_pages):
        # create request url
        url = "https://www.bankier.pl/gielda/notowania/akcje/{}/wiadomosci/{}".format(symbol,page)
        # send http request
        req = requests.get(url)
        # format the response usnign BS
        soup = BeautifulSoup(req.content, "lxml")
        # create a list of articles/links
        article_list = soup.find_all("span", {"class": "entry-title"})
        for item in article_list:
            try:
                for link in item.find_all('a'):
                    # append article link to the set
                   article_links.add('https://www.bankier.pl{}'.format(str(link.get("href"))))
            except:
                continue
    # transform links set to a list
    article_links = list(article_links)
    # create an empty list to store article's content
    articles = []
    n = 0 # counter
    for link in article_links:
        # download article and add it to the list
        articles.append(download_bankier_article(link))
        # increment number of downloaded and display progress
        n += 1
        print(n, "of", len(article_links), "downloaded")
    # filter out articles withour any content
    articles_filtered = [article for article in articles if article[1] != ""]
    # return list of downloaded articles
    return articles_filtered

def download_last_price(symbol):
    """Downloads the last session's OHLC prices and volume from stooq. Requires
    stooq symbol"""
    # create request url
    url = "https://stooq.pl/q/g/?s={}".format(symbol)
    # send http request
    req = requests.get(url)
    # format the response usnign BS
    soup = BeautifulSoup(req.content, "lxml")
    # find all tables in on the website
    site_tables = soup.find_all("table")
    # create a list of tables
    all_tables = []
    for item in site_tables:
        all_tables.append(item)
    # find table with price data
    for n, table in enumerate(all_tables):
        # if table starts with "Kurs" it contains the price data
        if table.text.startswith("Kurs"):
            pos = n
            break
    try:
        # select first and second tabel with price data
        data_tables = [all_tables[pos], all_tables[pos+1]]
        # create an empty list to store data from tables
        all_table_content = []
        # for each table of the two
        for table in data_tables:
            # find table body
            table_body = table.find('tbody')
            # find all rows in the table body
            rows = table_body.find_all('tr')
            # create an ampty list for data from tables
            data = []
            # for each row in row list
            for row in rows:
                # find row columns
                cols = row.find_all('td')
                # create a list of all elements of a row
                cols = [element.text.strip() for element in cols]
                data.append([element for element in cols])
            # add to all content
            all_table_content.append(data)
        # extract days and month - a list of two elements
        date = all_table_content[0][1][1].split(",")[0].split()
        # add current year as third element
        date.append(str(datetime.today().year))
        # change month name to number
        date = [date[2], chng_date(date[1], True), date[0]]
        # join with dashes as separators - create a date string
        date = "-".join(date)
        # find open price
        open_ = float(all_table_content[0][2][1])
        # find high pirce
        high = float(all_table_content[1][1][1].split("/")[0])
        # find low price
        low = float(all_table_content[1][1][1].split("/")[1])
        # find close price and transform format if necessary (if ccy units present)
        try:
            close = float(all_table_content[0][0][1])
        except ValueError:
            # remove ccy sing
            close = close = all_table_content[0][0][1].replace(u'\xa0', u' ')
            close = close.split(" ")
            close =  float(close[0])
        # find volume
        volume = float(all_table_content[0][4][1].replace(",", ""))
        # create a vector of data
        px_row = [date, open_, high, low, close, volume]
        # transform the vector to a dataframe
        df_new = pd.DataFrame([px_row], columns =["Date", "Open", "High", 
                                                  "Low", "Close", "Volume"])
        # set date as index
        df_new.set_index("Date", inplace = True)
        # change index's type to datetime
        df_new.index = pd.to_datetime(df_new.index, yearfirst = True)
        return df_new
    except:
        print("Unspecifier error - check yor internet connection or if ticker is valid")
