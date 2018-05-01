
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
from data.formatting import chng_date

# price/fin_data functions:

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

def download_bankier_symbols(index=''):
    """Downloads and returns stock names available on bankier.pl. Can be used
    to downloads historical financial data"""
    # download page's content
    url = "http://www.bankier.pl/gielda/notowania/akcje?index={}".format(index)
    # send an http request
    req = requests.get(url)
    # format the response usnign BS
    soup = BeautifulSoup(req.content, "lxml")
    # extract te symbols from table
    data_table = soup.find("div", {"class": "boxContent"})
    if len(data_table) == 5:
        data_table = soup.find_all("div", {"class": "boxContent"})[1]
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
    try:
        # download page's content
        url = 'http://stooq.pl/q/d/l/?s={}&i=d'.format(symbol)
        # load prices from downloaded CSV file using pandas
        px_series = pd.read_csv(url)
        if len(px_series.columns) == 6:
            # setup column names
            px_series.columns = ["date", "open", "high", "low", "close", "volume"]
            # setup quote date as index of the DataFrame
            px_series.set_index("date", inplace = True)
            # change the index format to pythons datetime format
            px_series.index = pd.to_datetime(px_series.index, 
                                             yearfirst = True)
            return px_series
        else:
            return []
    except (pd.io.common.EmptyDataError, ConnectionAbortedError):
        return []

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
    # set index name
    fin_data_df.index.rename('date', inplace=True)
    # return the dataframe with downloaded and transformed financial data
    return fin_data_df

def download_last_40_prices(symbol):
    """Downloads up to 40 most recent daily OHLC prices. Requires stooq symbol
    As opposed to downloading all historical prices from csv file there is no
    limit for this as it's scraing data from the website"""
    ## create request url
    url = "https://stooq.pl/q/d/?s={}".format(symbol)
    # send http request
    req = requests.get(url)
    # format the response usnign BS
    soup = BeautifulSoup(req.content, "lxml")
    # find the rable that contains price data
    price_table = soup.find("table", {"class": "fth1"})
    # find table body
    try:
        table_body = price_table.find('tbody')
    except AttributeError:
        return None
    # find all rows in table
    rows = table_body.find_all('tr')
    # create empty list to store data    
    data = []
    for row in rows:
        # find all elements (columns) in a row
        cols = row.find_all('td')
        # create a list of found elements
        cols = [element.text.strip() for element in cols]
        # append a row (vector) of data to data list
        data.append([element for element in cols])
    # create an empty list for formatted data
    formatted_data = []
    for row in data:
        try:
            # change date format
            date = chng_date(row[1])
            # change prices and volume to floats
            open_ = float(row[2])
            high = float(row[3])
            low = float(row[4])
            close = float(row[5])
            volume = float(row[6].replace(",", ""))
            # create a vector of formatted prices
            px_vector = [date, open_, high, low, close, volume]
            # append vector to formatted data list
            formatted_data.append(px_vector)
        except:
            continue
    # check if list contains any entries, display a warning if less than 40
    # don't return anyhing if list is empty
    if len(formatted_data) >= 1:   
        if len(formatted_data) < 40:
            print('{} less than 40 entries!'.format(symbol))
        df_new = pd.DataFrame(formatted_data)
        # set column names
        df_new.columns = ["date", "open", "high", "low", "close", "volume"]
        # set date as index
        df_new.set_index("date", inplace = True)
        # transform date to datetime object
        df_new.index = pd.to_datetime(df_new.index, yearfirst = True)
        return df_new
    else:
        print("{} invalid data nothing returned".format(symbol))

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
        df_new = pd.DataFrame([px_row], columns =["date", "open", "high", 
                                                  "low", "close", "volume"])
        # set date as index
        df_new.set_index("date", inplace = True)
        # change index's type to datetime
        df_new.index = pd.to_datetime(df_new.index, yearfirst = True)
        return df_new
    except:
        print("Unspecifier error - check yor internet connection or if ticker is valid")

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
    # find articles header
    article_header_raw = soup.find_all("h1", {"class": "entry-title"})[0]
    # extract header text
    article_header = article_header_raw.text
    # find entry date of the article
    entry_date_raw = soup.find_all("time", {"class": "entry-date"})
    entry_date = entry_date_raw[0].getText()
    # return a list of entry date, article content and url
    return [entry_date, article_header, article_formatted, url]

def download_bankier_article_urls(symbol, n_pages='all'):
    """Downloads urls urls of all available articles related to a given stock"""
    if n_pages == "all":
        # determine how many pages of articles are available for the given symbol
        n_pages = find_last_news_page_bankier(symbol)
    # create an empty set to store article links
    article_links = set()
    # downloand article links from every page
    for page in range(1,n_pages+1):
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
    return list(article_links)


def download_bankier_articles(symbol, n_pages='all'):
    """For a given symbol downloads available articles related to company with
    a timestamp.
    
    Params:
        n_pages: int or str: "all"
    """
    article_links = download_bankier_article_urls(symbol, n_pages)
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

# forum functions
    
def download_stockwatch_forum_symbols(n_pages = 15):
    """Downloads list of symbols and forum numbers from stockwatch forum. Data
    can be then used to download forum content"""
    # create empty list to store (froum_number, stock_symbol) pairs
    stockwatch_symbols_list = []
    # iterate through pages to find links and extract data
    for page in range(1,n_pages+1):
        # create url
        url = 'https://www.stockwatch.pl/forum/tematy-8p{}_Spolki-od-A-do-Z--GPW.aspx'.format(page)
        # send http request
        req = requests.get(url)
        # format the response usnign BS
        soup = BeautifulSoup(req.content, "lxml")
        # search for table that contains forum's content
        threads_table = soup.find("table", {"class": "threadList"})
        # find all rows (threads) on the downloaded page
        threads_raw_1 = threads_table.find_all("tr", {"class": "post"})
        threads_raw_2 = threads_table.find_all("tr", {"class": "post_alt"})
        # create an empty list to store thread urls 
        thread_urls = []
        for item in threads_raw_1:
            # append all found urls
            thread_urls.append([item['href'] for item in item.find_all("a")])
        for item in threads_raw_2:
            # append all found urls
            thread_urls.append([item['href'] for item in item.find_all("a")])
        # for each except the first one which is 'Forums policy'
        for item in thread_urls[1:]:
            # extract forum number and stock name/symbol from the link
            step1 = item[1].split("-")
            step2 = step1[1].split(".")
            step3 = step2[0].split("_")
            # append the number-symbol pair to the list
            stockwatch_symbols_list.append(step3)
    # create dictionary to store data
    stockwatch_symbols_dict = {}
    # assing forum number (value) to forum (stock) name (key)
    for item in stockwatch_symbols_list:
        stockwatch_symbols_dict[item[1]] = item[0]
    return stockwatch_symbols_dict

def download_stooq_forum_posts(stock, thread_num=-1, page_range='all'):
    """Function downloads posts from stockwatch forum related to given stock.
    if thread number is not given (default value -1) it will be found using
    "download_stockwatch_forum_symbols" function but it may take up to two
    minutes. Page_range argument may be left with default 'all' value. It will
    then download all available posts. Also, a tuple of (first,last) page can
    be given.
    Params:
        page_range: str, int, tuple:
            if str == 'all' - downloads all pages
            if tuple of ints == (first, last) - downloads pages from given range
    """
    # dictionray with numerical equivalents of Polish month names
    months = {'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
              'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
              'września': '09', 'października': '10', 'listopada': '11',
              'grudnia': '12'}
    # if the number thread number (included in forum's url) is not given it's
    # determined using download_stockwatch_forum_symbols function
    if thread_num == -1:
        symbol_dict = download_stockwatch_forum_symbols()
        thread_num = symbol_dict[stock]
    # if page range equals all set range big enough to include all pages
    if type(page_range) == str:
        if page_range == 'all':
            first_page = 1
            last_page = 1000
    # if a tuple of values (start,end) is given setup range accordingly
    elif type(page_range) == tuple:
        first_page = page_range[0]
        last_page = page_range[1]
    # setup a condition to stop returning the values twice
    completed = False
    # empty list to store posts before formatting
    all_posts_raw = []
    # empty list to store formatted post
    all_posts = []
    for page in range(first_page,last_page+1):
        # create url
        url = 'https://www.stockwatch.pl/forum/wpisy-{}p{}_{}.aspx'.format(thread_num,
                                                     page, stock)
        # send http request
        req = requests.get(url)
        # format the response usnign BS
        soup = BeautifulSoup(req.content, "lxml")
        # find posts content
        posts_content = soup.find_all("div", {"class": "postdiv"})
        # find posts send dates
        posts_dates = soup.find_all("div", {"class": "l"})
        # create a list of posts
        posts_raw = [item.text.strip() for item in posts_content]
        # add each post to the list of already downloaded posts so that when a 
        # post appears for the second time the loop stops and returns the result
        for item in posts_raw:
            if item not in all_posts_raw:
                all_posts_raw.append(item)
            else:
                return all_posts
                completed = True
                break
        # create a list of dates
        dates_raw = [item.text[57:90].strip() for item in posts_dates
                     if item.text[57:90] != ""]
        # split the elements in each date
        dates_split = [item.split() for item in dates_raw]
        # create a list of lists of day-month-year withc month changed to num
        dates_num = [[item[0], months[item[1]], item[2]] for item in dates_split]
        # join the elements of each date into one string
        dates_str = ['-'.join(item) for item in dates_num]
        # empty list for dates with hours
        dates_hr_raw = []
        # for each element in dates split create a list of [date, hour]
        for i in range(len(dates_split)):
            dates_hr_raw.append([dates_str[i], dates_split[i][3]])
        # join the date and hour into one string with a space
        dates_hr = [" ".join(item) for item in dates_hr_raw]
        # convert to datetime object
        dates_datetime = [pd.to_datetime(item, dayfirst=True) for item in dates_hr]
        # create [date, post] pairs
        posts_list = list(zip(dates_datetime, posts_raw))
        # add all new posts to all_posts list
        for item in posts_list:
            if item not in all_posts:
                all_posts.append(item)
    # return the list of posts if it hasn't been done before
    if not completed:
        return all_posts
