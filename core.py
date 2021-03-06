# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 00:47:33 2017

@author: Daniel
"""


from data.gathering import download_historical_prices, download_stooq_symbols
from data.gathering import download_last_40_prices, download_last_price
from data.gathering import download_bankier_articles, download_bankier_article_urls
from data.gathering import download_bankier_article, download_bankier_symbols
from data.storage import save_price_data_to_db, read_price_data_from_db
from data.storage import save_articles_to_db, read_articles_from_db
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import seaborn as sns
sns.set()


class PriceSeries():
    """Class that stores historical price data with various methods required
    in price analysis. Data source is set to database 'db' by default but if
    data is not available can be changed to 'web' to download it from stooq"""

    def __init__(self, stooq_symbol, source = 'db'):
        self.symbol = stooq_symbol
        if source == 'db':
            self.data = read_price_data_from_db(self.symbol)
        elif source == 'web':
            self.data = download_historical_prices(self.symbol)
            self.save_data_to_db()
        self.add_returns()
        self.add_monthly_returns()
        
    def download_all_historical_prices_for_all_stocks(self):
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
            self.get_symbols()
        # set a variable that stops iterations when limit is reached
        limit = False
        for symbol in self.symbols:
            if not limit:
                if symbol[0] not in self.downloaded:
                    # download prices
                    data = download_historical_prices(symbol[0])
                    # check if response contains any data
                    if len(data) > 0:
                        # save to db, print status and add to downloaded list
                        save_price_data_to_db(symbol[0], data)
                        print("Downloaded {}".format(symbol))
                        self.downloaded.append(symbol[0])
                    else:
                        # stop downloading when limit is reached
                        print("Limit reached, change IP and run again")
                        limit = True
                        break
                    
    def get_symbols(self):
        self.symbols = download_stooq_symbols()
        print("{} symbols downloaded".format(len(self.symbols)))
    
    def update_prices(self, only_last=True, date_offset=0):
        """When only_last is True  downloads only the last price of the stock.
        If set to False downloads up to 40 but can be limited to ~150 downloads 
        per day. Offset used to set last business day and prevent downloading 
        data multiple times on weekends/non-trading days. 
        For example for Saturday: date_offset=-1 (if Friday was a trading day)"""
        # find the last business/trading day
        last_b_day = pd.datetime.today()+pd.Timedelta(days=date_offset)
        # last business day tuple
        bd_tuple = (last_b_day.day, last_b_day.month, last_b_day.year)
        # make sure the price data is sorted by date
        self.data = self.data.sort_index()
        # find last price observation day in the database
        last_date = self.data.tail(1).index[0]
        # if last observation date is different than last business day
        # some data is missing so try to downloading it
        if (last_date.day, last_date.month, last_date.year) != bd_tuple:
            # by default download only last price
            if only_last:
                new_data = download_last_price(self.symbol)
            # if not "only_last" try downloading last 40 (subject to daily limits)
            else:
                new_data = download_last_40_prices(self.symbol)
            # count new rows added to the database
            rows_updated = 0
            if new_data is not None:
                # for each downloaded date
                for date in new_data.index:
                    # if it's not already in the database then add it
                    if date not in self.data.index:
                        row = new_data.xs(date)
                        self.data = self.data.append(row)
                        rows_updated += 1
                # save updated price series to the database
                self.data = self.data.sort_index()
                self.add_returns()
                self.save_data_to_db(silent=True)
                print('{} - added {} new prices'.format(self.symbol, rows_updated))
            else:
                raise ValueError("{} - no data downloaded")
        else:
            self.downloaded.append(self.symbol)
            print('{} already up to date'.format(self.symbol))
            
    def update_prices_for_all_stocks(self, only_last=True, date_offset=0):
        """When only_last is True method downloads only the last price of all
        available symbols. If set to False downloads up to 40 but can be limited
        to ~150 downloads per day. Offset used to set last business day and
        prevent downloading data multiple times on weekends/non-trading days
        For example for Saturday: date_offset=-1 (if Friday was a trading day"""
        # check if downloaded and symbols lists are already initialized
        if not hasattr(self, 'downloaded'):
            self.downloaded = []
        if not hasattr(self, 'symbols'):
            # download available stooq symbols
            self.get_symbols()
        # boolean value that stops the loop when set to false
        keep_iterating = True
        # number of prices failed to update
        self.error_count = 0
        for symbol in self.symbols:
            if symbol[0] not in self.downloaded:
                if keep_iterating:
                    # load price data from database by initializing price series object
                    try:
                        px = PriceSeries(symbol[0])
                        px.update_prices(only_last=only_last, date_offset=date_offset)
                        self.downloaded.append(symbol[0])
                    except pd.io.sql.DatabaseError:
                        print('No data found for {}'.format(symbol[0]))
                    except ValueError:
                        self.error_count += 1
                    except (IndexError, AttributeError):
                        print("Issue with {}".format(symbol[0]))
                        continue
                    if self.error_count >= 5:
                        keep_iterating = False
                        print("Limit reached!")

    def save_data_to_db(self, silent=False):
        columns = ['open', 'high', 'low', 'close', 'volume']
        save_price_data_to_db(self.symbol, self.data[columns], silent)

    def add_returns(self):
        self.data['log_return'] = np.log(self.data['close'].pct_change()+1)
        self.data.fillna(0, inplace=True)

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
            weights = np.linspace(0, 1, window)/sum(np.linspace(0, 1, window))
            self.data[col_name] = roll.apply(lambda x: (x*weights).std())
        if annualized:
            self.data[col_name] = self.data[col_name] * np.sqrt(252)
        self.data.dropna()

    def add_weighted_rolling_avg(self, window=21):
        pass

    def summary(self):
        """Returns a summary of rolling statustics for price series"""
        summ = pd.Series()
        ret = self.data['log_return']
        #returns
        def roll_return(returns, window):
            if len(returns) >= window:
                return returns.tail(window).sum()
            else:
                return np.nan

        def roll_std(returns, window):
            if len(returns) >= window:
                return returns.tail(window).std()
            else:
                return np.nan
        #general
        summ.set_value('first_day', pd.to_datetime(str(ret.head(1).index.values[0])).strftime('%Y-%m-%d'))
        summ.set_value('last_day', pd.to_datetime(str(ret.tail(1).index.values[0])).strftime('%Y-%m-%d'))
        summ.set_value('n_observations', len(ret))
        #returns
        summ.set_value('ret_1d', ret.tail(1)[0])
        summ.set_value('ret_5d', roll_return(ret, 5))
        summ.set_value('ret_21d', roll_return(ret, 21))
        summ.set_value('ret_252d', roll_return(ret, 252))
        summ.set_value('ret_3y', roll_return(ret, 252*3))
        summ.set_value('ret_5y', roll_return(ret, 252*5))
        # standard dev
        summ.set_value('std_5d', roll_std(ret, 5))
        summ.set_value('std_21d', roll_std(ret, 21))
        summ.set_value('std_252d', roll_std(ret, 252))
        summ.set_value('std_252d_ann', roll_std(ret, 252)*np.sqrt(252))
        # risk/return profile
        summ.set_value('return/risk_1y', summ['ret_252d']/summ['std_252d_ann'])
        # tails and shape
        summ.set_value('skew_252d', ret.tail(252).skew())
        summ.set_value('skew_3y', ret.tail(252*3).skew())
        summ.set_value('kurt_252d', ret.tail(252).kurt())
        summ.set_value('kurt_3y', ret.tail(252*3).kurt())
        summ.set_value('max_1d_ret', max(ret))
        summ.set_value('min_1d_ret', min(ret))
        summ.set_value('n_above_3_sigma', len(ret[ret > (ret.mean()+3*ret.std())]))
        summ.set_value('n_below_3_sigma', len(ret[ret < (ret.mean()-3*ret.std())]))
        return summ

    def price_return_hist(self, n_months=12, hist=True, kde=False,
                          display_normal=True, bins=100):
        """For a given series of stock returns function generates a histogram or
        kde plot or both with a corresponding normal distribution density function
        for comparison.

        Parameters:
            n_months: int
                number of months of returns to plot
            hist: bool
                determines if histogram will be shown
            kde: bool
                determines if kernel density function will be shown
            display_normal: bool
                determines if normal disc density function will be shown
            n_bins: int
                how many bins to display on histogram"""

        returns = self.data['log_return'].tail(n_months*21)
        # mean return
        mu = returns.mean()
        # standard deviation of return
        sigma = returns.std()
        if hist:
            # create histogram of returns
            [n,bins_intervals,patches] = plt.hist(returns, bins, normed=True)
            if display_normal:
                # create plot of normal dist density function with returns' mean and sd
                norm_dist_points = mlab.normpdf(bins_intervals, mu, sigma)
                plt.plot(bins_intervals, norm_dist_points, color='red', lw=2)
        elif display_normal:
            # create plot of normal dist density function with returns' mean and sd
            bins_intervals = np.linspace(returns.min(), returns.max(), bins)
            x = mlab.normpdf(bins_intervals, mu, sigma)
            plt.plot(bins_intervals, x, color='red', lw=2)
        plt.title("Return distribution of {} from last {} sessions".format(self.symbol, len(returns)))
        plt.xlabel("Returns")
        plt.ylabel("Frequency")
        # create kernel density estimation plot
        if kde:
            sns.kdeplot(returns)
        plt.show()


class Articles():
    """Class that stores articles related to stocks and provides methods
    to analayze textual data"""
    
    def __init__(self, symbol):
        self.symbol = symbol
        try:
            self.load_data_from_db()
        except pd.io.sql.DatabaseError:
            self.download_all_articles()
            
    def save_data_to_db(self):
        """Saves all articles in a sqlite database file"""
        save_articles_to_db(self.data, self.symbol)
        
    def load_data_from_db(self):
        """Loads articles from sqlite db file"""
        self.data = read_articles_from_db(self.symbol)
        
    def download_all_articles(self):
        """Downloads all articles from bankier.pl related to a given stock symbol"""
        # download all available articles related to a given symbol
        article_list = download_bankier_articles(self.symbol)
        # transform downloaded data into a dataframe
        self.data = pd.DataFrame()
        self.data['date'] = [pd.to_datetime(article[0]) for article in article_list]
        self.data['header'] = [article[1].strip() for article in article_list]
        self.data['content'] = [article[2].strip() for article in article_list]
        self.data['url'] = [article[3].strip() for article in article_list]
        self.data['source'] = ['bankier' for item in article_list]
        self.data.set_index('date', inplace = True)
        self.data = self.data.sort_index(ascending=False)
        # save dataframe with articles to database file
        self.save_data_to_db()
        
    def update_articles(self):
        """Compares the list of downloaded articles with articles available
        and downloads the ones that are missing"""
        # download a list of urls of all available articles
        available_urls = download_bankier_article_urls(self.symbol)
        # create a list of all urls that already have been downloaded
        downloaded_urls = list(self.data['url'])
        counter = 0
        # download all articles that have not been downloaded yet
        for url in available_urls:
            if url not in downloaded_urls:
                try:
                    art = download_bankier_article(url)
                    art.append('bankier')
                    art_df = pd.DataFrame([art], columns =["date", "header", "content", 
                                                      "url", "source"])
                    art_df.set_index("date", inplace = True)
                    art_df.index = pd.to_datetime(art_df.index, yearfirst = True)
                    self.data.append(art_df)
                    counter += 1
                except:
                    continue
        print("{} - downloaded {} new articles".format(self.symbol, counter))
        # if any new articles were downloaded sort the dataset and save it in db
        if counter > 0:
            self.data = self.data.sort_index(ascending=False)
            self.save_data_to_db()
            
    @classmethod
    def update_articles_for_all_stocks(cls):
        """Runs 'update_articles' method on all available symbols"""
        symbols = download_bankier_symbols()
        for symbol in symbols:
            try:
                test = cls(symbol)
                test.update_articles()
            except:
                continue
            

class PortfolioOptimizer():
    """Class that allows to construct and optimize a portfolio of stocks"""

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "Portfolio {} of: {}".format(self.name, ", ".join(self.symbols))

    def add_stocks(self, stock_symbols, weights=None):
        self.symbols = stock_symbols
        self.stocks = [PriceSeries(symbol) for symbol in self.symbols]
        self.returns = pd.DataFrame()
        for stock in self.stocks:
            self.returns[stock.symbol] = stock.data['log_return']
        self.monthly_returns = pd.DataFrame()
        for stock in self.stocks:
            self.monthly_returns[stock.symbol] = stock.monthly_returns
        self.weights = weights

    def set_weights(self, weights):
        """Provide weights of stocks in a form of a matrix/array
        Format:
            array([ 0.03295459,  0.11429331,  0.02292454,  0.22071245])
        """
        self.weights = weights

    def summary(self):
        summ = pd.DataFrame()
        for stock in self.stocks:
            summ[stock.symbol] = stock.summary()
        return summ

    def correlation(self, months=36, plot=False):
        price_matrix = pd.DataFrame()
        for stock in self.stocks:
            stock.add_monthly_returns()
            price_matrix[stock.symbol] = stock.monthly_returns.tail(months)
        if plot:
            sns.heatmap(price_matrix.corr())
        return price_matrix.corr()

    def generate_rand_portfolios(self, n_portfolios = 10000, months_of_data = 36,
                                 plot=True, weights=False, figsize=(12,6)):
        """Function takes a list of stock symbols as input and generates a given number
        of randomly weighted protfolios based of monthly price data from a given period.
        Function displays a scatterplot of generated portfolios' risk/return characteristics
        as well as points the portfolio with the best expected return to expected risk ratio"""
        # Merge monthly returns from the given period in one data frame
        df = pd.DataFrame()
        for price_series in self.stocks:
            df[price_series.symbol] = price_series.monthly_returns.tail(months_of_data)

        def random_weights(n):
            """Function returns an array of randomly generated weights that sum up to 1"""
            nums = np.random.rand(n)
            return nums/sum(nums)

        def create_rand_portfolio(df):
            """Function calculates mean return and standard deviation of a randomly
            generated portfolio of given stocks"""
            e_r = np.asmatrix(np.mean(df.T, axis=1))          # average returns
            w = np.asmatrix(random_weights(df.T.shape[0]))    # randomly generated weights
            C = np.asmatrix(np.cov(np.array(df).T))           # variance-covariance matrix
            # calculate the dot product of expected weights and mean returns
            # expected portfolio return:
            mu = np.dot(w, e_r.T)*12
            # clalculate the standard deviation of the portfolio
            std = np.sqrt(w * C * w.T)*np.sqrt(12)
            # rerun the random generation if the risk is too high to refuce outliers
            if std > 0.6:
                return create_rand_portfolio(df)
            #return the expected return, standard deviation and transposed vector of weights
            return mu, std, w.T

        # generate given number of random portfolios
        means, stds, w = np.column_stack([create_rand_portfolio(df) for _ in range(n_portfolios)])
        # create a list of return/risk ratios for all portfolios
        rr = []
        for m, s, w in zip(means, stds, w):
            rr.append([m/s, w, m, s])
        # find the portfolio with the best return/risk ratio
        best = max(rr, key=lambda item : item[0])
        if plot:
            # use the return/risk ratio to determine data points colors on the plot
            colors = [np.asscalar(item[0]) for item in rr]
            #create a scatterplot
            plt.figure(figsize=figsize)
            plt.scatter(stds, means, alpha = 0.5, c = colors)
            plt.xlabel('Annualized standard deviation')
            plt.ylabel('Expected annual return')
            # add a side bar with return/risk levels legend
            cbar = plt.colorbar()
            cbar.ax.get_yaxis().labelpad = 15
            cbar.ax.set_ylabel('E(r)/std', rotation=270)

            def desc(ex, weights, symbols):
                """Function creates a description of the expected return and weights of a
                portfolio"""
                string = "E(r): {}%, Portfolio: ".format("%.2f" % (np.asscalar(ex)*100))
                for n, w in enumerate(weights):
                    string += "{}: {}%, ".format(symbols[n], "%.2f" % (np.asscalar(w)*100))
                return string

            # create a description of the most efficient portfolio
            description = desc(best[2], best[1], df.columns)
            # add a pointer with description to the chart
            plt.annotate(description, xy=(best[3], best[2]), size = 10, xytext=(best[3]-0.12, best[2]+0.11),
                         arrowprops=dict(facecolor='grey', shrink=0.05),)
            # add a chart title
            plt.title('Mean and standard deviation of returns of {} randomly generated portfolios'.format(n_portfolios))
            plt.show()
        # return weights of the best portfolio
        if weights:
            return best[1]

    def plot_returns(self, window=252, figsize=(12,6)):
        """Plots cumulative return of all stocks in the porflolio for a given time
        windo"""
        data = self.returns.tail(window).cumsum()
        t = 'Cumulative log return from last {} days'.format(window)
        data.plot(title=t, figsize=figsize)
        plt.show()

    def plot_indiv_roll_std(self, window=252, figsize=(12,6)):
        """Displays a plot of trailing annualized standard deviation of individual
        stocks for data available for all stocks"""
        data = self.returns.dropna()
        roll_std = data.rolling(252).std()*np.sqrt(252)
        t = "Rolling annualized standard deviation od individual stocks, window = {}".format(window)
        roll_std.plot(title=t, figsize=figsize)

    def plot_portfolio_trailing_risk(self, months_of_data=24, figsize=(12,6)):
        # Merge monthly returns from the given period in one data frame
        data = self.monthly_returns.dropna()

        def portfolio_return(row):
            return (row*self.weights).sum()

        #return data.rolling(window=1, axis=1).apply(lambda x: f1(x))
        self.portfolio_returns = pd.DataFrame()
        self.portfolio_returns['Portfolio return'] = data.apply(lambda x: portfolio_return(x), axis=1)
        self.portfolio_returns['Expected ann return'] = self.portfolio_returns.rolling(months_of_data).mean()*12
        self.portfolio_returns['Rolling std ann'] = self.portfolio_returns['Portfolio return'].rolling(months_of_data).std()*np.sqrt(12)
        t = "Annualized trailing risk (std) of the portfolio, window = {} months".format(months_of_data)
        self.portfolio_returns.plot(title=t, figsize=figsize)


    def plot_portfolio_trailing_risk2(self, months_of_data=24, figsize=(12,6)):
        """Does almost the same thing as the previous function but using standard approach
        with variance-covariance matrix and matrix calculations. Returns only MA portfolio return
        (expected return) and not actual return as the first method does"""
        from data.formatting import slice_dataframe

        def create_portfolio(df):
            """Function calculates mean return and standard deviation of a randomly
            generated portfolio of given stocks"""
            e_r = np.asmatrix(np.mean(df.T, axis=1))         # average returns
            w = np.asmatrix(self.weights)    # randomly generated weights
            C = np.asmatrix(np.cov(np.array(df).T))           # variance-covariance matrix
            # calculate the dot product of expected weights and mean returns
            # expected portfolio return:
            mu = np.dot(w, e_r.T)
            # clalculate the standard deviation of the portfolio
            std = np.sqrt(w * C * w.T)*np.sqrt(12)
            #return the expected return, standard deviation and transposed vector of weights
            return (np.asscalar(mu), np.asscalar(std))

        data = self.monthly_returns.dropna()
        slices = slice_dataframe(data, months_of_data)
        return_risk_list = []
        for slc in slices:
            return_risk_list.append(create_portfolio(slc))

        returns = [item[0] for item in return_risk_list]
        std = [item[1] for item in return_risk_list]
        last_index_first_slice = slices[0].iloc[len(slices[0])-1].name
        self.portfolio_returns = pd.DataFrame(index=data.loc[last_index_first_slice:].index)
        self.portfolio_returns['Expected ann return'] = returns
        self.portfolio_returns['Expected ann return'] = self.portfolio_returns['Expected ann return']*12
        self.portfolio_returns['Rolling std ann'] = std
        t = "Annualized trailing risk (std) of the portfolio, window = {} months".format(months_of_data)
        self.portfolio_returns.plot(title=t, figsize=figsize)


if __name__ == '__main__':

    stocks = ['CDR', 'PZU', 'CCC', '11B']
    test = PriceSeries('11B')
#    test.download_all_historical_prices_for_all_stocks()
#    test.update_prices_for_all_stocks()
#    test = Articles("CDPROJEKT")
#    test.update_articles_for_all_stocks()
#    test.download_prices_of_all_stocks()
#    test = PortfolioOptimizer('test')
#    test.add_stocks(stocks)
#    test.set_weights(np.array([0.50,  0.1,  0.1,  0.2, 0.1]))
#    test.plot_returns()
#    print(test.summary())
#    x = test.correlation(plot=True)
    #print(x)
#    print(test.generate_rand_portfolios(plot=True, weights=False))
#    test.plot_indiv_roll_std()
#    test.plot_portfolio_trailing_risk()
#    test.plot_portfolio_trailing_risk2()
