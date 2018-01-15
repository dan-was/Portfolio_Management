# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 18:28:07 2017

@author: Daniel
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import numpy as np
import seaborn as sns
sns.set()


def price_return_hist(returns, hist=True, kde=False, display_normal=True, bins=100):
    """For a given series of stock returns function generates a histogram or 
    kde plot or both with a corresponding normal distribution density function 
    for comparison.
    
    Parameters:
        hist: bool
            determines if histogram will be shown
        kde: bool
            determines if kernel density function will be shown
        display_normal: bool
            determines if normal disc density function will be shown
        n_bins: int
            how many bins to display on histogram"""
    # mean return
    mu = returns.mean()
    # standard deviation of return
    sigma = returns.std()
    if hist:
        # create histogram of returns 
        [n,bins_intervals,patches] = plt.hist(returns, bins, normed=True)
        if display_normal:
            # create plot of normal dist density function with returns' mean and sd
            x = mlab.normpdf(bins_intervals, mu, sigma)
            plt.plot(bins_intervals, x, color='red', lw=2)
    elif display_normal:
        # create plot of normal dist density function with returns' mean and sd
        bins_intervals = np.linspace(returns.min(), returns.max(), bins)
        x = mlab.normpdf(bins_intervals, mu, sigma)
        plt.plot(bins_intervals, x, color='red', lw=2)
    plt.title("Return distribution from last {} sessions".format(len(returns)))
    plt.xlabel("Returns")
    plt.ylabel("Frequency")
    # create kernel density estimation plot
    if kde:
        sns.kdeplot(returns)
    plt.show()