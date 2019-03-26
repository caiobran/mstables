import pandas as pd
import numpy as np
import sqlite3
import json
import sys
import re
import os


class DataFrames():

    dbfile = 'db/mstables.sqlite' # Standard db file name

    def __init__(self, file = dbfile):
        msg = 'Creating intial DataFrames from file {}...'
        print(msg.format(file))

        # SQLite connection
        self.file = file
        self.conn = sqlite3.connect(file)
        self.cur = self.conn.cursor()

        # Row Headers
        colheaders = table(self.cur, 'ColHeaders', True)
        self.colheaders = colheaders.set_index('id')

        # Dates and time references
        timerefs = table(self.cur, 'TimeRefs', True)
        self.timerefs = timerefs.set_index('id').replace(['', '—'], None)

        # Reference tables
        self.urls = table(self.cur, 'URLs', True)
        self.securitytypes = table(self.cur, 'SecurityTypes', True)
        self.tickers = table(self.cur, 'Tickers', True)
        self.sectors = table(self.cur, 'Sectors', True)
        self.industries = table(self.cur, 'Industries', True)
        self.styles = table(self.cur, 'StockStyles', True)
        self.exchanges = table(self.cur, 'Exchanges', True)
        self.countries = table(self.cur, 'Countries', True)
        self.companies = table(self.cur, 'Companies', True)
        self.currencies = table(self.cur, 'Currencies', True)
        self.stocktypes = table(self.cur, 'StockTypes', True)
        #self.fetchedurls = table(self.cur, 'Fetched_urls', True)

        # Master table
        self.master = table(self.cur, 'Master', True)

        print('Initial DataFrames created.')


    def add_yr_cols(self, df):
        return (df
         .merge(self.timerefs, left_on='Y0', right_on='id')
         .drop('Y0', axis=1).rename(columns={'dates':'Y0'})
         .merge(self.timerefs, left_on='Y1', right_on='id')
         .drop('Y1', axis=1).rename(columns={'dates':'Y1'})
         .merge(self.timerefs, left_on='Y2', right_on='id')
         .drop('Y2', axis=1).rename(columns={'dates':'Y2'})
         .merge(self.timerefs, left_on='Y3', right_on='id')
         .drop('Y3', axis=1).rename(columns={'dates':'Y3'})
         .merge(self.timerefs, left_on='Y4', right_on='id')
         .drop('Y4', axis=1).rename(columns={'dates':'Y4'})
         .merge(self.timerefs, left_on='Y5', right_on='id')
         .drop('Y5', axis=1).rename(columns={'dates':'Y5'})
         .merge(self.timerefs, left_on='Y6', right_on='id')
         .drop('Y6', axis=1).rename(columns={'dates':'Y6'})
         .merge(self.timerefs, left_on='Y7', right_on='id')
         .drop('Y7', axis=1).rename(columns={'dates':'Y7'})
         .merge(self.timerefs, left_on='Y8', right_on='id')
         .drop('Y8', axis=1).rename(columns={'dates':'Y8'})
         .merge(self.timerefs, left_on='Y9', right_on='id')
         .drop('Y9', axis=1).rename(columns={'dates':'Y9'})
         .merge(self.timerefs, left_on='Y10', right_on='id')
         .drop('Y10', axis=1).rename(columns={'dates':'Y10'})
        )


    def quoteheader(self):
        return table(self.cur, 'MSheader')


    def valuation(self):
        val = table(self.cur, 'MSvaluation')
        yrs = val.iloc[0, 2:13].replace(self.timerefs['dates']).to_dict()
        cols = val.columns[:13].values.tolist() + list(map(
            lambda col: ''.join([col[:3], yrs[col[3:]]]), val.columns[13:]))
        val.columns = cols

        return val.set_index(['exchange_id', 'ticker_id']).iloc[:, 11:]


    def keyratios(self):
        keyratios = table(self.cur, 'MSfinancials')
        return self.add_yr_cols(keyratios)


    def finhealth(self):
        finanhealth = table(self.cur, 'MSratio_financial')
        return finanhealth


    def profitability(self):
        profitab = table(self.cur, 'MSratio_profitability')
        return profitab


    def growth(self):
        growth = table(self.cur, 'MSratio_growth')
        return growth


    def cfhealth(self):
        cfhealth = table(self.cur, 'MSratio_cashflow')
        return cfhealth


    def efficiency(self):
        efficiency = table(self.cur, 'MSratio_efficiency')
        return efficiency

    # Income Statement - Annual
    def annualIS(self):
        rep_is_yr = table(self.cur, 'MSreport_is_yr')
        rep_is_yr.iloc[:,2:8] = (
            rep_is_yr.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_is_yr.columns if 'label' in col]
        rep_is_yr[cols] = (
            rep_is_yr[cols].replace(self.colheaders['header']))
        return rep_is_yr

    # Income Statement - Quarterly
    def quarterlyIS(self):
        rep_is_qt = table(self.cur, 'MSreport_is_qt')
        rep_is_qt.iloc[:,2:8] = (
            rep_is_qt.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_is_qt.columns if 'label' in col]
        rep_is_qt[cols] = (
            rep_is_qt[cols].replace(self.colheaders['header']))
        return rep_is_qt

    # Balance Sheet - Annual
    def annualBS(self):
        rep_bs_yr = table(self.cur, 'MSreport_bs_yr')
        rep_bs_yr.iloc[:,2:7] = (
            rep_bs_yr.iloc[:,2:7].replace(self.timerefs['dates']))
        cols = [col for col in rep_bs_yr.columns if 'label' in col]
        rep_bs_yr[cols] = (
            rep_bs_yr[cols].replace(self.colheaders['header']))
        return rep_bs_yr

    # Balance Sheet - Quarterly
    def quarterlyBS(self):
        rep_bs_qt = table(self.cur, 'MSreport_bs_qt')
        rep_bs_qt.iloc[:,2:7] = (
            rep_bs_qt.iloc[:,2:7].replace(self.timerefs['dates']))
        cols = [col for col in rep_bs_qt.columns if 'label' in col]
        rep_bs_qt[cols] = (
            rep_bs_qt[cols].replace(self.colheaders['header']))
        return rep_bs_qt

    # Cashflow Statement - Annual
    def annualCF(self):
        rep_cf_yr = table(self.cur, 'MSreport_cf_yr')
        rep_cf_yr.iloc[:,2:8] = (
            rep_cf_yr.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_cf_yr.columns if 'label' in col]
        rep_cf_yr[cols] = (
            rep_cf_yr[cols].replace(self.colheaders['header']))
        return rep_cf_yr

    # Cashflow Statement - Quarterly
    def quarterlyCF(self):
        rep_cf_qt = table(self.cur, 'MSreport_cf_qt')
        rep_cf_qt.iloc[:,2:8] = (
            rep_cf_qt.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_cf_qt.columns if 'label' in col]
        rep_cf_qt[cols] = (
            rep_cf_qt[cols].replace(self.colheaders['header']))
        return rep_cf_qt

    # 10yr Price History
    def priceHistory(self):
        return table(self.cur, 'MSpricehistory')


    def __del__(self):
        self.cur.close()
        self.conn.close()
        #print('Database connection for file {} closed.'.format(self.file))


def table(cur, tbl, prnt = False):
    cur.execute('select * from {}'.format(tbl))
    cols = list(tbl_js[tbl].keys())
    if 'PRIMARY KEY' in cols: cols = cols[:-1]

    try:
        if prnt == True:
            msg = 'Creating DataFrame \'{}\' ...'
            print(msg.format(tbl.lower()))
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except:
        raise


with open('input/tables.json') as file:
    tbl_js = json.load(file)
    tbl_names = list(tbl_js.keys())
