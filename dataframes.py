import pandas as pd
import numpy as np
import sqlite3
import json
import sys
import re
import os


class DataFrames():

    db_file = 'db/mstables.sqlite' # Standard db file name

    def __init__(self, file = db_file):

        print('Creating intial DataFrames from file {}...'.format(file))

        self.conn = sqlite3.connect(
            file, detect_types=sqlite3.PARSE_COLNAMES)
        self.cur = self.conn.cursor()

        # Row Headers
        colheaders = self.table('ColHeaders', True)
        self.colheaders = colheaders.set_index('id')

        # Dates and time references
        timerefs = self.table('TimeRefs', True)
        self.timerefs = timerefs.set_index('id').replace(['', '—'], None)

        # Reference tables
        self.urls = self.table('URLs', True)
        self.securitytypes = self.table('SecurityTypes', True)
        self.tickers = self.table('Tickers', True)
        self.sectors = self.table('Sectors', True)
        self.industries = self.table('Industries', True)
        self.styles = self.table('StockStyles', True)
        self.exchanges = self.table('Exchanges', True)
        self.countries = (self.table('Countries', True)
            .rename(columns={'a2_iso':'country_c2', 'a3_un':'country_c3'}))
        self.companies = self.table('Companies', True)
        self.currencies = self.table('Currencies', True)
        self.stocktypes = self.table('StockTypes', True)
        #self.fetchedurls = self.table('Fetched_urls', True)

        # Master table
        self.master = self.table('Master', True)

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
        return self.table('MSheader')


    def valuation(self):
        val = self.table('MSvaluation')

        yrs = val.iloc[0, 2:13].replace(self.timerefs['dates']).to_dict()
        cols = val.columns[:13].values.tolist() + list(map(
            lambda col: ''.join([col[:3], yrs[col[3:]]]), val.columns[13:]))
        val.columns = cols

        return val.set_index(['exchange_id', 'ticker_id']).iloc[:, 11:]


    def keyratios(self):
        keyratios = self.table('MSfinancials')
        keyratios = self.add_yr_cols(keyratios)
        keyratios.loc[:, 'Y0':'Y9'] = (
            keyratios.loc[:, 'Y0':'Y9'].astype('datetime64'))

        return keyratios


    def finhealth(self):
        finanhealth = self.table('MSratio_financial')
        return finanhealth


    def profitability(self):
        profitab = self.table('MSratio_profitability')
        return profitab


    def growth(self):
        growth = self.table('MSratio_growth')
        return growth


    def cfhealth(self):
        cfhealth = self.table('MSratio_cashflow')
        return cfhealth


    def efficiency(self):
        efficiency = self.table('MSratio_efficiency')
        return efficiency

    # Income Statement - Annual
    def annualIS(self):
        rep_is_yr = self.table('MSreport_is_yr')

        '''# Replace date Columns
        rep_is_yr.iloc[:,2:8] = (rep_is_yr.iloc[:,2:8]
            .replace(self.timerefs['dates']))

        # Format Date Columns
        rep_is_yr.iloc[:,2:7] = (rep_is_yr.iloc[:,2:7].astype('datetime64'))

        # Replace column header values in label columns
        cols = [col for col in rep_is_yr.columns if 'label' in col]
        rep_is_yr[cols] = (
            rep_is_yr[cols].replace(self.colheaders['header']))'''

        return rep_is_yr

    # Income Statement - Quarterly
    def quarterlyIS(self):
        rep_is_qt = self.table('MSreport_is_qt')

        '''rep_is_qt.iloc[:,2:8] = (rep_is_qt.iloc[:,2:8]
            .replace(self.timerefs['dates']))
        rep_is_qt.iloc[:,2:7] = (rep_is_qt.iloc[:,2:7].astype('datetime64'))
        cols = [col for col in rep_is_qt.columns if 'label' in col]
        rep_is_qt[cols] = (
            rep_is_qt[cols].replace(self.colheaders['header']))'''

        return rep_is_qt

    # Balance Sheet - Annual
    def annualBS(self):
        rep_bs_yr = self.table('MSreport_bs_yr')

        '''rep_bs_yr.iloc[:,2:7] = (rep_bs_yr.iloc[:,2:7]
            .replace(self.timerefs['dates'])
            .astype('datetime64'))
        cols = [col for col in rep_bs_yr.columns if 'label' in col]
        rep_bs_yr[cols] = (
            rep_bs_yr[cols].replace(self.colheaders['header']))'''

        return rep_bs_yr

    # Balance Sheet - Quarterly
    def quarterlyBS(self):
        rep_bs_qt = self.table('MSreport_bs_qt')

        '''rep_bs_qt.iloc[:,2:7] = (rep_bs_qt.iloc[:,2:7]
            .replace(self.timerefs['dates'])
            .astype('datetime64'))
        cols = [col for col in rep_bs_qt.columns if 'label' in col]
        rep_bs_qt[cols] = (
            rep_bs_qt[cols].replace(self.colheaders['header']))'''

        return rep_bs_qt

    # Cashflow Statement - Annual
    def annualCF(self):
        rep_cf_yr = self.table('MSreport_cf_yr')

        '''rep_cf_yr.iloc[:,2:8] = (rep_cf_yr.iloc[:,2:8]
            .replace(self.timerefs['dates']))
        rep_cf_yr.iloc[:,2:7] = (rep_cf_yr.iloc[:,2:7].astype('datetime64'))
        cols = [col for col in rep_cf_yr.columns if 'label' in col]
        rep_cf_yr[cols] = (
            rep_cf_yr[cols].replace(self.colheaders['header']))'''

        return rep_cf_yr

    # Cashflow Statement - Quarterly
    def quarterlyCF(self):
        rep_cf_qt = self.table('MSreport_cf_qt')

        '''rep_cf_qt.iloc[:,2:8] = (rep_cf_qt.iloc[:,2:8]
            .replace(self.timerefs['dates']))
        rep_cf_qt.iloc[:,2:7] = (rep_cf_qt.iloc[:,2:7].astype('datetime64'))
        cols = [col for col in rep_cf_qt.columns if 'label' in col]
        rep_cf_qt[cols] = (
            rep_cf_qt[cols].replace(self.colheaders['header']))'''

        return rep_cf_qt

    # 10yr Price History
    def priceHistory(self):
        return self.table('MSpricehistory')


    def table(self, tbl, prnt = False):
        self.cur.execute('SELECT * FROM {}'.format(tbl))
        cols = list(zip(*self.cur.description))[0]

        try:
            if prnt == True:
                msg = 'Creating DataFrame \'{}\' ...'
                print(msg.format(tbl.lower()))
            return pd.DataFrame(self.cur.fetchall(), columns=cols)
        except:
            raise


    def __del__(self):
        self.cur.close()
        self.conn.close()
