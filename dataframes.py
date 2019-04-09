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


    def quoteheader(self):
        return self.table('MSheader')


    def valuation(self):
        # Create DataFrame
        val = self.table('MSvaluation')

        # Rename column headers with actual year values
        yrs = val.iloc[0, 2:13].replace(self.timerefs['dates']).to_dict()
        cols = val.columns[:13].values.tolist() + list(map(
            lambda col: ''.join([col[:3], yrs[col[3:]]]), val.columns[13:]))
        val.columns = cols

        # Resize and reorder columns
        val = val.set_index(['exchange_id', 'ticker_id']).iloc[:, 11:]

        return val


    def keyratios(self):
        keyr = self.table('MSfinancials')
        yr_cols = ['Y0', 'Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6',
            'Y7', 'Y8', 'Y9', 'Y10']
        keyr = self.get_yrcolumns(keyr, yr_cols)
        keyr[yr_cols[:-1]] = keyr[yr_cols[:-1]].astype('datetime64')

        return keyr


    def finhealth(self):
        finan = self.table('MSratio_financial')
        yr_cols = [col for col in finan.columns if col.startswith('fh_Y')]
        finan = self.get_yrcolumns(finan, yr_cols)

        return finan


    def profitability(self):
        profit= self.table('MSratio_profitability')
        yr_cols = [col for col in profit.columns if col.startswith('pr_Y')]
        profit = self.get_yrcolumns(profit, yr_cols)

        return profit


    def growth(self):
        growth = self.table('MSratio_growth')
        yr_cols = [col for col in growth.columns if col.startswith('gr_Y')]
        growth = self.get_yrcolumns(growth, yr_cols)

        return growth


    def cfhealth(self):
        cfhealth = self.table('MSratio_cashflow')
        yr_cols = [col for col in cfhealth.columns if col.startswith('cf_Y')]
        cfhealth = self.get_yrcolumns(cfhealth, yr_cols)

        return cfhealth


    def efficiency(self):
        effic = self.table('MSratio_efficiency')
        yr_cols = [col for col in effic.columns if col.startswith('ef_Y')]
        effic = self.get_yrcolumns(effic, yr_cols)

        return effic

    # Income Statement - Annual
    def annualIS(self):
        rep_is_yr = self.table('MSreport_is_yr')
        yr_cols = [col for col in rep_is_yr.columns
                    if col.startswith('Year_Y')]
        rep_is_yr = self.get_yrcolumns(rep_is_yr, yr_cols)

        return rep_is_yr

    # Income Statement - Quarterly
    def quarterlyIS(self):
        rep_is_qt = self.table('MSreport_is_qt')
        yr_cols = [col for col in rep_is_qt.columns
                    if col.startswith('Year_Y')]
        rep_is_qt = self.get_yrcolumns(rep_is_qt, yr_cols)

        return rep_is_qt

    # Balance Sheet - Annual
    def annualBS(self):
        rep_bs_yr = self.table('MSreport_bs_yr')
        yr_cols = [col for col in rep_bs_yr.columns
                    if col.startswith('Year_Y')]
        rep_bs_yr = self.get_yrcolumns(rep_bs_yr, yr_cols)

        return rep_bs_yr

    # Balance Sheet - Quarterly
    def quarterlyBS(self):
        rep_bs_qt = self.table('MSreport_bs_qt')
        yr_cols = [col for col in rep_bs_qt.columns
                    if col.startswith('Year_Y')]
        rep_bs_qt = self.get_yrcolumns(rep_bs_qt, yr_cols)

        return rep_bs_qt

    # Cashflow Statement - Annual
    def annualCF(self):
        rep_cf_yr = self.table('MSreport_cf_yr')
        yr_cols = [col for col in rep_cf_yr.columns
                    if col.startswith('Year_Y')]
        rep_cf_yr = self.get_yrcolumns(rep_cf_yr, yr_cols)

        return rep_cf_yr

    # Cashflow Statement - Quarterly
    def quarterlyCF(self):
        rep_cf_qt = self.table('MSreport_cf_qt')
        yr_cols = [col for col in rep_cf_qt.columns
                    if col.startswith('Year_Y')]
        rep_cf_qt = self.get_yrcolumns(rep_cf_qt, yr_cols)

        return rep_cf_qt

    # 10yr Price History
    def priceHistory(self):

        return self.table('MSpricehistory')


    def get_yrcolumns(self, df, cols):
        for yr in cols:
            df = (df.merge(self.timerefs, left_on=yr, right_on='id')
                .drop(yr, axis=1).rename(columns={'dates':yr}))

        return df


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
